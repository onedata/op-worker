%%%-------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles middleware operations (create, get, delete)
%%% corresponding to QoS management.
%%% @end
%%%-------------------------------------------------------------------
-module(qos_middleware).
-author("Michal Cwiertnia").

-behaviour(middleware_plugin).

-include("middleware/middleware.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/privileges.hrl").

-export([
    operation_supported/3,
    data_spec/1,
    fetch_entity/1,
    authorize/2,
    validate/2
]).
-export([create/1, get/2, update/1, delete/1]).

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback operation_supported/3.
%% @end
%%--------------------------------------------------------------------
-spec operation_supported(middleware:operation(), gri:aspect(),
    middleware:scope()) -> boolean().
operation_supported(create, instance, private) -> true;

operation_supported(get, instance, private) -> true;

operation_supported(delete, instance, private) -> true;

operation_supported(_, _, _) -> false.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback data_spec/1.
%% @end
%%--------------------------------------------------------------------
-spec data_spec(middleware:req()) -> undefined | middleware_sanitizer:data_spec().
data_spec(#op_req{operation = create, gri = #gri{aspect = instance}}) -> #{
    required => #{
        <<"expression">> => {binary, 
            fun(Expression) -> {true, qos_expression:parse(Expression)} end},
        <<"fileId">> => {binary, 
            fun(ObjectId) -> {true, middleware_utils:decode_object_id(ObjectId, <<"fileId">>)} end}
    },
    optional => #{<<"replicasNum">> => {integer, {not_lower_than, 1}}}
};

data_spec(#op_req{operation = get, gri = #gri{aspect = instance}}) ->
    undefined;

data_spec(#op_req{operation = delete, gri = #gri{aspect = instance}}) ->
    undefined.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback fetch_entity/1.
%% @end
%%--------------------------------------------------------------------
-spec fetch_entity(middleware:req()) ->
    {ok, middleware:versioned_entity()} | errors:error().
fetch_entity(#op_req{operation = create, gri = #gri{aspect = instance}}) ->
    {ok, {undefined, 1}};

fetch_entity(#op_req{operation = get, auth = Auth, gri = #gri{
    id = QosEntryId,
    aspect = instance
}}) ->
    fetch_qos_entry(Auth, QosEntryId);

fetch_entity(#op_req{operation = delete, auth = Auth, gri = #gri{
    id = QosEntryId,
    aspect = instance
}}) ->
    fetch_qos_entry(Auth, QosEntryId).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback authorize/2.
%%
%% Checks only membership in space.
%% @end
%%--------------------------------------------------------------------
-spec authorize(middleware:req(), middleware:entity()) -> boolean().
authorize(#op_req{auth = ?GUEST}, _) ->
    false;

authorize(#op_req{operation = create, auth = ?USER(UserId), gri = #gri{aspect = instance}, data = #{
    <<"fileId">> := FileGuid
}}, _) ->
    SpaceId = file_id:guid_to_space_id(FileGuid),
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_MANAGE_QOS);

authorize(#op_req{operation = get, auth = ?USER(UserId), gri = #gri{
    id = QosEntryId,
    aspect = instance
}}, _QosEntry) ->
    {ok, SpaceId} = ?check(qos_entry:get_space_id(QosEntryId)),
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_VIEW_QOS);

authorize(#op_req{operation = delete, auth = ?USER(UserId), gri = #gri{
    id = QosEntryId,
    aspect = instance
}}, _QosEntry) ->
    {ok, SpaceId} = ?check(qos_entry:get_space_id(QosEntryId)),
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_MANAGE_QOS).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback validate/2.
%% @end
%%--------------------------------------------------------------------
-spec validate(middleware:req(), middleware:entity()) -> ok | no_return().
validate(#op_req{operation = create, gri = #gri{aspect = instance}, data = #{
    <<"fileId">> := FileGuid
}}, _) ->
    SpaceId = file_id:guid_to_space_id(FileGuid),
    middleware_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = get, gri = #gri{
    id = QosEntryId,
    aspect = instance
}}, _QosEntry) ->
    {ok, SpaceId} = ?check(qos_entry:get_space_id(QosEntryId)),
    middleware_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = delete, gri = #gri{
    id = QosEntryId,
    aspect = instance
}}, _QosEntry) ->
    {ok, SpaceId} = ?check(qos_entry:get_space_id(QosEntryId)),
    middleware_utils:assert_space_supported_locally(SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(middleware:req()) -> middleware:create_result().
create(#op_req{auth = Auth, gri = #gri{aspect = instance} = GRI} = Req) ->
    SessionId = Auth#auth.session_id,
    Expression = maps:get(<<"expression">>, Req#op_req.data),
    ReplicasNum = maps:get(<<"replicasNum">>, Req#op_req.data, 1),
    FileGuid = maps:get(<<"fileId">>, Req#op_req.data),
    SpaceId = file_id:guid_to_space_id(FileGuid),

    case lfm:add_qos_entry(SessionId, {guid, FileGuid}, Expression, ReplicasNum) of
        {ok, QosEntryId} ->
            {ok, QosEntry} = ?check(lfm:get_qos_entry(SessionId, QosEntryId)),
            Status = case qos_entry:is_possible(QosEntry) of
                true -> ?PENDING;
                false -> ?IMPOSSIBLE
            end,
            {ok, resource, {GRI#gri{id = QosEntryId}, entry_to_details(QosEntry, Status, SpaceId)}};
        {error, Errno} ->
            ?ERROR_POSIX(Errno)
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(middleware:req(), middleware:entity()) -> middleware:get_result().
get(#op_req{auth = Auth, gri = #gri{id = QosEntryId, aspect = instance}}, QosEntry) ->
    SessionId = Auth#auth.session_id,
    {ok, SpaceId} = qos_entry:get_space_id(QosEntryId),
    {ok, Status} = ?check(lfm_qos:check_qos_status(SessionId, QosEntryId)),
    {ok, entry_to_details(QosEntry, Status, SpaceId)}.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback update/1.
%% @end
%%--------------------------------------------------------------------
-spec update(middleware:req()) -> middleware:update_result().
update(_) ->
    ?ERROR_NOT_SUPPORTED.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(middleware:req()) -> middleware:delete_result().
delete(#op_req{auth = Auth, gri = #gri{id = QosEntryId, aspect = instance}}) ->
    ?check(lfm:remove_qos_entry(Auth#auth.session_id, QosEntryId)).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec fetch_qos_entry(aai:auth(), qos_entry:id()) ->
    {ok, {qos_entry:record(), middleware:revision()}} | ?ERROR_NOT_FOUND.
fetch_qos_entry(_Auth, QosEntryId) ->
    case lfm:get_qos_entry(?ROOT_SESS_ID, QosEntryId) of
        {ok, QosEntry} ->
            {ok, {QosEntry, 1}};
        _ ->
            ?ERROR_NOT_FOUND
    end.

%% @private
-spec entry_to_details(qos_entry:record(), qos_status:summary(), od_space:id()) -> map().
entry_to_details(QosEntry, Status, SpaceId) ->
    {ok, Expression} = qos_entry:get_expression(QosEntry),
    {ok, ReplicasNum} = qos_entry:get_replicas_num(QosEntry),
    {ok, QosRootFileUuid} = qos_entry:get_file_uuid(QosEntry),
    QosRootFileGuid = file_id:pack_guid(QosRootFileUuid, SpaceId),
    {ok, QosRootFileObjectId} = file_id:guid_to_objectid(QosRootFileGuid),
    #{
        <<"expression">> => Expression,
        <<"replicasNum">> => ReplicasNum,
        <<"fileId">> => QosRootFileObjectId,
        <<"status">> => Status
    }.
