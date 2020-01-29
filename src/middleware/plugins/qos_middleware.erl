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
operation_supported(get, effective_qos, private) -> true;

operation_supported(delete, instance, private) -> true;

operation_supported(_, _, _) -> false.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback data_spec/1.
%% @end
%%--------------------------------------------------------------------
-spec data_spec(middleware:req()) -> undefined | middleware_sanitizer:data_spec().
data_spec(#op_req{operation = create, gri = #gri{aspect = instance}}) -> #{
    required => #{<<"expression">> => {binary, non_empty}},
    optional => #{<<"replicasNum">> => {integer, {not_lower_than, 1}}}
};

data_spec(#op_req{operation = get, gri = #gri{aspect = instance}}) ->
    undefined;

data_spec(#op_req{operation = get, gri = #gri{aspect = effective_qos}}) ->
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

fetch_entity(#op_req{operation = get, gri = #gri{aspect = effective_qos}}) ->
    {ok, {undefined, 1}};

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

authorize(#op_req{operation = create, auth = ?USER(UserId), gri = #gri{
    id = FileGuid,
    aspect = instance
}}, _) ->
    SpaceId = file_id:guid_to_space_id(FileGuid),
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_MANAGE_QOS);

authorize(#op_req{operation = get, auth = ?USER(UserId), gri = #gri{
    id = QosEntryId,
    aspect = instance
}}, _QosEntry) ->
    {ok, SpaceId} = ?check(qos_entry:get_space_id(QosEntryId)),
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_VIEW_QOS);

authorize(#op_req{operation = get, auth = ?USER(UserId), gri = #gri{
    id = FileGuid,
    aspect = effective_qos
}}, _) ->
    SpaceId = file_id:guid_to_space_id(FileGuid),
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
validate(#op_req{operation = create, gri = #gri{
    id = Guid,
    aspect = instance
}}, _) ->
    SpaceId = file_id:guid_to_space_id(Guid),
    middleware_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = get, gri = #gri{
    id = QosEntryId,
    aspect = instance
}}, _QosEntry) ->
    {ok, SpaceId} = ?check(qos_entry:get_space_id(QosEntryId)),
    middleware_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = get, gri = #gri{
    id = Guid,
    aspect = effective_qos
}}, _) ->
    SpaceId = file_id:guid_to_space_id(Guid),
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
create(#op_req{auth = Auth, gri = #gri{id = FileGuid, aspect = instance}} = Req) ->
    SessionId = Auth#auth.session_id,
    QosExpression = maps:get(<<"expression">>, Req#op_req.data),
    ReplicasNum = maps:get(<<"replicasNum">>, Req#op_req.data, 1),

    case lfm:add_qos_entry(SessionId, {guid, FileGuid}, QosExpression, ReplicasNum) of
        {ok, QosEntryId} ->
            {ok, value, QosEntryId};
        ?ERROR_INVALID_QOS_EXPRESSION ->
            ?ERROR_INVALID_QOS_EXPRESSION;
        {error, Errno} ->
            ?ERROR_POSIX(Errno)
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(middleware:req(), middleware:entity()) -> middleware:get_result().
get(#op_req{auth = Auth, gri = #gri{id = FileGuid, aspect = effective_qos}}, _) ->
    SessionId = Auth#auth.session_id,
    case lfm:get_effective_file_qos(SessionId, {guid, FileGuid}) of
        {ok, {QosEntries, AssignedEntries}} ->
            {ok, Fulfilled} = ?check(lfm_qos:check_qos_fulfilled(
                SessionId, QosEntries, {guid, FileGuid})
            ),
            {ok, #{
                <<"qosEntries">> => QosEntries,
                <<"assignedEntries">> => AssignedEntries,
                <<"fulfilled">> => Fulfilled
            }};
        ?ERROR_NOT_FOUND ->
            ?ERROR_NOT_FOUND;
        {error, Errno} ->
            ?ERROR_POSIX(Errno)
    end;

get(#op_req{auth = Auth, gri = #gri{id = QosEntryId, aspect = instance}}, QosEntry) ->
    SessionId = Auth#auth.session_id,
    {ok, Fulfilled} = ?check(lfm_qos:check_qos_fulfilled(SessionId, QosEntryId)),
    {ok, Expression} = qos_entry:get_expression(QosEntry),
    {ok, ReplicasNum} = qos_entry:get_replicas_num(QosEntry),
    {ok, #{
        <<"qosEntryId">> => QosEntryId,
        <<"expression">> => Expression,
        <<"replicasNum">> => ReplicasNum,
        <<"fulfilled">> => Fulfilled
    }}.


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
fetch_qos_entry(?USER(_UserId, SessionId), QosEntryId) ->
    case lfm:get_qos_entry(SessionId, QosEntryId) of
        {ok, QosEntry} ->
            {ok, {QosEntry, 1}};
        _ ->
            ?ERROR_NOT_FOUND
    end.
