%%%-------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019-2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles middleware operations (create, get, delete)
%%% corresponding to QoS management.
%%% @end
%%%-------------------------------------------------------------------
-module(qos_middleware_plugin).
-author("Michal Cwiertnia").

-behaviour(middleware_router).
-behaviour(middleware_handler).

-include("middleware/middleware.hrl").
-include("modules/datastore/qos.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("cluster_worker/include/modules/datastore/infinite_log.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/privileges.hrl").

%% middleware_router callbacks
-export([resolve_handler/3]).

%% middleware_handler callbacks
-export([data_spec/1, fetch_entity/1, authorize/2, validate/2]).
-export([create/1, get/2, update/1, delete/1]).

-define(MAX_LIST_LIMIT, 1000).

%%%===================================================================
%%% middleware_router callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_router} callback resolve_handler/3.
%% @end
%%--------------------------------------------------------------------
-spec resolve_handler(middleware:operation(), gri:aspect(), middleware:scope()) ->
    module() | no_return().
resolve_handler(create, instance, private) -> ?MODULE;

resolve_handler(get, instance, private) -> ?MODULE;
resolve_handler(get, audit_log, private) -> ?MODULE;
resolve_handler(get, {transfer_stats_collection_schema, ?BYTES_STATS}, public) -> ?MODULE;
resolve_handler(get, {transfer_stats_collection_schema, ?FILES_STATS}, public) -> ?MODULE;
resolve_handler(get, {transfer_stats_collection, ?BYTES_STATS}, private) -> ?MODULE;
resolve_handler(get, {transfer_stats_collection, ?FILES_STATS}, private) -> ?MODULE;

resolve_handler(delete, instance, private) -> ?MODULE;

resolve_handler(_, _, _) -> throw(?ERROR_NOT_SUPPORTED).


%%%===================================================================
%%% middleware_handler callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback data_spec/1.
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
data_spec(#op_req{operation = get, gri = #gri{aspect = audit_log}}) ->
    audit_log_browse_opts:json_data_spec();
data_spec(#op_req{operation = get, gri = #gri{aspect = {transfer_stats_collection_schema, _}}}) ->
    undefined;
data_spec(#op_req{operation = get, gri = #gri{aspect = {transfer_stats_collection, _}}}) -> #{
    % for this aspect data is sanitized in `get` function, but all possible parameters
    % still have to be specified so they are not removed during sanitization
    optional => #{
        <<"mode">> => {any, any},
        <<"layout">> => {any, any},
        <<"startTimestamp">> => {any, any},
        <<"windowLimit">> => {any, any},
        <<"extendedInfo">> => {any, any}
    }
};

data_spec(#op_req{operation = delete, gri = #gri{aspect = instance}}) ->
    undefined.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback fetch_entity/1.
%% @end
%%--------------------------------------------------------------------
-spec fetch_entity(middleware:req()) ->
    {ok, middleware:versioned_entity()} | no_return().
fetch_entity(#op_req{operation = create, gri = #gri{aspect = instance}}) ->
    {ok, {undefined, 1}};

fetch_entity(#op_req{operation = get, auth = Auth, gri = #gri{
    id = QosEntryId,
    aspect = instance
}}) ->
    fetch_qos_entry(Auth, QosEntryId);
fetch_entity(#op_req{operation = get, gri = #gri{aspect = audit_log}}) ->
    {ok, {undefined, 1}};
fetch_entity(#op_req{operation = get, gri = #gri{aspect = {transfer_stats_collection_schema, _}}}) ->
    {ok, {undefined, 1}};
fetch_entity(#op_req{operation = get, gri = #gri{aspect = {transfer_stats_collection, _}}}) ->
    {ok, {undefined, 1}};

fetch_entity(#op_req{operation = delete, auth = Auth, gri = #gri{
    id = QosEntryId,
    aspect = instance
}}) ->
    fetch_qos_entry(Auth, QosEntryId).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback authorize/2.
%%
%% Checks only membership in space.
%% @end
%%--------------------------------------------------------------------
-spec authorize(middleware:req(), middleware:entity()) -> boolean().
authorize(#op_req{operation = get, gri = #gri{
    id = undefined,
    aspect = {transfer_stats_collection_schema, _},
    scope = public
}}, _QosEntry) ->
    true;

authorize(#op_req{auth = ?GUEST}, _) ->
    false;

authorize(#op_req{operation = create, auth = ?USER(UserId), gri = #gri{aspect = instance}, data = #{
    <<"fileId">> := FileGuid
}}, _) ->
    SpaceId = file_id:guid_to_space_id(FileGuid),
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_MANAGE_QOS);

authorize(#op_req{operation = get, auth = ?USER(UserId), gri = #gri{
    id = QosEntryId,
    aspect = Aspect
}}, _QosEntry) when
    Aspect =:= instance;
    Aspect =:= audit_log;
    element(1, Aspect) =:= transfer_stats_collection
->
    {ok, SpaceId} = ?lfm_check(qos_entry:get_space_id(QosEntryId)),
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_VIEW_QOS);

authorize(#op_req{operation = delete, auth = ?USER(UserId), gri = #gri{
    id = QosEntryId,
    aspect = instance
}}, _QosEntry) ->
    {ok, SpaceId} = ?lfm_check(qos_entry:get_space_id(QosEntryId)),
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_MANAGE_QOS).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback validate/2.
%% @end
%%--------------------------------------------------------------------
-spec validate(middleware:req(), middleware:entity()) -> ok | no_return().
validate(#op_req{operation = create, gri = #gri{aspect = instance}, data = #{
    <<"fileId">> := FileGuid
}}, _) ->
    SpaceId = file_id:guid_to_space_id(FileGuid),
    middleware_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = get, gri = #gri{id = QosEntryId, aspect = Aspect}}, _QosEntry) when
    Aspect =:= instance;
    Aspect =:= audit_log;
    element(1, Aspect) =:= transfer_stats_collection
->
    {ok, SpaceId} = ?lfm_check(qos_entry:get_space_id(QosEntryId)),
    middleware_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = get, gri = #gri{
    id = undefined,
    aspect = {transfer_stats_collection_schema, _},
    scope = public
}}, _QosEntry) ->
    ok;

validate(#op_req{operation = delete, gri = #gri{
    id = QosEntryId,
    aspect = instance
}}, _QosEntry) ->
    {ok, SpaceId} = ?lfm_check(qos_entry:get_space_id(QosEntryId)),
    middleware_utils:assert_space_supported_locally(SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(middleware:req()) -> middleware:create_result().
create(#op_req{auth = Auth, gri = #gri{aspect = instance} = GRI} = Req) ->
    SessionId = Auth#auth.session_id,
    Expression = maps:get(<<"expression">>, Req#op_req.data),
    ReplicasNum = maps:get(<<"replicasNum">>, Req#op_req.data, 1),
    FileGuid = maps:get(<<"fileId">>, Req#op_req.data),
    SpaceId = file_id:guid_to_space_id(FileGuid),

    QosEntryId = mi_qos:add_qos_entry(SessionId, ?FILE_REF(FileGuid), Expression, ReplicasNum),
    QosEntry = mi_qos:get_qos_entry(SessionId, QosEntryId),

    Status = case qos_entry:is_possible(QosEntry) of
        true -> ?PENDING_QOS_STATUS;
        false -> ?IMPOSSIBLE_QOS_STATUS
    end,
    {ok, resource, {GRI#gri{id = QosEntryId}, entry_to_details(QosEntry, Status, SpaceId)}}.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(middleware:req(), middleware:entity()) -> middleware:get_result().
get(#op_req{auth = Auth, gri = #gri{id = QosEntryId, aspect = instance}}, QosEntry) ->
    SessionId = Auth#auth.session_id,
    {ok, SpaceId} = qos_entry:get_space_id(QosEntryId),
    Status = mi_qos:check_qos_status(SessionId, QosEntryId),
    {ok, entry_to_details(QosEntry, Status, SpaceId)};

get(#op_req{gri = #gri{id = QosEntryId, aspect = audit_log}, data = Data}, _QosEntry) ->
    BrowseOpts = audit_log_browse_opts:from_json(Data),
    case qos_entry_audit_log:browse_content(QosEntryId, BrowseOpts) of
        {ok, BrowseResult} -> {ok, value, BrowseResult};
        {error, _} = Error -> Error
    end;

get(#op_req{gri = #gri{id = undefined, aspect = {transfer_stats_collection_schema, Type}}}, _QosEntry) ->
    {ok, value, qos_transfer_stats:get_collection_schema(Type)};

get(#op_req{gri = #gri{id = QosEntryId, aspect = {transfer_stats_collection, Type}}, data = Data}, _QosEntry) ->
    BrowseRequest = ts_browse_request:from_json(Data),
    case qos_transfer_stats:browse(QosEntryId, Type, BrowseRequest) of
        {ok, BrowseResult} ->
            {ok, value, BrowseResult};
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback update/1.
%% @end
%%--------------------------------------------------------------------
-spec update(middleware:req()) -> middleware:update_result().
update(_) ->
    ?ERROR_NOT_SUPPORTED.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(middleware:req()) -> middleware:delete_result().
delete(#op_req{auth = Auth, gri = #gri{id = QosEntryId, aspect = instance}}) ->
    mi_qos:remove_qos_entry(Auth#auth.session_id, QosEntryId).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec fetch_qos_entry(aai:auth(), qos_entry:id()) ->
    {ok, {qos_entry:record(), middleware:revision()}} | no_return().
fetch_qos_entry(_Auth, QosEntryId) ->
    {ok, {mi_qos:get_qos_entry(?ROOT_SESS_ID, QosEntryId), 1}}.


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
