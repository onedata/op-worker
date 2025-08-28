%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% HTTP handler for space file events stream.
%%% @end
%%%--------------------------------------------------------------------
-module(space_file_events_stream_handler).
-author("Bartosz Walkowicz").

-behaviour(cowboy_loop).

-include("http/space_file_events_stream.hrl").
-include("middleware/middleware.hrl").

%% API
-export([init/2, info/3]).

-record(state, {
    space_id :: od_space:id(),
    auth :: aai:auth(),
    files_monitoring_spec :: space_files_monitoring_spec:t(),
    monitor_pid :: pid()
}).
-type state() :: #state{}.


%%%===================================================================
%%% API
%%%===================================================================


-spec init(cowboy_req:req(), term()) ->
    {ok, cowboy_req:req(), no_state} | {cowboy_loop, cowboy_req:req(), state()}.
init(Req, _Opts) ->
    % Trap exits as this connection process will link with space files monitor process
    process_flag(trap_exit, true),

    try
        {ok, State, Req2} = handle_init(Req),
        {cowboy_loop, Req2, State}
    catch Class:Reason:Stacktrace ->
        Error = ?examine_exception(Class, Reason, Stacktrace),
        {ok, http_req:send_error(Error, Req), no_state}
    end.


-spec info(space_files_monitor:event(), cowboy_req:req(), state()) ->
    {ok, cowboy_req:req(), state()}.
info(Event = #file_deleted_event{id = Id}, Req, State) ->
    ResponseEvent = #{
        id => Id,
        event => <<"deleted">>,
        data => json_utils:encode(file_deleted_event_to_json(Event))
    },
    cowboy_req:stream_events(ResponseEvent, nofin, Req),
    {ok, Req, State};

info(Event = #file_changed_or_created_event{id = Id}, Req, State) ->
    ResponseEvent = #{
        id => Id,
        event => <<"changedOrCreated">>,
        data => json_utils:encode(file_changed_or_created_event_to_json(Event, State))
    },
    cowboy_req:stream_events(ResponseEvent, nofin, Req),
    {ok, Req, State};

info({'EXIT', MonitorPid, _Reason}, Req, State = #state{monitor_pid = MonitorPid}) ->
    cowboy_req:stream_events(#{}, fin, Req),
    {stop, Req, State};

info(Msg, Req, State) ->
    ?log_bad_request(Msg),
    {ok, Req, State}.


%%%===================================================================
%%% Content type handler functions
%%%===================================================================


%% @private
-spec handle_init(cowboy_req:req()) -> {ok, state(), cowboy_req:req()} | no_return().
handle_init(Req) ->
    SpaceId = cowboy_req:binding(sid, Req),
    middleware_utils:assert_space_supported_locally(SpaceId),

    Auth = ?check(authenticate(Req)),
    SessionId = Auth#auth.session_id,
    ?check(preauthorize(SpaceId, Auth)),

    {SpaceFilesMonitoringSpec, Req2} = space_files_monitoring_spec:parse_and_validate(
        SpaceId, SessionId, Req
    ),

    MonitorPid = space_files_monitor_sup:ensure_monitor_started(SpaceId),
    ok = space_files_monitor:subscribe_link(MonitorPid, SessionId, SpaceFilesMonitoringSpec),
    Req3 = cowboy_req:stream_reply(
        ?HTTP_200_OK, #{?HDR_CONTENT_TYPE => <<"text/event-stream">>}, Req2
    ),

    State = #state{
        space_id = SpaceId,
        auth = Auth,
        files_monitoring_spec = SpaceFilesMonitoringSpec,
        monitor_pid = MonitorPid
    },

    {ok, State, Req3}.


%% @private
-spec authenticate(cowboy_req:req()) -> {ok, aai:auth()} | errors:error().
authenticate(Req) ->
    AuthCtx = #http_auth_ctx{
        interface = rest,
        data_access_caveats_policy = allow_data_access_caveats
    },
    case http_auth:authenticate(Req, AuthCtx) of
        {ok, ?USER} = Result ->
            Result;
        {ok, ?GUEST} ->
            ?ERR_UNAUTHORIZED(?err_ctx(), undefined);
        ?ERR = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks space membership and api auth. Access to observed directories
%% will be verified later, after sanitization.
%% @end
%%--------------------------------------------------------------------
-spec preauthorize(od_space:id(), aai:auth()) -> ok | errors:error().
preauthorize(SpaceId, Auth) ->
    case middleware_utils:is_eff_space_member(Auth, SpaceId) of
        true ->
            GRI = #gri{type = op_space, id = SpaceId, aspect = file_events},
            ?catch_exceptions(api_auth:check_authorization(Auth, ?OP_WORKER, create, GRI));
        false ->
            ?ERR_FORBIDDEN(?err_ctx())
    end.


%% @private
-spec file_deleted_event_to_json(space_files_monitor:file_deleted_event()) ->
    json_utils:json_map().
file_deleted_event_to_json(#file_deleted_event{
    file_guid = FileGuid,
    parent_file_guid = ParentGuid
}) ->
    #{
        <<"fileId">> => file_id:check_guid_to_objectid(FileGuid),
        <<"parentFileId">> => file_id:check_guid_to_objectid(ParentGuid)
    }.


%% @private
-spec file_changed_or_created_event_to_json(space_files_monitor:file_changed_or_created_event(), state()) ->
    json_utils:json_map().
file_changed_or_created_event_to_json(#file_changed_or_created_event{
    file_guid = FileGuid,
    parent_file_guid = ParentGuid,
    doc_type = DocType,
    file_attr = FileAttr
}, State) ->
    ObservedAttrs = get_observed_doc_attrs(DocType, State),

    #{
        <<"fileId">> => file_id:check_guid_to_objectid(FileGuid),
        <<"parentFileId">> => file_id:check_guid_to_objectid(ParentGuid),
        <<"attributes">> => file_attr_translator:to_json(
            FileAttr, current, ObservedAttrs
        )
    }.


%% @private
-spec get_observed_doc_attrs(file_meta | times | file_location, state()) ->
    [onedata_file:attr_name()].
get_observed_doc_attrs(DocType, #state{files_monitoring_spec = #space_files_monitoring_spec{
    observed_attrs_per_doc = ObservedAttrsPerDoc
}}) ->
    maps:get(DocType, ObservedAttrsPerDoc).
