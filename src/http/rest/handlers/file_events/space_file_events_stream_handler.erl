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

-include("http/space_file_events_stream.hrl").
-include("middleware/middleware.hrl").
-include("modules/fslogic/data_access_control.hrl").

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
    process_flag(trap_exit, true),

    try
        SpaceId = cowboy_req:binding(sid, Req),
        middleware_utils:assert_space_supported_locally(SpaceId),

        Auth = authenticate(Req),
        SessionId = Auth#auth.session_id,
        ?check(preauthorize(SpaceId, Auth)),

        {SpaceFilesMonitoringSpec, Req2} = space_files_monitoring_spec:parse_and_validate(
            SpaceId, SessionId, Req
        ),

        MonitorPid = space_files_monitor_sup:ensure_monitor_started(SpaceId),
        ok = space_files_monitor:subscribe(MonitorPid, SessionId, SpaceFilesMonitoringSpec),
        Req3 = cowboy_req:stream_reply(
            ?HTTP_200_OK, #{?HDR_CONTENT_TYPE => <<"text/event-stream">>}, Req2
        ),

        State = #state{
            space_id = SpaceId,
            auth = Auth,
            files_monitoring_spec = SpaceFilesMonitoringSpec,
            monitor_pid = MonitorPid
        },
        {cowboy_loop, Req3, State}
    catch Class:Reason:Stacktrace ->
        Error = ?examine_exception(Class, Reason, Stacktrace),
        {ok, http_req:send_error(Error, Req), no_state}
    end.


-spec info(space_files_monitor:event(), cowboy_req:req(), state()) ->
    {ok, cowboy_req:req(), state()}.
info(Event = #file_changed_or_created_event{id = Id}, Req, State) ->
    ResponseEvent = #{
        id => Id,
        event => <<"changedOrCreated">>,
        data => json_utils:encode(prepare_changed_or_created_event(Event, State))
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
-spec authenticate(cowboy_req:req()) -> aai:auth() | no_return().
authenticate(Req) ->
    AuthCtx = #http_auth_ctx{
        interface = rest,
        data_access_caveats_policy = disallow_data_access_caveats
    },
    case http_auth:authenticate(Req, AuthCtx) of
        {ok, Auth = ?USER} ->
            Auth;
        {ok, ?GUEST} ->
            throw(?ERR_UNAUTHORIZED(?err_ctx(), undefined));
        ?ERR = Error ->
            throw(Error)
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
            GRI = #gri{type = op_metrics, id = SpaceId, aspect = file_events},
            ?catch_exceptions(api_auth:check_authorization(Auth, ?OP_WORKER, create, GRI));
        false ->
            ?ERR_FORBIDDEN(?err_ctx())
    end.


%% @private
-spec prepare_changed_or_created_event(space_files_monitor:file_changed_or_created_event(), state()) ->
    json_utils:json_map().
prepare_changed_or_created_event(#file_changed_or_created_event{
    file_guid = FileGuid,
    parent_file_guid = ParentGuid,
    doc_type = DocType,
    file_attr = FileAttr
}, State) ->
    FilesMonitoringSpec = State#state.files_monitoring_spec,
    ObservedAttrsPerDoc = FilesMonitoringSpec#space_files_monitoring_spec.observed_attrs_per_doc,
    ObservedAttrs = maps:get(DocType, ObservedAttrsPerDoc),
    FileAttrJson = file_attr_translator:to_json(FileAttr, current, ObservedAttrs),

    #{
        <<"parentFileId">> => ?check(file_id:guid_to_objectid(ParentGuid)),
        <<"fileId">> => ?check(file_id:guid_to_objectid(FileGuid)),
        <<"attributes">> => FileAttrJson
    }.
