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
    subscription :: space_files_monitoring_api:subscription()
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


-spec info(space_files_monitor_common:event(), cowboy_req:req(), state()) ->
    {ok, cowboy_req:req(), state()}.
info(Event = #heartbeat_event{}, Req, State) ->
    stream_event(build_heartbeat_sse_event(Event), Req),
    {ok, Req, State};

info(Event = #file_deleted_event{}, Req, State) ->
    stream_event(build_file_deleted_sse_event(Event), Req),
    {ok, Req, State};

info(Event = #file_changed_or_created_event{}, Req, State) ->
    stream_event(build_file_changed_or_created_sse_event(Event, State), Req),
    {ok, Req, State};

info({'EXIT', MonitorPid, Reason}, Req, State = #state{subscription = Subscription}) ->
    case space_files_monitoring_api:handle_exit(MonitorPid, Reason, Subscription) of
        {ok, NewSubscription} ->
            {ok, Req, State#state{subscription = NewSubscription}};
        stop ->
            end_stream(Req),
            {stop, Req, State}
    end;

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

    SinceSeq = get_since_seq(Req),
    {ok, Subscription} = space_files_monitoring_api:subscribe(
        SpaceId, SessionId, SpaceFilesMonitoringSpec, SinceSeq
    ),

    State = #state{
        space_id = SpaceId,
        auth = Auth,
        files_monitoring_spec = SpaceFilesMonitoringSpec,
        subscription = Subscription
    },

    Req3 = init_stream(Req2),

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
-spec get_since_seq(cowboy_req:req()) -> undefined | couchbase_changes:seq() | no_return().
get_since_seq(Req) ->
    Header = <<"last-event-id">>,

    case cowboy_req:header(Header, Req) of
        undefined ->
            undefined;
        LastEventId ->
            try binary_to_integer(LastEventId) of
                InvalidSeq when InvalidSeq < 0 ->
                    throw(?ERR_BAD_VALUE_TOO_LOW(?err_ctx(), Header, 0));
                ValidSeq ->
                    ValidSeq
            catch _:_ ->
                throw(?ERR_BAD_VALUE_INTEGER(?err_ctx(), Header))
            end
    end.


%% @private
-spec init_stream(cowboy_req:req()) -> cowboy_req:req().
init_stream(Req) ->
    Headers = #{
        ?HDR_CONTENT_TYPE => <<"text/event-stream">>,
        ?HDR_CACHE_CONTROL => <<"no-cache">>,
        ?HDR_CONNECTION => <<"keep-alive">>
    },
    cowboy_req:stream_reply(?HTTP_200_OK, Headers, Req).


%% @private
-spec stream_event(cow_sse:event(), cowboy_req:req()) -> ok.
stream_event(Event, Req) ->
    cowboy_req:stream_events(Event, nofin, Req).


%% @private
-spec end_stream(cowboy_req:req()) -> ok.
end_stream(Req) ->
    cowboy_req:stream_events(#{}, fin, Req).


%% @private
-spec build_heartbeat_sse_event(space_files_monitor_common:heartbeat_event()) ->
    cow_sse:event().
build_heartbeat_sse_event(#heartbeat_event{id = Id}) ->
    #{
        id => Id,
        event => <<"heartbeat">>,
        data => json_utils:encode(null)
    }.


%% @private
-spec build_file_deleted_sse_event(space_files_monitor_common:file_deleted_event()) ->
    cow_sse:event().
build_file_deleted_sse_event(#file_deleted_event{
    id = Id,
    file_guid = FileGuid,
    parent_file_guid = ParentGuid
}) ->
    #{
        id => Id,
        event => <<"deleted">>,
        data => json_utils:encode(#{
            <<"fileId">> => file_id:check_guid_to_objectid(FileGuid),
            <<"parentFileId">> => file_id:check_guid_to_objectid(ParentGuid)
        })
    }.


%% @private
-spec build_file_changed_or_created_sse_event(space_files_monitor_common:file_changed_or_created_event(), state()) ->
    cow_sse:event().
build_file_changed_or_created_sse_event(#file_changed_or_created_event{
    id = Id,
    file_guid = FileGuid,
    parent_file_guid = ParentGuid,
    doc_type = DocType,
    file_attr = FileAttr
}, State) ->
    ObservedAttrs = get_observed_doc_attrs(DocType, State),

    #{
        id => Id,
        event => <<"changedOrCreated">>,
        data => json_utils:encode(#{
            <<"fileId">> => file_id:check_guid_to_objectid(FileGuid),
            <<"parentFileId">> => file_id:check_guid_to_objectid(ParentGuid),
            <<"attributes">> => file_attr_translator:to_json(
                FileAttr, current, ObservedAttrs
            )
        })
    }.


%% @private
-spec get_observed_doc_attrs(file_meta | times | file_location, state()) ->
    [onedata_file:attr_name()].
get_observed_doc_attrs(DocType, #state{files_monitoring_spec = #space_files_monitoring_spec{
    observed_attrs_per_doc = ObservedAttrsPerDoc
}}) ->
    maps:get(DocType, ObservedAttrsPerDoc).
