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
-export([
    init/2, terminate/3,
    allowed_methods/2, is_authorized/2,
    content_types_accepted/2
]).

%% resource functions
-export([stream_file_events/2]).

-type state() :: map().


%%%===================================================================
%%% API
%%%===================================================================


-spec init(cowboy_req:req(), term()) ->
    {cowboy_rest, cowboy_req:req(), state()}.
init(Req, _Opts) ->
    {cowboy_rest, Req, #{}}.


-spec terminate(Reason :: term(), cowboy_req:req(), state()) -> ok.
terminate(_, _, #{changes_stream := Stream, loop_pid := Pid, ref := Ref}) ->
    couchbase_changes:cancel_stream(Stream),
    Pid ! {Ref, stream_ended};
terminate(_, _, #{changes_stream := Stream}) ->
    couchbase_changes:cancel_stream(Stream);
terminate(_, _, #{}) ->
    ok.


-spec allowed_methods(cowboy_req:req(), state() | {error, term()}) ->
    {[binary()], cowboy_req:req(), state()}.
allowed_methods(Req, State) ->
    {[<<"POST">>], Req, State}.


-spec is_authorized(cowboy_req:req(), state()) ->
    {true | stop, cowboy_req:req(), state()}.
is_authorized(Req, State) ->
    AuthCtx = #http_auth_ctx{
        interface = rest,
        data_access_caveats_policy = disallow_data_access_caveats
    },
    case http_auth:authenticate(Req, AuthCtx) of
        {ok, Auth = ?USER(UserId, SessionId)} ->
            SpaceId = cowboy_req:binding(sid, Req),

            case preauthorize(SpaceId, Auth) of
                ok ->
                    {true, Req, State#{user_id => UserId, session_id => SessionId, space_id => SpaceId}};
                {error, _} = Error ->
                    {stop, http_req:send_error(Error, Req), State}
            end;
        {ok, ?GUEST} ->
            {stop, http_req:send_error(?ERR_UNAUTHORIZED(?err_ctx(), undefined), Req), State};
        ?ERR = Error ->
            {stop, http_req:send_error(Error, Req), State}
    end.


-spec content_types_accepted(cowboy_req:req(), state()) ->
    {[{binary(), atom()}], cowboy_req:req(), state()}.
content_types_accepted(Req, State) ->
    {[
        {<<"application/json">>, stream_file_events}
    ], Req, State}.


%%%===================================================================
%%% Content type handler functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks space membership and api auth. Access to observed directories
%% will be verified later, after sanitization.
%% @end
%%--------------------------------------------------------------------
-spec preauthorize(od_space:id(), aai:auth()) -> ok | errors:error().
preauthorize(SpaceId, ?USER = Auth) ->
    case middleware_utils:is_eff_space_member(Auth, SpaceId) of
        true ->
            GRI = #gri{type = op_metrics, id = SpaceId, aspect = file_events},
            ?catch_exceptions(api_auth:check_authorization(Auth, ?OP_WORKER, create, GRI));
        false ->
            ?ERR_FORBIDDEN(?err_ctx())
    end.


%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/spaces/:sid/events/files'
%% @doc
%% This method streams file events happening in space filtering only
%% relevant/requested ones.
%% @end
%%--------------------------------------------------------------------
-spec stream_file_events(cowboy_req:req(), state()) ->
    {term(), cowboy_req:req(), state()}.
stream_file_events(Req, State = #{space_id := SpaceId, session_id := SessionId}) ->
    try
        middleware_utils:assert_space_supported_locally(SpaceId),

        {SpaceFilesMonitoringSpec, Req2} = space_files_monitoring_spec:parse_and_validate(
            SpaceId, SessionId, Req
        ),

        State2 = State#{space_files_monitoring_spec => SpaceFilesMonitoringSpec},
        State3 = init_stream(State2),
        Req3 = cowboy_req:stream_reply(
            ?HTTP_200_OK, #{?HDR_CONTENT_TYPE => <<"application/json">>}, Req2
        ),
        stream_loop(Req3, State3),
        cowboy_req:stream_body(<<"">>, fin, Req3),

        {stop, Req3, State3}
    catch Class:Reason:Stacktrace ->
        Error = ?examine_exception(Class, Reason, Stacktrace),
        {stop, http_req:send_error(Error, Req), State}
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec init_stream(state()) -> state().
init_stream(State = #{space_id := SpaceId, space_files_monitoring_spec := SpaceFilesMonitoringSpec}) ->
    ?info("[ space file events ]: Starting stream"),

    Pid = self(),
    Ref = make_ref(),
    Triggers = maps:keys(SpaceFilesMonitoringSpec#space_files_monitoring_spec.observed_attrs_per_doc),
    Since = dbsync_state:get_seq(SpaceId, oneprovider:get_id()),

    % TODO VFS-5570
    % TODO VFS-6389 - maybe, instead of aborting http connection on Node failure
    % (stream process will die and in turn kill this one - terminate),
    % try to restart couchbase_changes stream on different node
    Node = datastore_key:any_responsible_node(SpaceId),
    {ok, Stream} = rpc:call(Node, couchbase_changes, stream, [
        <<"onedata">>,
        SpaceId,
        fun(Feed) -> notify_http_conn_proc(Pid, Ref, Triggers, Feed) end,
        [{since, Since}],
        [Pid]
    ]),

    State#{changes_stream => Stream, ref => Ref, loop_pid => Pid}.


%% @private
-spec stream_loop(cowboy_req:req(), state()) -> ok.
stream_loop(Req, State = #{
    changes_stream := Stream,
    ref := Ref,
    session_id := SessionId,
    space_files_monitoring_spec := SpaceFilesMonitoringSpec
}) ->
    receive
        {Ref, stream_ended} ->
            ok;
        {Ref, ChangedDocs} when is_list(ChangedDocs) ->
            Stream ! {Ref, ok},
            UserCtx = user_ctx:new(SessionId),
            lists:foreach(fun(ChangedDoc) ->
                try
                    case process_doc(UserCtx, ChangedDoc, SpaceFilesMonitoringSpec) of
                        ok ->
                            ok;
                        {ok, Event} ->
                            Response = json_utils:encode(Event),
                            cowboy_req:stream_body(<<Response/binary, "\r\n">>, nofin, Req)
                    end
                catch Class:Reason:Stacktrace ->
                    % Can appear when document connected with deleted file_meta appears
                    ?debug_exception("Cannot stream file event of ~tp", [ChangedDoc], Class, Reason, Stacktrace)
                end
            end, ChangedDocs),
            stream_loop(Req, State);
        Msg ->
            ?log_bad_request(Msg),
            stream_loop(Req, State)
    after infinity ->
        % TODO VFS-4025 - is it always ok?
        ok
    end.


%% @private
-spec process_doc(user_ctx:ctx(), datastore:doc(), space_files_monitoring_spec:t()) ->
    ok | {ok, json_utils:json_map()}.
process_doc(UserCtx, ChangedDoc, SpaceFilesMonitoringSpec) ->
    FileCtx = get_file_ctx(ChangedDoc),

    case is_child_of_observed_dir(UserCtx, FileCtx, SpaceFilesMonitoringSpec) of
        {true, FileCtx2, ParentGuid} ->
            FileGuid = file_ctx:get_logical_guid_const(FileCtx2),

            ObservedAttrs = get_doc_observed_attrs(ChangedDoc, SpaceFilesMonitoringSpec),
            try
                {FileAttr, _FileCtx3} = file_attr:resolve(UserCtx, FileCtx2, #{attributes => ObservedAttrs}),
                FileAttrJson = file_attr_translator:to_json(FileAttr, current, ObservedAttrs),

                {ok, #{
                    <<"eventType">> => <<"changedOrCreated">>,
                    <<"eventId">> => str_utils:to_binary(ChangedDoc#document.seq),
                    <<"parentFileId">> => ?check(file_id:guid_to_objectid(ParentGuid)),
                    <<"fileId">> => ?check(file_id:guid_to_objectid(FileGuid)),
                    <<"data">> => FileAttrJson
                }}
            catch
                throw:Errno when is_atom(Errno) ->
                    throw(?ERR_POSIX(?err_ctx(), Errno));
                Class:Reason:Stacktrace ->
                    Error = ?examine_exception(Class, Reason, Stacktrace),
                    throw(Error)
            end;
        {false, _, _} ->
            ok
    end.


%% @private
-spec get_file_ctx(datastore:doc()) -> file_ctx:ctx().
get_file_ctx(ChangedDoc = #document{value = #times{}}) ->
    file_ctx:new_by_uuid(ChangedDoc#document.key, ChangedDoc#document.scope);
get_file_ctx(ChangedDoc = #document{value = #file_meta{}}) ->
    file_ctx:new_by_doc(ChangedDoc, ChangedDoc#document.scope);
get_file_ctx(ChangedDoc = #document{value = #custom_metadata{}}) ->
    file_ctx:new_by_uuid(ChangedDoc#document.key, ChangedDoc#document.scope);
get_file_ctx(ChangedDoc = #document{value = #file_location{uuid = FileUUid}}) ->
    file_ctx:new_by_uuid(FileUUid, ChangedDoc#document.scope).


%% @private
-spec is_child_of_observed_dir(user_ctx:ctx(), file_ctx:ctx(), space_files_monitoring_spec:t()) ->
    {boolean(), file_ctx:ctx(), undefined | file_id:file_guid()}.
is_child_of_observed_dir(UserCtx, FileCtx, #space_files_monitoring_spec{observed_dirs = ObservedDirGuids}) ->
    {ParentCtx, FileCtx2} = file_tree:get_parent(FileCtx, UserCtx),

    case file_ctx:equals(FileCtx, ParentCtx) of
        true ->
            {false, FileCtx2, undefined};
        false ->
            ParentGuid = file_ctx:get_logical_guid_const(ParentCtx),
            {lists:member(ParentGuid, ObservedDirGuids), FileCtx2, ParentGuid}
    end.


%% @private
-spec get_doc_observed_attrs(datastore:doc(), space_files_monitoring_spec:t()) -> [onedata_file:attr_name()].
get_doc_observed_attrs(
    #document{value = Record},
    #space_files_monitoring_spec{observed_attrs_per_doc = ObservedAttrsPerDoc}
) ->
    maps:get(utils:record_type(Record), ObservedAttrsPerDoc).


%% @private
-spec notify_http_conn_proc(pid(), reference(), space_monitoring_stream_processor:triggers(),
    {ok, [datastore:doc()] | datastore:doc() | end_of_stream} |
    {error, couchbase_changes:since(), term()}) -> ok.
notify_http_conn_proc(Pid, Ref, Triggers, {ok, {change, #document{} = Doc}}) ->
    case is_observed_doc(Doc, Triggers) of
        true ->
            call_space_file_events_monitoring_stream_handler(Pid, Ref, [Doc]);
        false ->
            ok
    end,
    ok;
notify_http_conn_proc(Pid, Ref, Triggers, {ok, Docs}) when is_list(Docs) ->
    case lists:filtermap(fun({change, Doc}) ->
        case is_observed_doc(Doc, Triggers) of
            true -> {true, Doc};
            false -> false
        end
    end, Docs) of
        [] ->
            ok;
        RelevantDocs ->
            call_space_file_events_monitoring_stream_handler(Pid, Ref, RelevantDocs)
    end,
    ok;
notify_http_conn_proc(Pid, Ref, _Triggers, {ok, end_of_stream}) ->
    Pid ! {Ref, stream_ended},
    ok;
notify_http_conn_proc(Pid, Ref, _Triggers, {error, _Seq, shutdown = Reason}) ->
    ?debug("Changes stream terminated due to: ~tp", [Reason]),
    Pid ! {Ref, stream_ended},
    ok;
notify_http_conn_proc(Pid, Ref, _Triggers, {error, _Seq, Reason}) ->
    ?error("Changes stream terminated abnormally due to: ~tp", [Reason]),
    Pid ! {Ref, stream_ended},
    ok.


%% @private
-spec is_observed_doc(datastore:doc(), space_monitoring_stream_processor:triggers()) -> boolean().
is_observed_doc(#document{value = Record}, Triggers) when is_tuple(Record) ->
    lists:member(element(1, Record), Triggers);
is_observed_doc(_Doc, _Triggers) ->
    false.


%% @private
call_space_file_events_monitoring_stream_handler(Pid, Ref, Msg) ->
    Pid ! {Ref, Msg},
    receive
        {Ref, ok} ->
            ok
    end,
    ok.
