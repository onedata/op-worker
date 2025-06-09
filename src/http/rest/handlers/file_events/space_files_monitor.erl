%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Server observing changes in space files and informing subscribed clients.
%%% TODO VFS-12886 add inactivity DIE_TIMEOUT - similar to replica_synchronizer
%%% @end
%%%--------------------------------------------------------------------
-module(space_files_monitor).
-author("cyfrinet").

-behaviour(gen_server).

-include("http/space_file_events_stream.hrl").
-include("middleware/middleware.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include("timeouts.hrl").

%% API
-export([start_link/1]).
-export([subscribe/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(observer, {
    session_id :: session:id(),
    files_monitoring_spec :: space_files_monitoring_spec:t()
}).
-type observer() :: #observer{}.

-type observed_attrs_per_doc() :: #{
    file_meta => [onedata_file:attr_name()],
    times => [onedata_file:attr_name()],
    file_location => [onedata_file:attr_name()]
}.

-record(dir_monitoring_spec, {
    observers :: [pid()],
    observed_attrs_per_doc :: observed_attrs_per_doc()
}).
-type dir_monitoring_spec() :: #dir_monitoring_spec{}.

-record(state, {
    space_id :: od_space:id(),
    changes_stream_pid :: pid(),
    observers = #{} :: #{pid() => observer()},
    dir_monitoring_specs = #{} :: #{file_id:file_guid() => dir_monitoring_spec()}
}).
-type state() ::#state{}.

-record(subscribe_req, {
    pid :: pid(),
    session_id :: session:id(),
    files_monitoring_spec :: space_files_monitoring_spec:t()
}).

-record(docs_change_notification, {
    docs :: [datastore:doc()]
}).

-type request() :: #subscribe_req{} | #docs_change_notification{}.

-type file_changed_or_created_event() :: #file_changed_or_created_event{}.

-type event() :: file_changed_or_created_event().

-export_type([
    observed_attrs_per_doc/0,
    file_changed_or_created_event/0, event/0
]).


-define(OBSERVABLE_FILE_DOCS, [file_meta, times, file_location]).

-define(MAX_AUTHORIZE_OBSERVERS_PROCS, op_worker:get_env(
    max_authorize_space_files_observers_procs, 20
)).


%%%===================================================================
%%% API
%%%===================================================================


-spec start_link(od_space:id()) -> {ok, pid()} | ignore | {error, Reason :: term()}.
start_link(SpaceId) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [SpaceId], []).


-spec subscribe(pid(), session:id(), space_files_monitoring_spec:t()) ->
    ok | errors:error().
subscribe(MonitorPid, SessionId, FilesMonitoringSpec) ->
    call_monitor(MonitorPid, #subscribe_req{
        pid = self(),
        session_id = SessionId,
        files_monitoring_spec = FilesMonitoringSpec
    }).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================


-spec init([od_space:id()]) -> {ok, state()}.
init([SpaceId]) ->
    process_flag(trap_exit, true),

    ?info("[ space file events ]: Starting monitor for space '~ts'", [SpaceId]),

    Self = self(),
    SinceSeq = dbsync_state:get_seq(SpaceId, oneprovider:get_id()),

    % TODO VFS-5570
    % TODO VFS-6389 - maybe, instead of aborting http connection on Node failure
    % (stream process will die and in turn kill this one - terminate),
    % try to restart couchbase_changes stream on different node
    {ok, ChangesStreamPid} = couchbase_changes:stream(
        <<"onedata">>,
        SpaceId,
        fun(Feed) -> notify_monitor(Self, Feed) end,
        [{since, SinceSeq}],
        [Self]
    ),

    {ok, #state{
        space_id = SpaceId,
        changes_stream_pid = ChangesStreamPid
    }}.


-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()}, state()) ->
    {reply, Reply :: term(), NewState :: #state{}} |
    {noreply, NewState :: #state{}}.
handle_call(SubscribeReq = #subscribe_req{}, _From, State = #state{}) ->
    Pid = SubscribeReq#subscribe_req.pid,
    erlang:link(Pid),

    case maps:is_key(Pid, State#state.observers) of
        true ->
            {reply, ?ERROR_ALREADY_EXISTS, State};
        false ->
            Observer = #observer{
                session_id = SubscribeReq#subscribe_req.session_id,
                files_monitoring_spec = SubscribeReq#subscribe_req.files_monitoring_spec
            },
            {reply, ok, add_observer(State, Pid, Observer)}
    end;

handle_call(#docs_change_notification{docs = ChangedDocs}, From, State) ->
    gen_server2:reply(From, ok),

    RootUserCtx = user_ctx:new(?ROOT_SESS_ID),
    lists:foreach(fun(ChangedDoc) ->
        try
            process_doc(RootUserCtx, ChangedDoc, State)
        catch Class:Reason:Stacktrace ->
            ?error_exception("[ space file events ]: Failed to process doc ", Class, Reason, Stacktrace)
        end
    end, ChangedDocs),

    {noreply, State};

handle_call(Request, _From, #state{} = State) ->
    ?log_bad_request(Request),
    {noreply, State}.


-spec handle_cast(Request :: term(), state()) ->
    {noreply, NewState :: #state{}}.
handle_cast(Request, #state{} = State) ->
    ?log_bad_request(Request),
    {noreply, State}.


-spec handle_info(timeout() | term(), state()) ->
    {noreply, #state{}}.
handle_info({'EXIT', ObserverPid, _Reason}, State = #state{}) ->
    {noreply, remove_observer(State, ObserverPid)};

handle_info(stream_ended, State = #state{}) ->
    {stop, stream_ended, State};

handle_info(Info, #state{} = State) ->
    ?log_bad_request(Info),
    {noreply, State}.


-spec terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()), state()) ->
    term().
terminate(Reason, State = #state{changes_stream_pid = ChangesStreamPid}) ->
    couchbase_changes:cancel_stream(ChangesStreamPid),
    ?log_terminate(Reason, State).


-spec code_change(OldVsn :: term() | {down, term()}, state(), Extra :: term()) ->
    {ok, state()} | {error, Reason :: term()}.
code_change(_OldVsn, State = #state{}, _Extra) ->
    {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec call_monitor(pid(), request()) -> ok | errors:error().
call_monitor(MonitorPid, Request) ->
    try
        gen_server2:call(MonitorPid, Request, ?DEFAULT_REQUEST_TIMEOUT)
    catch
        exit:{noproc, _} ->
            ?debug("Process '~tp' does not exist", [?MODULE]),
            ?ERROR_NOT_FOUND;
        exit:{normal, _} ->
            ?debug("Exit of '~tp' process", [?MODULE]),
            ?ERROR_NOT_FOUND;
        exit:{timeout, _} ->
            ?debug("Timeout of '~tp' process", [?MODULE]),
            ?ERROR_TIMEOUT;
        Class:Reason:Stacktrace ->
            ?examine_exception("Cannot call space file monitor", Class, Reason, Stacktrace)
    end.


%% @private
-spec add_observer(state(), pid(), observer()) -> state().
add_observer(State, ObserverPid, Observer = #observer{files_monitoring_spec = FilesMonitoringSpec}) ->
    DirsToObserve = FilesMonitoringSpec#space_files_monitoring_spec.observed_dirs,
    AttrsToObservePerDoc = FilesMonitoringSpec#space_files_monitoring_spec.observed_attrs_per_doc,

    NewDirMonitoringSpecs = lists:foldl(fun(DirToObserveGuid, DirMonitoringSpecsAcc) ->
        case maps:is_key(DirToObserveGuid, DirMonitoringSpecsAcc) of
            true ->
                DirMonitoringSpec = maps:get(DirToObserveGuid, DirMonitoringSpecsAcc),
                NewObservedAttrsPerDoc = update_observed_attrs_per_doc(
                    AttrsToObservePerDoc,
                    DirMonitoringSpec#dir_monitoring_spec.observed_attrs_per_doc
                ),
                DirMonitoringSpecsAcc#{DirToObserveGuid => DirMonitoringSpec#dir_monitoring_spec{
                    observers = [ObserverPid | DirMonitoringSpec#dir_monitoring_spec.observers],
                    observed_attrs_per_doc = NewObservedAttrsPerDoc
                }};
            false ->
                DirMonitoringSpecsAcc#{DirToObserveGuid => #dir_monitoring_spec{
                    observers = [ObserverPid],
                    observed_attrs_per_doc = AttrsToObservePerDoc
                }}
        end
    end, State#state.dir_monitoring_specs, DirsToObserve),

    State#state{
        observers = (State#state.observers)#{ObserverPid => Observer},
        dir_monitoring_specs = NewDirMonitoringSpecs
    }.


%% @private
-spec remove_observer(state(), pid()) -> state().
remove_observer(State, ObserverPid) ->
    case maps:take(ObserverPid, State#state.observers) of
        {#observer{files_monitoring_spec = FilesMonitoringSpec}, LeftoverObservers} ->
            ObservedDirs = FilesMonitoringSpec#space_files_monitoring_spec.observed_dirs,

            NewDirMonitoringSpecs = lists:foldl(fun(ObservedDirGuid, DirMonitoringSpecsAcc) ->
                DirMonitoringSpec = maps:get(ObservedDirGuid, DirMonitoringSpecsAcc),
                AllDirObservers = DirMonitoringSpec#dir_monitoring_spec.observers,
                case lists:delete(ObserverPid, AllDirObservers) of
                    [] ->
                        maps:remove(ObservedDirGuid, DirMonitoringSpecsAcc);
                    LeftoverDirObservers ->
                        DirMonitoringSpecsAcc#{ObservedDirGuid => #dir_monitoring_spec{
                            observers = LeftoverDirObservers,
                            observed_attrs_per_doc = gather_dir_observed_attrs_per_doc(
                                LeftoverDirObservers, State
                            )
                        }}
                end
            end, State#state.dir_monitoring_specs, ObservedDirs),

            State#state{
                observers = LeftoverObservers,
                dir_monitoring_specs = NewDirMonitoringSpecs
            };
        error ->
            State
    end.


%% @private
-spec gather_dir_observed_attrs_per_doc([pid()], state()) -> observed_attrs_per_doc().
gather_dir_observed_attrs_per_doc(ObserverPids, #state{observers = Observers}) ->
    lists:foldl(fun(ObserverPid, ObservedAttrsPerDocAcc) ->
        Observer = maps:get(ObserverPid, Observers),
        FilesMonitoringSpec = Observer#observer.files_monitoring_spec,

        update_observed_attrs_per_doc(
            FilesMonitoringSpec#space_files_monitoring_spec.observed_attrs_per_doc,
            ObservedAttrsPerDocAcc
        )
    end, #{}, ObserverPids).


%% @private
-spec update_observed_attrs_per_doc(observed_attrs_per_doc(), observed_attrs_per_doc()) ->
    observed_attrs_per_doc().
update_observed_attrs_per_doc(AttrsToObservePerDoc, ObservedAttrsPerDoc) ->
    maps:fold(fun(DocName, AttrsToObserve, Acc) ->
        DocObservedAttrs = maps:get(DocName, Acc, []),
        Acc#{DocName => lists_utils:union(AttrsToObserve, DocObservedAttrs)}
    end, ObservedAttrsPerDoc, AttrsToObservePerDoc).


%% @private
-spec process_doc(user_ctx:ctx(), datastore:doc(), state()) -> ok.
process_doc(RootUserCtx, ChangedDoc, State) ->
    FileCtx = get_file_ctx(ChangedDoc),

    case is_observed_file(RootUserCtx, FileCtx, ChangedDoc, State) of
        {true, FileCtx2, ParentGuid, ObservedAttrs} ->
            FileGuid = file_ctx:get_logical_guid_const(FileCtx2),
            DirMonitoringSpec = maps:get(ParentGuid, State#state.dir_monitoring_specs),

            case get_authorized_observers(FileCtx2, DirMonitoringSpec, State) of
                [] ->
                    ok;
                ObserverPids ->
                    {FileAttr, _FileCtx3} = file_attr:resolve(RootUserCtx, FileCtx, #{
                        attributes => ObservedAttrs
                    }),
                    Event = #file_changed_or_created_event{
                        id = str_utils:to_binary(ChangedDoc#document.seq),
                        file_guid = FileGuid,
                        parent_file_guid = ParentGuid,
                        doc_type = utils:record_type(ChangedDoc#document.value),
                        file_attr = FileAttr
                    },
                    broadcast_event(ObserverPids, Event)
            end;
        false ->
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
-spec is_observed_file(user_ctx:ctx(), file_ctx:ctx(), datastore:doc(), state()) ->
    {true, file_ctx:ctx(), file_id:file_guid(), [onedata_file:attr_name()]} | false.
is_observed_file(UserCtx, FileCtx, ChangedDoc, State) ->
    {ParentCtx, FileCtx2} = file_tree:get_parent(FileCtx, UserCtx),

    case file_ctx:equals(FileCtx, ParentCtx) of
        true ->
            false;
        false ->
            ParentGuid = file_ctx:get_logical_guid_const(ParentCtx),

            case maps:find(ParentGuid, State#state.dir_monitoring_specs) of
                {ok, #dir_monitoring_spec{observed_attrs_per_doc = ObservedAttrsPerDoc}} ->
                    ChangedDocType = utils:record_type(ChangedDoc#document.value),

                    case maps:get(ChangedDocType, ObservedAttrsPerDoc, undefined) of
                        undefined ->
                            false;
                        ObservedAttrs ->
                            {true, FileCtx2, ParentGuid, ObservedAttrs}
                    end;
                error ->
                    false
            end
    end.


%% @private
-spec get_authorized_observers(file_ctx:ctx(), dir_monitoring_spec(), state()) -> [pid()].
get_authorized_observers(FileCtx, DirMonitoringSpec, State) ->
    AllObservers = DirMonitoringSpec#dir_monitoring_spec.observers,

    FilterMapFun = fun(ObserverPid) ->
        Observer = maps:get(ObserverPid, State#state.observers),
        ObserverUserCtx = user_ctx:new(Observer#observer.session_id),

        try
            fslogic_authz:ensure_authorized(
                ObserverUserCtx, FileCtx, [?TRAVERSE_ANCESTORS]
            ),
            {true, ObserverPid}
        catch _:_ ->
            false
        end
    end,
    lists_utils:pfiltermap(FilterMapFun, AllObservers, ?MAX_AUTHORIZE_OBSERVERS_PROCS).


%% @private
-spec broadcast_event([pid()], event()) -> ok.
broadcast_event(ObserverPids, Event) ->
    lists:foreach(fun(ObserverPid) -> ObserverPid ! Event end, ObserverPids).


%% @private
-spec notify_monitor(
    pid(),
    {ok, [datastore:doc()] | datastore:doc() | end_of_stream} | {error, couchbase_changes:since(), term()}
) ->
    ok.
notify_monitor(Pid, {ok, {change, #document{} = Doc}}) ->
    case is_file_doc(Doc) of
        true ->
            notify_monitor_about_doc_change(Pid, [Doc]);
        false ->
            ok
    end,
    ok;
notify_monitor(Pid, {ok, Docs}) when is_list(Docs) ->
    case lists:filtermap(fun({change, Doc}) ->
        case is_file_doc(Doc) of
            true -> {true, Doc};
            false -> false
        end
    end, Docs) of
        [] ->
            ok;
        RelevantDocs ->
            notify_monitor_about_doc_change(Pid, RelevantDocs)
    end,
    ok;
notify_monitor(Pid, {ok, end_of_stream}) ->
    Pid ! stream_ended,
    ok;
notify_monitor(Pid, {error, _Seq, shutdown = Reason}) ->
    ?debug("Changes stream terminated due to: ~tp", [Reason]),
    Pid ! stream_ended,
    ok;
notify_monitor(Pid, {error, _Seq, Reason}) ->
    ?error("Changes stream terminated abnormally due to: ~tp", [Reason]),
    Pid ! stream_ended,
    ok.


%% @private
-spec is_file_doc(datastore:doc()) -> boolean().
is_file_doc(#document{value = Record}) when is_tuple(Record) ->
    lists:member(element(1, Record), ?OBSERVABLE_FILE_DOCS);
is_file_doc(_Doc) ->
    false.


%% @private
-spec notify_monitor_about_doc_change(pid(), [datastore:doc()]) -> ok.
notify_monitor_about_doc_change(Pid, Docs) ->
    call_monitor(Pid, #docs_change_notification{docs = Docs}),
    ok.
