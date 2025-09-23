%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Space File Events - real-time streaming of file changes in a space.
%%%
%%% This module implements a monitoring system that allows clients to subscribe
%%% to file change events within specific directories of a space. When files
%%% are created or modified, subscribers receive real-time notifications via
%%% Server-Sent Events (SSE).
%%%
%%% == How it works ==
%%%
%%% 1. Client connects via HTTP SSE to `space_file_events_stream_handler`
%%% 2. Handler authenticates user and parses monitoring specification
%%% 3. Handler ensures a space monitor exists and subscribes to it
%%% 4. Monitor observes Couchbase changes and filters relevant events
%%% 5. Events are sent to authorized subscribers via SSE stream
%%%
%%% == What gets monitored ==
%%%
%%% The system monitors three types of document changes:
%%% - `file_meta`
%%% - `times`
%%% - `file_location`
%%%
%%% === Monitoring Scope ===
%%%
%%% Only direct children of specified directories are monitored - there is
%%% no recursive monitoring of subdirectories. To monitor files in nested
%%% directories, each subdirectory must be explicitly included in the
%%% subscription specification.
%%%
%%% == Subscription specification ==
%%%
%%% Clients specify what they want to monitor via:
%%% - `observed_dirs` - list of directory GUIDs to watch
%%% - `observed_attrs_per_doc` - which file attributes to include for each
%%%   document type (see `?OBSERVABLE_FILE_ATTRS` in
%%%   `include/http/space_file_events_stream.hrl`)
%%%
%%% == Process architecture ==
%%%
%%% One monitor process per space:
%%% - Started on-demand by `space_files_monitor_sup`
%%% - Runs a Couchbase changes stream from the last known sequence
%%% - Multiple clients can subscribe to the same monitor
%%% - Monitor dies when inactive for `?INACTIVITY_PERIOD_MS`
%%%
%%% == Security ==
%%%
%%% Two-level authorization:
%%% 1. Space-level: user must be space member with `file_events` permission
%%% 2. File-level: for each event, user needs `?TRAVERSE_ANCESTORS` and
%%%    permissions for requested attributes
%%%
%%% == Failure handling ==
%%%
%%% - Client disconnect: doesn't affect monitor or other clients
%%% - Monitor crash: all connected clients are disconnected and must reconnect
%%% - Couchbase stream error: monitor shuts down, clients must reconnect
%%%
%%% == Event format ==
%%%
%%% The system generates two types of events sent via SSE:
%%%
%%% === File Changed/Created Events ===
%%% - `id` - unique sequence number from Couchbase changes feed
%%% - `event` - "changedOrCreated"
%%% - `data` - JSON containing:
%%%   - `fileId` - GUID of the affected file
%%%   - `parentFileId` - GUID of the parent directory
%%%   - `attributes` - requested file attributes as specified in the subscription
%%%
%%% === File Deleted Events ===
%%% - `id` - unique sequence number from Couchbase changes feed
%%% - `event` - "deleted"
%%% - `data` - JSON containing:
%%%   - `fileId` - GUID of the deleted file
%%%   - `parentFileId` - GUID of the parent directory
%%%
%%% == Performance notes ==
%%%
%%% Authorization checks are parallelized up to `?MAX_AUTHZ_VERIFY_PROCS`
%%% concurrent processes to avoid blocking on permission verification.
%%%
%%% == Important Caveats and Limitations ==
%%%
%%% Due to the architecture based on Couchbase changes feed, several important
%%% behaviors and edge cases must be understood:
%%%
%%% === Document Field Granularity ===
%%% Individual document fields are not monitored separately, and the change
%%% notification does not indicate which specific field was modified. The system
%%% only knows that *some* field in the document changed. Therefore:
%%% - All observed attributes for a document type are sent to clients, even if
%%%   only a non-observed field was actually modified
%%% - Clients must compare received data with their cached state to determine
%%%   what actually changed
%%% - This may result in "false positive" change notifications
%%%
%%% === Event Ordering and Race Conditions ===
%%% The Couchbase changes feed does not guarantee strict ordering across different
%%% document types. Specifically:
%%% - A `deleted` event is triggered by changes to the `file_meta` document
%%% - Changes to related documents (`times`, `file_location`) may arrive after
%%%   the deletion notification
%%% - Clients may receive `changed_or_created` events for a file that was already
%%%   reported as deleted
%%% - **Important**: Always verify file existence before processing change events
%%%
%%% === Duplicate Deletion Events ===
%%% A `file_meta` document may continue to be modified even after being marked
%%% as deleted, appearing multiple times in the changes feed. This means:
%%% - The same file deletion may trigger multiple `deleted` events
%%% - Clients should be idempotent when handling deletion notifications
%%% - Subsequent deletion events for the same file should be ignored
%%%
%%% == Client Implementation Guidelines ==
%%%
%%% @see space_file_events_stream_handler
%%% @see space_files_monitor_sup
%%% @see include/http/space_file_events_stream.hrl
%%% @end
%%%--------------------------------------------------------------------
-module(space_files_monitor).
-author("Bartosz Walkowicz").

-behaviour(gen_server).

-include("http/space_file_events_stream.hrl").
-include("middleware/middleware.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include("timeouts.hrl").

%% API
-export([start_link/1]).
-export([subscribe_link/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-type doc_type() :: file_meta | times | file_location.

-record(observer, {
    session_id :: session:id(),
    files_monitoring_spec :: space_files_monitoring_spec:t()
}).
-type observer() :: #observer{}.

-type observed_attrs_per_doc() :: #{
    doc_type() => [onedata_file:attr_name()]
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

-record(process_doc_ctx, {
    root_user_ctx :: user_ctx:ctx(),
    file_ctx :: file_ctx:ctx(),
    parent_guid :: file_id:file_guid(),
    dir_monitoring_spec :: dir_monitoring_spec(),
    changed_doc :: datastore:doc(),
    state :: state()
}).
-type process_doc_ctx() :: #process_doc_ctx{}.

-type file_deleted_event() :: #file_deleted_event{}.

-type file_changed_or_created_event() :: #file_changed_or_created_event{}.

-type event_type() :: deleted | changed_or_created.

-type event() :: file_deleted_event() | file_changed_or_created_event().

-export_type([
    observed_attrs_per_doc/0,
    file_deleted_event/0, file_changed_or_created_event/0, event/0
]).


%% The process is supposed to die after ?INACTIVITY_PERIOD_MS time of idling (no subscribers)
-define(INACTIVITY_PERIOD_MS, 10_000).

%% Maximum number of concurrent processes verifying whether subscribed observers
%% can see produced events
-define(MAX_AUTHZ_VERIFY_PROCS, op_worker:get_env(
    max_authorize_space_files_observers_procs, 20
)).


%%%===================================================================
%%% API
%%%===================================================================


-spec start_link(od_space:id()) -> {ok, pid()} | ignore | {error, Reason :: term()}.
start_link(SpaceId) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [SpaceId], []).


-spec subscribe_link(pid(), session:id(), space_files_monitoring_spec:t()) ->
    ok | errors:error().
subscribe_link(MonitorPid, SessionId, FilesMonitoringSpec) ->
    call_monitor(MonitorPid, #subscribe_req{
        pid = self(),
        session_id = SessionId,
        files_monitoring_spec = FilesMonitoringSpec
    }).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================


-spec init([od_space:id()]) -> {ok, state(), non_neg_integer()}.
init([SpaceId]) ->
    process_flag(trap_exit, true),

    ?info("[ space file events ]: Starting monitor for space '~ts'", [SpaceId]),

    Self = self(),
    SinceSeq = dbsync_state:get_seq(SpaceId, oneprovider:get_id()),

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

    State = #state{
        space_id = SpaceId,
        changes_stream_pid = ChangesStreamPid
    },
    {ok, State, ?INACTIVITY_PERIOD_MS}.


-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()}, state()) ->
    {reply, Reply :: term(), state()} |
    {noreply, state()} |
    {noreply, state(), non_neg_integer()}.
handle_call(SubscribeReq = #subscribe_req{}, _From, State = #state{}) ->
    Pid = SubscribeReq#subscribe_req.pid,

    case maps:is_key(Pid, State#state.observers) of
        true ->
            {reply, ?ERROR_ALREADY_EXISTS, State};
        false ->
            erlang:link(Pid),

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
    noreply_with_timeout_if_no_subscribers(State).


-spec handle_cast(Request :: term(), state()) ->
    {noreply, state()} |
    {noreply, state(), non_neg_integer()}.
handle_cast(Request, #state{} = State) ->
    ?log_bad_request(Request),
    noreply_with_timeout_if_no_subscribers(State).


-spec handle_info(timeout() | term(), state()) ->
    {noreply, state()} |
    {noreply, state(), non_neg_integer()} |
    {stop, term(), state()}.
handle_info({'EXIT', ObserverPid, _Reason}, State = #state{}) ->
    noreply_with_timeout_if_no_subscribers(remove_observer(State, ObserverPid));

handle_info(stream_ended, State = #state{}) ->
    {stop, {shutdown, stream_ended}, State};

handle_info(timeout, State = #state{}) ->
    case maps_utils:is_empty(State#state.observers) of
        true ->
            ?info(
                "[ space file events ]: Stopping monitor for space '~ts' due to inactivity",
                [State#state.space_id]
            ),

            ?debug("Exiting due to inactivity with state: ~tp", [State]),
            {stop, {shutdown, timeout}, State};
        false ->
            {noreply, State}
    end;

handle_info(Info, #state{} = State) ->
    ?log_bad_request(Info),
    noreply_with_timeout_if_no_subscribers(State).


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
            ?report_internal_server_error("Process '~tp' does not exist", [?MODULE]);
        exit:{normal, _} ->
            ?report_internal_server_error("Exit of '~tp' process", [?MODULE]);
        exit:{timeout, _} ->
            ?report_internal_server_error("Timeout of '~tp' process", [?MODULE]);
        Class:Reason:Stacktrace ->
            ?examine_exception("Cannot call space file monitor", Class, Reason, Stacktrace)
    end.


%% @private
-spec noreply_with_timeout_if_no_subscribers(state()) ->
    {noreply, state()} |
    {noreply, state(), non_neg_integer()}.
noreply_with_timeout_if_no_subscribers(State) ->
    case maps_utils:is_empty(State#state.observers) of
        true -> {noreply, State, ?INACTIVITY_PERIOD_MS};
        false -> {noreply, State}
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
                            observed_attrs_per_doc = gather_observed_attrs_per_doc(
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
-spec gather_observed_attrs_per_doc([pid()], state()) -> observed_attrs_per_doc().
gather_observed_attrs_per_doc(ObserverPids, #state{observers = Observers}) ->
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

    case is_observed_file(RootUserCtx, FileCtx, State) of
        {true, FileCtx2, ParentGuid, DirMonitoringSpec} ->
            Ctx1 = #process_doc_ctx{
                root_user_ctx = RootUserCtx,
                file_ctx = FileCtx2,
                parent_guid = ParentGuid,
                dir_monitoring_spec = DirMonitoringSpec,
                changed_doc = ChangedDoc,
                state = State
            },
            {EventType, Ctx2} = infer_event_type(Ctx1),

            case get_authorized_observers(EventType, Ctx2) of
                [] ->
                    ok;
                ObserverPids ->
                    Event = gen_event(EventType, Ctx2),
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
get_file_ctx(ChangedDoc = #document{value = #file_location{uuid = FileUUid}}) ->
    file_ctx:new_by_uuid(FileUUid, ChangedDoc#document.scope).


%% @private
-spec is_observed_file(user_ctx:ctx(), file_ctx:ctx(), state()) ->
    {true, file_ctx:ctx(), file_id:file_guid(), dir_monitoring_spec()} | false.
is_observed_file(UserCtx, FileCtx, State) ->
    {ParentCtx, FileCtx2} = file_tree:get_parent(FileCtx, UserCtx),

    case file_ctx:equals(FileCtx, ParentCtx) of
        true ->
            false;
        false ->
            ParentGuid = file_ctx:get_logical_guid_const(ParentCtx),

            case maps:find(ParentGuid, State#state.dir_monitoring_specs) of
                {ok, DirMonitoringSpec = #dir_monitoring_spec{}} ->
                    {true, FileCtx2, ParentGuid, DirMonitoringSpec};
                error ->
                    false
            end
    end.


%% @private
-spec infer_event_type(process_doc_ctx()) ->
    {event_type(), process_doc_ctx()}.
infer_event_type(Ctx = #process_doc_ctx{changed_doc = #document{value = #file_meta{}}}) ->
    FileCtx1 = Ctx#process_doc_ctx.file_ctx,
    case file_ctx:file_exists_or_is_deleted(FileCtx1) of
        {?FILE_DELETED, FileCtx2} ->
            {deleted, Ctx#process_doc_ctx{file_ctx = FileCtx2}};
        {_, FileCtx2} ->
            {changed_or_created, Ctx#process_doc_ctx{file_ctx = FileCtx2}}
    end;

infer_event_type(Ctx) ->
    {changed_or_created, Ctx}.


%% @private
-spec get_authorized_observers(event_type(), process_doc_ctx()) -> [pid()].
get_authorized_observers(deleted, Ctx) ->
    get_authorized_deleted_event_observers(Ctx);
get_authorized_observers(changed_or_created, Ctx) ->
    get_authorized_changed_or_created_event_observers(Ctx).


%% @private
-spec gen_event(event_type(), process_doc_ctx()) -> event().
gen_event(deleted, Ctx) ->
    gen_deleted_event(Ctx);
gen_event(changed_or_created, Ctx) ->
    gen_changed_or_created_event(Ctx).


%% @private
-spec get_authorized_deleted_event_observers(process_doc_ctx()) -> [pid()].
get_authorized_deleted_event_observers(#process_doc_ctx{
    file_ctx = FileCtx,
    dir_monitoring_spec = DirMonitoringSpec,
    state = State
}) ->
    AllDirObservers = DirMonitoringSpec#dir_monitoring_spec.observers,

    FilterMapFun = fun(DirObserverPid) ->
        try
            DirObserver = maps:get(DirObserverPid, State#state.observers),
            DirObserverUserCtx = user_ctx:new(DirObserver#observer.session_id),
            fslogic_authz:ensure_authorized(
                DirObserverUserCtx, FileCtx, [?TRAVERSE_ANCESTORS]
            ),
            {true, DirObserverPid}
        catch _:_ ->
            false
        end
    end,
    lists_utils:pfiltermap(FilterMapFun, AllDirObservers, ?MAX_AUTHZ_VERIFY_PROCS).


%% @private
-spec gen_deleted_event(process_doc_ctx()) -> file_deleted_event().
gen_deleted_event(#process_doc_ctx{
    file_ctx = FileCtx,
    parent_guid = ParentGuid,
    changed_doc = ChangedDoc
}) ->
    #file_deleted_event{
        id = str_utils:to_binary(ChangedDoc#document.seq),
        file_guid = file_ctx:get_logical_guid_const(FileCtx),
        parent_file_guid = ParentGuid
    }.


%% @private
-spec get_authorized_changed_or_created_event_observers(process_doc_ctx()) -> [pid()].
get_authorized_changed_or_created_event_observers(#process_doc_ctx{
    file_ctx = FileCtx,
    dir_monitoring_spec = DirMonitoringSpec,
    changed_doc = ChangedDoc,
    state = State
}) ->
    AllDirObservers = DirMonitoringSpec#dir_monitoring_spec.observers,

    FilterMapFun = fun(DirObserverPid) ->
        ChangedDocType = utils:record_type(ChangedDoc#document.value),
        DirObserver = maps:get(DirObserverPid, State#state.observers),

        case get_observed_attrs_for_doc(ChangedDocType, DirObserver) of
            undefined ->
                false;

            ObservedAttrs ->
                DirObserverUserCtx = user_ctx:new(DirObserver#observer.session_id),

                try
                    RequiredPerms = [
                        ?TRAVERSE_ANCESTORS,
                        ?OPERATIONS(file_attr:optional_attrs_perms_mask(ObservedAttrs))
                    ],
                    fslogic_authz:ensure_authorized(
                        DirObserverUserCtx, FileCtx, RequiredPerms
                    ),
                    {true, DirObserverPid}
                catch _:_ ->
                    false
                end
        end
    end,
    lists_utils:pfiltermap(FilterMapFun, AllDirObservers, ?MAX_AUTHZ_VERIFY_PROCS).


%% @private
-spec get_observed_attrs_for_doc(doc_type(), observer()) -> undefined | [onedata_file:attr_name()].
get_observed_attrs_for_doc(ChangedDocType, #observer{
    files_monitoring_spec = #space_files_monitoring_spec{observed_attrs_per_doc = ObservedAttrsPerDoc}
}) ->
    maps:get(ChangedDocType, ObservedAttrsPerDoc, undefined).


%% @private
-spec gen_changed_or_created_event(process_doc_ctx()) -> file_changed_or_created_event().
gen_changed_or_created_event(#process_doc_ctx{
    root_user_ctx = RootUserCtx,
    file_ctx = FileCtx,
    parent_guid = ParentGuid,
    dir_monitoring_spec = #dir_monitoring_spec{observed_attrs_per_doc = ObservedAttrsPerDoc},
    changed_doc = ChangedDoc
}) ->
    ChangedDocType = utils:record_type(ChangedDoc#document.value),
    ObservedAttrs = maps:get(ChangedDocType, ObservedAttrsPerDoc),
    {FileAttr, _FileCtx3} = file_attr:resolve(RootUserCtx, FileCtx, #{
        attributes => ObservedAttrs,
        name_conflicts_resolution_policy => allow_name_conflicts
    }),
    #file_changed_or_created_event{
        id = str_utils:to_binary(ChangedDoc#document.seq),
        file_guid = file_ctx:get_logical_guid_const(FileCtx),
        parent_file_guid = ParentGuid,
        doc_type = utils:record_type(ChangedDoc#document.value),
        file_attr = FileAttr
    }.


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
    case is_observable_doc(Doc) of
        true ->
            notify_monitor_about_doc_change(Pid, [Doc]);
        false ->
            ok
    end,
    ok;
notify_monitor(Pid, {ok, Docs}) when is_list(Docs) ->
    case lists:filtermap(fun({change, Doc}) ->
        case is_observable_doc(Doc) of
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


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if a document change is relevant for space file events monitoring.
%% Returns true for file-related documents that are not deleted, except for
%% file_meta documents which are used to generate deletion events.
%% @end
%%--------------------------------------------------------------------
-spec is_observable_doc(datastore:doc()) -> boolean().
is_observable_doc(#document{value = #file_meta{}}) -> true;
is_observable_doc(#document{deleted = true}) -> false;
is_observable_doc(#document{value = #times{}}) -> true;
is_observable_doc(#document{value = #file_location{}}) -> true;
is_observable_doc(_Doc) -> false.


%% @private
-spec notify_monitor_about_doc_change(pid(), [datastore:doc()]) -> ok.
notify_monitor_about_doc_change(Pid, Docs) ->
    call_monitor(Pid, #docs_change_notification{docs = Docs}),
    ok.
