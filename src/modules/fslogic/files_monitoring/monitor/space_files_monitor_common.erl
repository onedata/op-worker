%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Common helpers for main and catching space files monitors.
%%% @end
%%%-------------------------------------------------------------------
-module(space_files_monitor_common).
-author("Bartosz Walkowicz").

-include("http/space_file_events_stream.hrl").
-include("middleware/middleware.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include("timeouts.hrl").
-include_lib("ctool/include/logging.hrl").

%% Client API
-export([call_monitor/2]).

%% Monitor helpers
-export([
    start_link_changes_stream/2,
    start_link_changes_stream/3,

    has_observers/1,
    add_observer/2,
    remove_observer/2,

    process_docs/2
]).

-type subscribe_req() :: #subscribe_req{}.

-type observer() :: #observer{}.

-type doc_type() :: file_meta | times | file_location.
-type observed_attrs_per_doc() :: #{doc_type() => [onedata_file:attr_name()]}.
-type dir_monitoring_spec() :: #dir_monitoring_spec{}.

-type monitoring() :: #monitoring{}.

-type heartbeat_event() :: #heartbeat_event{}.
-type file_deleted_event() :: #file_deleted_event{}.
-type file_changed_or_created_event() :: #file_changed_or_created_event{}.

-type event_type() :: deleted | changed_or_created.
-type event() :: heartbeat_event() | file_deleted_event() | file_changed_or_created_event().

-record(process_doc_ctx, {
    root_user_ctx :: user_ctx:ctx(),
    file_ctx :: file_ctx:ctx(),
    parent_guid :: file_id:file_guid(),
    dir_monitoring_spec :: dir_monitoring_spec(),
    changed_doc :: datastore:doc(),
    monitor_ctx :: monitoring()
}).
-type process_doc_ctx() :: #process_doc_ctx{}.

-export_type([
    subscribe_req/0,
    observer/0, doc_type/0, observed_attrs_per_doc/0, dir_monitoring_spec/0, monitoring/0,
    heartbeat_event/0, file_deleted_event/0, file_changed_or_created_event/0, event/0
]).


%% Maximum number of concurrent processes verifying whether subscribed observers
%% can see produced events
-define(MAX_AUTHZ_VERIFY_PROCS, op_worker:get_env(
    space_files_observers_max_authorize_procs, 20
)).

%% Minimum difference between current sequence and observer's last_seen_seq 
%% to trigger a heartbeat event. Prevents reconnecting clients from replaying
%% events that aren't relevant to their observed directories. For example, if
%% threshold is 100 and a client hasn't received events for 100+ sequence numbers
%% (because nothing changed in their observed directories), they'll receive a
%% heartbeat to update their position and avoid replaying on reconnect.
-define(LAST_SEEN_SEQ_HEARTBEAT_THRESHOLD, op_worker:get_env(
    space_files_observers_last_seen_seq_heartbeat_threshold, 100
)).


%%%===================================================================
%%% API for clients
%%%===================================================================


-spec call_monitor(pid(), term()) -> ok | {ok, term()} | {error, Reason :: term()}.
call_monitor(MonitorPid, Request) ->
    try
        gen_server2:call(MonitorPid, Request, ?DEFAULT_REQUEST_TIMEOUT)
    catch
        exit:{noproc, _} ->
            ?report_internal_server_error("Space files monitor process does not exist");
        exit:{normal, _} ->
            ?report_internal_server_error("Exit of space files monitor process");
        exit:{timeout, _} ->
            ?report_internal_server_error("Timeout of space files monitor process");
        Class:Reason:Stacktrace ->
            ?examine_exception("Cannot call space files monitor", Class, Reason, Stacktrace)
    end.


%%%===================================================================
%%% Couchbase stream helpers
%%%===================================================================


-spec start_link_changes_stream(od_space:id(), couchbase_changes:seq()) -> pid().
start_link_changes_stream(SpaceId, SinceSeq) ->
    start_link_changes_stream(SpaceId, SinceSeq, undefined).


-spec start_link_changes_stream(
    od_space:id(),
    couchbase_changes:seq(),
    undefined | couchbase_changes:seq()
) ->
    pid().
start_link_changes_stream(SpaceId, SinceSeq, UntilSeq) ->
    MonitorPid = self(),
    StreamOpts = case UntilSeq of
        undefined -> [{since, SinceSeq}];
        UntilSeq -> [{since, SinceSeq}, {until, UntilSeq}]
    end,

    {ok, ChangesStreamPid} = couchbase_changes:stream(
        <<"onedata">>,
        SpaceId,
        fun(Feed) -> notify_monitor_callback(MonitorPid, Feed) end,
        StreamOpts,
        [MonitorPid]
    ),
    ChangesStreamPid.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Callback for Couchbase changes stream.
%% Filters and forwards relevant changes to monitor.
%% @end
%%--------------------------------------------------------------------
%% @private
-spec notify_monitor_callback(
    pid(),
    {ok, [datastore:doc()] | datastore:doc() | end_of_stream} | {error, couchbase_changes:since(), term()}
) ->
    ok.
notify_monitor_callback(Pid, {ok, {change, #document{} = Doc}}) ->
    case is_observable_doc(Doc) of
        true ->
            notify_monitor_about_doc_change(Pid, [Doc]);
        false ->
            ok
    end,
    ok;
notify_monitor_callback(Pid, {ok, Docs}) when is_list(Docs) ->
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
notify_monitor_callback(Pid, {ok, end_of_stream}) ->
    Pid ! stream_ended,
    ok;
notify_monitor_callback(Pid, {error, _Seq, shutdown = Reason}) ->
    ?debug("Changes stream terminated due to: ~tp", [Reason]),
    Pid ! stream_ended,
    ok;
notify_monitor_callback(Pid, {error, _Seq, Reason}) ->
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


%%%===================================================================
%%% Observer management
%%%===================================================================


-spec has_observers(monitoring()) -> boolean().
has_observers(#monitoring{observers = Observers}) ->
    not maps_utils:is_empty(Observers).


-spec add_observer(monitoring(), subscribe_req()) ->
    {ok, monitoring()} | errors:error().
add_observer(Monitoring, SubscribeReq) ->
    Pid = SubscribeReq#subscribe_req.observer_pid,

    case maps:is_key(Pid, Monitoring#monitoring.observers) of
        true ->
            ?ERROR_ALREADY_EXISTS;
        false ->
            erlang:link(Pid),

            Observer = #observer{
                session_id = SubscribeReq#subscribe_req.session_id,
                files_monitoring_spec = SubscribeReq#subscribe_req.files_monitoring_spec,
                last_seen_seq = case SubscribeReq#subscribe_req.since_seq of
                    undefined -> Monitoring#monitoring.current_seq;
                    Seq -> Seq
                end
            },
            {ok, add_observer(Monitoring, Pid, Observer)}
    end.


%% @private
-spec add_observer(monitoring(), pid(), observer()) -> monitoring().
add_observer(Monitoring, ObserverPid, Observer = #observer{files_monitoring_spec = FilesMonitoringSpec}) ->
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
    end, Monitoring#monitoring.dir_monitoring_specs, DirsToObserve),

    Monitoring#monitoring{
        observers = (Monitoring#monitoring.observers)#{ObserverPid => Observer},
        dir_monitoring_specs = NewDirMonitoringSpecs
    }.


-spec remove_observer(monitoring(), pid()) -> monitoring().
remove_observer(Monitoring, ObserverPid) ->
    case maps:take(ObserverPid, Monitoring#monitoring.observers) of
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
                                LeftoverDirObservers, Monitoring
                            )
                        }}
                end
            end, Monitoring#monitoring.dir_monitoring_specs, ObservedDirs),

            Monitoring#monitoring{
                observers = LeftoverObservers,
                dir_monitoring_specs = NewDirMonitoringSpecs
            };
        error ->
            Monitoring
    end.


%% @private
-spec gather_observed_attrs_per_doc([pid()], monitoring()) -> observed_attrs_per_doc().
gather_observed_attrs_per_doc(ObserverPids, #monitoring{observers = Observers}) ->
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


%%%===================================================================
%%% Document processing
%%%===================================================================


-spec process_docs([datastore:doc()], monitoring()) -> monitoring().
process_docs(ChangedDocs, Monitoring) ->
    RootUserCtx = user_ctx:new(?ROOT_SESS_ID),

    NewMonitoring = lists:foldl(fun(ChangedDoc, MonitoringAcc) ->
        UpdatedMonitoring = try
            process_doc(RootUserCtx, ChangedDoc, MonitoringAcc)
        catch Class:Reason:Stacktrace ->
            ?error_exception("[ space file events ]: Failed to process doc ", Class, Reason, Stacktrace),
            MonitoringAcc
        end,
        UpdatedMonitoring#monitoring{current_seq = ChangedDoc#document.seq}
    end, Monitoring, ChangedDocs),

    send_heartbeats_if_needed(NewMonitoring).


%% @private
-spec process_doc(user_ctx:ctx(), datastore:doc(), monitoring()) -> monitoring().
process_doc(RootUserCtx, ChangedDoc, Monitoring) ->
    FileCtx = get_file_ctx(ChangedDoc),

    case is_observed_file(RootUserCtx, FileCtx, Monitoring) of
        {true, FileCtx2, ParentGuid, DirMonitoringSpec} ->
            Ctx1 = #process_doc_ctx{
                root_user_ctx = RootUserCtx,
                file_ctx = FileCtx2,
                parent_guid = ParentGuid,
                dir_monitoring_spec = DirMonitoringSpec,
                changed_doc = ChangedDoc,
                monitor_ctx = Monitoring
            },
            {EventType, Ctx2} = infer_event_type(Ctx1),

            case get_authorized_observers(EventType, Ctx2) of
                [] ->
                    Monitoring;
                ObserverPids ->
                    Event = gen_event(EventType, Ctx2),
                    broadcast_event(ObserverPids, Event),
                    update_observers_last_seen_seq(ObserverPids, ChangedDoc#document.seq, Monitoring)
            end;

        false ->
            Monitoring
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
-spec is_observed_file(user_ctx:ctx(), file_ctx:ctx(), monitoring()) ->
    {true, file_ctx:ctx(), file_id:file_guid(), dir_monitoring_spec()} | false.
is_observed_file(UserCtx, FileCtx, Monitoring) ->
    {ParentCtx, FileCtx2} = file_tree:get_parent(FileCtx, UserCtx),

    case file_ctx:equals(FileCtx, ParentCtx) of
        true ->
            false;
        false ->
            ParentGuid = file_ctx:get_logical_guid_const(ParentCtx),

            case maps:find(ParentGuid, Monitoring#monitoring.dir_monitoring_specs) of
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
    monitor_ctx = Monitoring
}) ->
    AllDirObservers = DirMonitoringSpec#dir_monitoring_spec.observers,

    FilterMapFun = fun(DirObserverPid) ->
        try
            DirObserver = maps:get(DirObserverPid, Monitoring#monitoring.observers),
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
    monitor_ctx = Monitoring
}) ->
    AllDirObservers = DirMonitoringSpec#dir_monitoring_spec.observers,

    FilterMapFun = fun(DirObserverPid) ->
        ChangedDocType = utils:record_type(ChangedDoc#document.value),
        DirObserver = maps:get(DirObserverPid, Monitoring#monitoring.observers),

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
-spec send_heartbeats_if_needed(monitoring()) -> monitoring().
send_heartbeats_if_needed(Monitoring = #monitoring{
    current_seq = CurrentSeq,
    observers = Observers
}) ->
    SeqThreshold = ?LAST_SEEN_SEQ_HEARTBEAT_THRESHOLD,

    ObserversToHeartbeat = lists:foldl(fun({ObserverPid, Observer}, AccPids) ->
        case CurrentSeq - Observer#observer.last_seen_seq >= SeqThreshold of
            true -> [ObserverPid | AccPids];
            false -> AccPids
        end
    end, [], maps:to_list(Observers)),

    case ObserversToHeartbeat of
        [] ->
            Monitoring;
        _ ->
            HeartbeatEvent = #heartbeat_event{
                id = str_utils:to_binary(CurrentSeq)
            },
            broadcast_event(ObserversToHeartbeat, HeartbeatEvent),
            update_observers_last_seen_seq(ObserversToHeartbeat, CurrentSeq, Monitoring)
    end.


%% @private
-spec update_observers_last_seen_seq([pid()], couchbase_changes:seq(), monitoring()) ->
    monitoring().
update_observers_last_seen_seq(ObserverPids, Seq, Monitoring = #monitoring{observers = Observers}) ->
    NewObservers = lists:foldl(fun(ObserverPid, ObserversAcc) ->
        case maps:find(ObserverPid, ObserversAcc) of
            {ok, Observer} ->
                ObserversAcc#{ObserverPid => Observer#observer{last_seen_seq = Seq}};
            error ->
                ObserversAcc
        end
    end, Observers, ObserverPids),

    Monitoring#monitoring{observers = NewObservers}.


%% @private
-spec broadcast_event([pid()], event()) -> ok.
broadcast_event(ObserverPids, Event) ->
    lists:foreach(fun(ObserverPid) -> ObserverPid ! Event end, ObserverPids).
