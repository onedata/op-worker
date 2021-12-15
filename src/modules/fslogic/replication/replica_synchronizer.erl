%%%--------------------------------------------------------------------
%%% @author Konrad Zemek
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Creates a process per FileGuid/SessionId that handles remote file
%%% synchronization by *this user* (more specifically: this session),
%%% for *this file*.
%%% Note: this module operates on referenced uuids. Synchronizer is always
%%% created for original file, even if hardlink ctx is provided in argument.
%%% TODO - udpate sizes of dirs here, files count when created/deleted, handle rename (add to new, change value of old parent)
%%% @end
%%%--------------------------------------------------------------------
-module(replica_synchronizer).
-author("Konrad Zemek").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include("timeouts.hrl").

-behaviour(gen_server).

-define(MINIMAL_SYNC_REQUEST, op_worker:get_env(minimal_sync_request, 32768)).
-define(TRIGGER_BYTE, op_worker:get_env(trigger_byte, 52428800)).
-define(PREFETCH_SIZE, op_worker:get_env(prefetch_size, 104857600)).

%% The process is supposed to die after ?DIE_AFTER time of idling (no requests in flight)
-define(DIE_AFTER, 60000).

%% How long transfer stats are aggregated before updating transfer document
-define(STATS_AGGREGATION_TIME, application:get_env(
    ?APP_NAME, rtransfer_stats_aggregation_time, 1000)
).
%% How long file blocks are aggregated before updating location document
-define(BLOCKS_AGGREGATION_TIME, application:get_env(
    ?APP_NAME, rtransfer_blocks_aggregation_time, 1000)
).
%% How long file blocks are aggregated before updating location document
-define(EVENTS_CACHING_TIME, application:get_env(
    ?APP_NAME, rtransfer_events_caching_time, 1000)
).

-define(REF_TO_TIDS_CLEARING_DELAY, 15000).
-define(REF_TO_TIDS_CLEARING_MSG(__Ref), {clear_ref_to_tids_association, __Ref}).

-define(PREFETCH_PRIORITY,
    op_worker:get_env(default_prefetch_priority, 96)).
-define(MAX_RETRIES, op_worker:get_env(synchronizer_max_retries, 0)).
-define(MIN_BACKOFF, op_worker:get_env(synchronizer_min_backoff, timer:seconds(1))).
-define(BACKOFF_RATE, op_worker:get_env(synchronizer_backoff_rate, 1.5)).
-define(MAX_BACKOFF, op_worker:get_env(synchronizer_max_backoff, timer:minutes(5))).

-define(FLUSH_STATS, flush_stats).
-define(FLUSH_BLOCKS, flush_blocks).
-define(FLUSH_EVENTS, flush_events).
-define(FLUSH_LOCATION, flush_location).

-type fetch_ref() :: reference().
-type block() :: fslogic_blocks:block().
-type priority() :: non_neg_integer().
-type from() :: {pid(), any()}. %% `From` argument to gen_server:call callback
-type request_type() :: sync | async.

% module implementing transfer_stats_callback_behaviour
-type stats_callback_module() :: module().

-record(state, {
    file_ctx :: file_ctx:ctx(),
    file_guid :: undefined | fslogic_worker:file_guid(),
    space_id :: undefined | od_space:id(),
    dest_storage_id :: storage:id() | undefined,
    dest_file_id :: helpers:file_id() | undefined,
    last_transfer :: undefined | block(),
    in_progress :: ordsets:ordset({block(), fetch_ref(), priority()}),
    in_sequence_hits = 0 :: non_neg_integer(),
    from_to_refs = #{} :: #{from() => [fetch_ref()]},
    ref_to_froms = #{} :: #{fetch_ref() => [from()]},
    from_to_session = #{} :: #{from() => session:id()},
    session_to_froms = #{} :: #{session:id() => sets:set(from())},
    from_requests_types = #{} :: #{from() => request_type()},
    from_to_transfer_id = #{} :: #{from() => transfer:id() | undefined},
    ref_to_transfer_ids = #{} :: #{fetch_ref() => [transfer:id()]},
    transfer_id_to_from = #{} :: #{transfer:id() | undefined => from()},
    cached_stats = #{} :: #{undefined | transfer:id() => #{od_provider:id() => integer()}},
    requested_blocks = #{} :: #{from() => block()},
    cached_blocks = [] :: [block()],
    caching_stats_timer :: undefined | reference(),
    caching_blocks_timer :: undefined | reference(),
    caching_events_timer :: undefined | reference(),
    retries_number = #{} :: #{fetch_ref() => non_neg_integer()},
    transfer_id_to_stats_callback_module = #{} :: #{transfer:id() => stats_callback_module()}
}).

-define(BLOCK(__Offset, __Size), #file_block{offset = __Offset, size = __Size}).

% API
-export([
    synchronize/7, request_synchronization/7, request_synchronization/8,
    update_replica/4, force_flush_events/1, delete_whole_file_replica/2,
    cancel/1, cancel_transfers_of_session/2,
    terminate_all/0, cancel_and_terminate_slaves/0
]).
% gen_server callbacks
-export([handle_call/3, handle_cast/2, handle_info/2, init/1, code_change/3,
    terminate/2]).
% Apply functions
-export([apply_if_alive/2, apply/2, apply_or_run_locally/3,
    apply_or_run_locally/4]).
% For RPC
-export([apply_if_alive_internal/2, apply_internal/2, apply_or_run_locally_internal/3,
    init_or_return_existing/1]).
% For testing
-export([find_overlapping/3, get_holes/2, cancel_transfers_of_session_sync/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @equiv request_synchronization(UserCtx, FileCtx, Block, Prefetch,
%% TransferId, Priority, sync).
%% @end
%%--------------------------------------------------------------------
-spec synchronize(user_ctx:ctx(), file_ctx:ctx(), block(),
    Prefetch :: boolean(), transfer:id() | undefined, non_neg_integer(), stats_callback_module()) ->
    {ok, #file_location_changed{}} | {error, Reason :: any()}.
synchronize(UserCtx, FileCtx, Block, Prefetch, TransferId, Priority, StatsCallbackModule) ->
    request_synchronization(UserCtx, FileCtx, Block, Prefetch, TransferId,
        Priority, sync, StatsCallbackModule).

%%--------------------------------------------------------------------
%% @doc
%% @equiv request_synchronization(UserCtx, FileCtx, Block, Prefetch,
%% TransferId, Priority, async).
%% @end
%%--------------------------------------------------------------------
-spec request_synchronization(user_ctx:ctx(), file_ctx:ctx(), block(),
    Prefetch :: boolean(), transfer:id() | undefined, non_neg_integer(), stats_callback_module()) ->
    ok | {error, Reason :: any()}.
request_synchronization(UserCtx, FileCtx, Block, Prefetch, TransferId, Priority, StatsCallbackModule) ->
    request_synchronization(UserCtx, FileCtx, Block, Prefetch, TransferId,
        Priority, async, StatsCallbackModule).

%%--------------------------------------------------------------------
%% @doc
%% Sends sync request. If request type is sync,
%% blocks until a block is synchronized from a remote provider with
%% a newer version of data.
%% @end
%%--------------------------------------------------------------------
-spec request_synchronization(user_ctx:ctx(), file_ctx:ctx(), block(),
    Prefetch :: boolean(), transfer:id() | undefined, non_neg_integer(),
    request_type(), stats_callback_module()) -> ok | {ok, #file_location_changed{}} | {error, Reason :: any()}.
request_synchronization(UserCtx, FileCtx, Block, Prefetch, TransferId,
    Priority, SyncType, StatsCallbackModule) ->
    EnlargedBlock = enlarge_block(Block, Prefetch),
    SessionId = user_ctx:get_session_id(UserCtx),
    apply_no_check(FileCtx, {synchronize, FileCtx, EnlargedBlock,
        Prefetch, TransferId, SessionId, Priority, SyncType, StatsCallbackModule}).

%%--------------------------------------------------------------------
%% @doc
%% @equiv update_replica_internal(FileCtx, Blocks, FileSize, BumpVersion)
%% on chosen node.
%% @end
%%--------------------------------------------------------------------
-spec update_replica(file_ctx:ctx(), fslogic_blocks:blocks(),
    FileSize :: non_neg_integer() | undefined, BumpVersion :: boolean()) ->
    {ok, replica_updater:report()} | {error, Reason :: term()}.
update_replica(FileCtx, Blocks, FileSize, BumpVersion) ->
    replica_updater:update(FileCtx, Blocks, FileSize, BumpVersion).

%%--------------------------------------------------------------------
%% @doc
%% Forces events cache flush.
%% @end
%%--------------------------------------------------------------------
-spec force_flush_events(file_meta:uuid()) -> ok.
force_flush_events(Uuid) ->
    apply_if_alive_no_check(Uuid, ?FLUSH_EVENTS).

%%--------------------------------------------------------------------
%% @doc
%% Executes delete_whole_file_replica_internal inside synchronizer.
%% @end
%%--------------------------------------------------------------------
-spec delete_whole_file_replica(file_ctx:ctx(), version_vector:version_vector()) ->
    ok | {error, term()}.
delete_whole_file_replica(FileCtx, AllowedVV) ->
    apply_no_check(FileCtx, {delete_whole_file_replica, AllowedVV}).

%%--------------------------------------------------------------------
%% @doc
%% Asynchronously cancels a transfer in a best-effort manner.
%% @end
%%--------------------------------------------------------------------
-spec cancel(transfer:id()) -> ok.
cancel(TransferId) ->
    lists:foreach(
        fun(Pid) -> gen_server2:cast(Pid, {cancel, TransferId}) end,
        gproc:lookup_pids({c, l, TransferId})).

%%--------------------------------------------------------------------
%% @doc
%% Asynchronously cancels transfers associated with specified session and file.
%% @end
%%--------------------------------------------------------------------
-spec cancel_transfers_of_session(file_meta:uuid(), session:id()) -> ok.
cancel_transfers_of_session(FileUuid, SessionId) ->
    % TODO VFS-6153 race with open
    apply_if_alive(FileUuid, {async, {cancel_transfers_of_session, SessionId}}).

%%--------------------------------------------------------------------
%% @doc
%% Synchronously cancels transfers associated with specified session and file.
%% @end
%%--------------------------------------------------------------------
-spec cancel_transfers_of_session_sync(file_meta:uuid(), session:id()) -> ok.
cancel_transfers_of_session_sync(FileUuid, SessionId) ->
    apply_if_alive(FileUuid, {cancel_transfers_of_session, SessionId}).

%%--------------------------------------------------------------------
%% @doc
%% Terminates all synchronizers.
%% @end
%%--------------------------------------------------------------------
-spec terminate_all() -> ok.
terminate_all() ->
    Selection = gproc:select([{{{'_', '_', '$1'}, '_', '_'},
        [{is_binary, '$1'}], ['$$']}]),
    Pids = request_terminate(Selection),

    lists:foreach(fun(Pid) -> Pid ! terminate end, Pids),
    wait_for_terminate(Pids).

%%--------------------------------------------------------------------
%% @doc
%% Terminates all synchronizers that are slaves according to HA
%% (work on other than dedicated node because of dedicated node failure).
%% Cancels all requests before termination.
%% @end
%%--------------------------------------------------------------------
-spec cancel_and_terminate_slaves() -> ok.
cancel_and_terminate_slaves() ->
    Selection = gproc:select([{{{'_', '_', '$1'}, '_', '_'},
        [{is_binary, '$1'}], ['$$']}]),
    Pids = request_terminate(Selection),

    ReportTo = self(),
    lists:foreach(fun(Pid) -> Pid ! {check_and_terminate_slave, ReportTo} end, Pids),
    wait_for_slave_check(Pids).

%%%===================================================================
%%% Apply functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Executes function on synchronizer (does nothing if synchronizer does not work).
%% If function is called from the inside of synchronizer does not perform call
%% (executes function immediately).
%% @end
%%--------------------------------------------------------------------
-spec apply_if_alive(file_meta:uuid(), term()) ->
    term().
apply_if_alive(Uuid, Fun) ->
    case fslogic_cache:is_current_proc_cache() of
        false ->
            apply_if_alive_no_check(Uuid, Fun);
        _ ->
            Fun()
    end.

%%--------------------------------------------------------------------
%% @doc
%% Executes function on synchronizer. Starts synchronizer if it does not work.
%% If function is called from the inside of synchronizer does not perform call
%% (executes function immediately).
%% @end
%%--------------------------------------------------------------------
-spec apply(file_ctx:ctx(), fun(() -> term())) ->
    term().
apply(FileCtx, Fun) ->
    case fslogic_cache:is_current_proc_cache() of
        false ->
            apply_no_check(FileCtx, Fun);
        _ ->
            Fun()
    end.

%%--------------------------------------------------------------------
%% @doc
%% Executes function 1 on synchronizer or function 2 if synchronizer is not working.
%% If function is called from the inside of synchronizer does not perform call
%% (executes function immediately).
%% @end
%%--------------------------------------------------------------------
-spec apply_or_run_locally(file_meta:uuid(), fun(() -> term()),
    fun(() -> term())) -> term().
apply_or_run_locally(Uuid, Fun, FallbackFun) ->
    case fslogic_cache:is_current_proc_cache() of
        false ->
            apply_or_run_locally_no_check(Uuid, Fun, FallbackFun);
        _ ->
            Fun()
    end.

%%--------------------------------------------------------------------
%% @doc
%% Executes function 1 if call is from the inside of synchronizer, function 2 if
%% synchronizer is working and function 3 if not.
%% @end
%%--------------------------------------------------------------------
-spec apply_or_run_locally(file_meta:uuid(), fun(() -> term()),
    fun(() -> term()), fun(() -> term())) -> term().
apply_or_run_locally(Uuid, InCacheFun, ApplyOnCacheFun, FallbackFun) ->
    case fslogic_cache:is_current_proc_cache() of
        false ->
            apply_or_run_locally_no_check(Uuid, ApplyOnCacheFun, FallbackFun);
        _ ->
            InCacheFun()
    end.

%%%===================================================================
%%% Apply functions for internal use
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% @equiv apply_if_alive_internal(Uuid, FunOrMsg) on chosen node.
%% @end
%%--------------------------------------------------------------------
-spec apply_if_alive_no_check(file_meta:uuid(), term()) ->
    term().
apply_if_alive_no_check(Uuid, FunOrMsg) ->
    ReferencedUuid = fslogic_uuid:ensure_referenced_uuid(Uuid),
    Node = datastore_key:any_responsible_node(ReferencedUuid),
    rpc:call(Node, ?MODULE, apply_if_alive_internal, [ReferencedUuid, FunOrMsg]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% @equiv apply_internal(Uuid, FunOrMsg) on chosen node.
%% @end
%%--------------------------------------------------------------------
-spec apply_no_check(file_ctx:ctx(), term()) ->
    term().
apply_no_check(FileCtx, FunOrMsg) ->
    ReferencedUuidBasedFileCtx = file_ctx:ensure_based_on_referenced_guid(FileCtx),
    Node = datastore_key:any_responsible_node(file_ctx:get_logical_uuid_const(ReferencedUuidBasedFileCtx)),
    rpc:call(Node, ?MODULE, apply_internal, [ReferencedUuidBasedFileCtx, FunOrMsg]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% @equiv apply_or_run_locally_internal(Uuid, Fun, FallbackFun) on chosen node.
%% @end
%%--------------------------------------------------------------------
-spec apply_or_run_locally_no_check(file_meta:uuid(), fun(() -> term()), fun(() -> term())) ->
    term().
apply_or_run_locally_no_check(Uuid, Fun, FallbackFun) ->
    ReferencedUuid = fslogic_uuid:ensure_referenced_uuid(Uuid),
    Node = datastore_key:any_responsible_node(ReferencedUuid),
    rpc:call(Node, ?MODULE, apply_or_run_locally_internal, [ReferencedUuid, Fun, FallbackFun]).

%%%===================================================================
%%% Apply and start functions helpers
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Executes function on synchronizer or sends message to the synchronizer.
%% Starts synchronizer if it does not work.
%% Warning: cannot be called from the inside of synchronizer.
%% @end
%%--------------------------------------------------------------------
-spec apply_internal(file_ctx:ctx(), term()) -> term().
apply_internal(FileCtx, FunOrMsg) ->
    try
        {ok, Process} = get_process(FileCtx),
        case send_or_apply(Process, FunOrMsg) of
            {error, node_changed} ->
                ?debug("Synchronizer process stopped because of node change, "
                "retrying with a new one"),
                apply_no_check(FileCtx, FunOrMsg);
            Other ->
                Other
        end
    catch
        %% The process we called was already terminating because of idle timeout,
        %% there's nothing to worry about.
        exit:{{shutdown, timeout}, _} ->
            ?debug("Synchronizer process stopped because of a timeout, "
            "retrying with a new one"),
            apply_no_check(FileCtx, FunOrMsg);
        _:{noproc, _} ->
            ?debug("Synchronizer noproc, retrying with a new one"),
            apply_no_check(FileCtx, FunOrMsg);
        exit:{normal, _} ->
            ?debug("Synchronizer process stopped because of exit:normal, "
            "retrying with a new one"),
            apply_no_check(FileCtx, FunOrMsg)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Executes function on synchronizer or sends message to the synchronizer
%% (does nothing if synchronizer does not work).
%% Warning: cannot be called from the inside of synchronizer.
%% @end
%%--------------------------------------------------------------------
-spec apply_if_alive_internal(file_meta:uuid(), term()) -> term().
apply_if_alive_internal(Uuid, FunOrMsg) ->
    try
        case gproc:lookup_local_name(Uuid) of
            undefined ->
                ok;
            Process ->
                send_or_apply(Process, FunOrMsg)
        end
    catch
        %% The process we called was already terminating because of idle timeout,
        %% there's nothing to worry about.
        exit:{{shutdown, timeout}, _} ->
            ?debug("Synchronizer process stopped because of a timeout, "
            "apply cancelled"),
            ok;
        _:{noproc, _} ->
            ?debug("Synchronizer noproc, apply cancelled"),
            ok;
        exit:{normal, _} ->
            ?debug("Synchronizer process stopped because of exit:normal, "
            "apply cancelled"),
            ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% Executes function 1 on synchronizer or function 2 if synchronizer is not working.
%% Warning: cannot be called from the inside of synchronizer.
%% @end
%%--------------------------------------------------------------------
-spec apply_or_run_locally_internal(file_meta:uuid(), fun(() -> term()),
    fun(() -> term())) -> term().
apply_or_run_locally_internal(Uuid, Fun, FallbackFun) ->
    try
        case gproc:lookup_local_name(Uuid) of
            undefined ->
                FallbackFun();
            Process ->
                gen_server2:call(Process, {apply, Fun}, infinity)
        end
    catch
        %% The process we called was already terminating because of idle timeout,
        %% there's nothing to worry about.
        exit:{{shutdown, timeout}, _} ->
            ?debug("Synchronizer process stopped because of a timeout, "
            "executing fallback function"),
            FallbackFun();
        _:{noproc, _} ->
            ?debug("Synchronizer noproc, executing fallback function"),
            FallbackFun();
        exit:{normal, _} ->
            ?debug("Synchronizer process stopped because of exit:normal, "
            "executing fallback function"),
            FallbackFun()
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sends message to synchronizer or requests application of function.
%% @end
%%--------------------------------------------------------------------
-spec send_or_apply(pid(), term()) -> term().
send_or_apply(Process, FunOrMsg) when is_function(FunOrMsg) ->
    gen_server2:call(Process, {apply, FunOrMsg}, infinity);
send_or_apply(Process, {async, FunOrMsg}) ->
    gen_server2:cast(Process, FunOrMsg);
send_or_apply(Process, FunOrMsg) ->
    gen_server2:call(Process, FunOrMsg, infinity).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a new or existing process for synchronization of this file,
%% by this user session.
%% @end
%%--------------------------------------------------------------------
-spec get_process(file_ctx:ctx()) -> {ok, pid()} | {error, Reason :: any()}.
get_process(FileCtx) ->
    proc_lib:start(?MODULE, init_or_return_existing, [FileCtx], 10000).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns an existing process for synchronization of this file,
%% by this user session, or continues and makes this process the one.
%% @end
%%--------------------------------------------------------------------
-spec init_or_return_existing(file_ctx:ctx()) -> no_return() | normal.
init_or_return_existing(FileCtx) ->
    ReferencedUuidBasedFileCtx = file_ctx:ensure_based_on_referenced_guid(FileCtx),
    {Pid, _} = gproc:reg_or_locate({n, l, file_ctx:get_logical_uuid_const(ReferencedUuidBasedFileCtx)}),
    ok = proc_lib:init_ack({ok, Pid}),
    % TODO VFS-6389 - check race with node changing
    case self() of
        Pid ->
            {ok, State, Timeout} = init(ReferencedUuidBasedFileCtx),
            gen_server2:enter_loop(?MODULE, [], State, Timeout);
        _ ->
            normal
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec init(file_ctx:ctx()) -> {ok, #state{}, Timeout :: non_neg_integer()}.
init(FileCtx) ->
    ReferencedUuidBasedFileCtx = file_ctx:ensure_based_on_referenced_guid(FileCtx),
    fslogic_cache:init(file_ctx:get_logical_uuid_const(ReferencedUuidBasedFileCtx)),
    {ok, #state{
        file_ctx = ReferencedUuidBasedFileCtx,
        in_progress = ordsets:new()
    }, ?DIE_AFTER}.

%%--------------------------------------------------------------------
%% @doc
%% Synchronize a file block.
%% Only schedules synchronization of fragments where newer data is
%% available on other providers AND they are not already being
%% synchronized by this process.
%% The caller is notified once whole range he requested is
%% synchronized to the newest version.
%% @end
%%--------------------------------------------------------------------
handle_call({synchronize, FileCtx, Block, Prefetch, TransferId, Session, Priority, Type, StatsCallbackModule}, From,
    #state{
        requested_blocks = RB,
        file_guid = FG,
        from_requests_types = RequestTypesMap,
        transfer_id_to_stats_callback_module = TidToStatsCallback
    } = State0
) ->
    try
        State = case FG of
            undefined ->
                FileGuid = file_ctx:get_logical_guid_const(FileCtx),
                SpaceId = file_ctx:get_space_id_const(FileCtx),
                {_LocalDoc, FileCtx2} = file_ctx:get_or_create_local_file_location_doc(FileCtx),
                {DestStorageId, FileCtx3} = file_ctx:get_storage_id(FileCtx2),
                {DestFileId, FileCtx4} = file_ctx:get_storage_file_id(FileCtx3),
                {_LocationDocs, FileCtx5} = file_ctx:get_file_location_docs(FileCtx4),
                State0#state{
                    file_ctx = FileCtx5,
                    dest_storage_id = DestStorageId,
                    dest_file_id = DestFileId,
                    file_guid = FileGuid,
                    space_id = SpaceId,
                    transfer_id_to_stats_callback_module =
                        TidToStatsCallback#{TransferId => StatsCallbackModule}
                };
            _ ->
                State0#state{
                    transfer_id_to_stats_callback_module =
                        TidToStatsCallback#{TransferId => StatsCallbackModule}
                }
        end,

        TransferId =/= undefined andalso (catch gproc:add_local_counter(TransferId, 1)),
        OverlappingInProgress = find_overlapping(Block, Priority, State#state.in_progress),
        {OverlappingBlocks, ExistingRefs, _Priorities} = lists:unzip3(OverlappingInProgress),
        Holes = get_holes(Block, OverlappingBlocks),
        NewTransfers = start_transfers(Holes, TransferId, State, Priority),
        {_, NewRefs, _} = lists:unzip3(NewTransfers),
        case ExistingRefs ++ NewRefs of
            [] ->
                FileLocation = fslogic_location:get_local_blocks_and_fill_location_gaps([Block],
                    fslogic_cache:get_local_location(), fslogic_cache:get_all_locations(), fslogic_cache:get_uuid()
                ),
                {ChangeOffset, ChangeEnd} = fslogic_location_cache:get_blocks_range(FileLocation, [Block]),
                FLC = #file_location_changed{file_location = FileLocation,
                    change_beg_offset = ChangeOffset, change_end_offset = ChangeEnd},
                case Type of
                    sync ->
                        {reply, {ok, FLC}, State, ?DIE_AFTER};
                    async ->
                        {reply, ok, State, ?DIE_AFTER}
                end;
            RelevantRefs ->
                State1 = associate_from_with_refs(From, RelevantRefs, State),
                State2 = associate_from_with_tid(From, TransferId, State1),
                State3 = add_in_progress(NewTransfers, State2),
                State5 = case Prefetch of
                    true ->
                        % TODO VFS-4690 - does not work well while many simultaneous
                        % transfers do prefetching
                        State4 = adjust_sequence_hits(Block, NewRefs, State3),
                        prefetch(NewTransfers, TransferId, State4);
                    _ ->
                        State3
                end,
                State6 = associate_from_with_session(From, Session, State5),
                State7 = State6#state{requested_blocks = maps:put(From, Block, RB)},
                State8 = State7#state{from_requests_types =
                maps:put(From, Type, RequestTypesMap)},

                case Type of
                    sync ->
                        {noreply, State8, ?DIE_AFTER};
                    async ->
                        {reply, ok, State8, ?DIE_AFTER}
                end
        end
    catch
        E1:E2:Stacktrace ->
            ?error_stacktrace("Unable to start transfer due to error ~p:~p",
                [E1, E2], Stacktrace),
            {reply, {error, E2}, State0, ?DIE_AFTER}
    end;

handle_call(?FLUSH_EVENTS, _From, State) ->
    {reply, ok, flush_events(State), ?DIE_AFTER};

handle_call({delete_whole_file_replica, AllowedVV}, From, State) ->
    case delete_whole_file_replica_internal(AllowedVV, From, State) of
        {helper_process_spawned, _Pid} -> {noreply, State, ?DIE_AFTER};
        {error, _} = Error -> {reply, Error, State, ?DIE_AFTER}
    end;

handle_call({apply, Fun}, _From, State) ->
    Ans = Fun(),
    {reply, Ans, State, ?DIE_AFTER};

handle_call({cancel_transfers_of_session, SessionId}, _From, State) ->
    NewState = cancel_session(SessionId, State),
    {reply, ok, NewState, ?DIE_AFTER}.

handle_cast({cancel, TransferId}, State) ->
    try
        NewState = cancel_transfer_id(TransferId, State),
        {noreply, NewState, ?DIE_AFTER}
    catch
        _:Reason:Stacktrace ->
            ?error_stacktrace("Unable to cancel ~p: ~p", [TransferId, Reason], Stacktrace),
            {noreply, State, ?DIE_AFTER}
    end;

handle_cast({cancel_transfers_of_session, SessionId}, State) ->
    NewState = cancel_session(SessionId, State),
    {noreply, NewState, ?DIE_AFTER};

handle_cast(Msg, State) ->
    ?log_bad_request(Msg),
    {noreply, State, ?DIE_AFTER}.

handle_info(timeout, #state{in_progress = []} = State) ->
    ?debug("Exiting due to inactivity with state: ~p", [State]),
    {stop, {shutdown, timeout}, State};

handle_info(timeout, State) ->
    {noreply, State, ?DIE_AFTER};

handle_info(?REF_TO_TIDS_CLEARING_MSG(Ref), State) ->
    NewRefToTransferIds = maps:remove(Ref, State#state.ref_to_transfer_ids),
    {noreply, State#state{ref_to_transfer_ids = NewRefToTransferIds}, ?DIE_AFTER};

handle_info({Ref, active, ProviderId, Block}, #state{
    file_ctx = FileCtx,
    ref_to_froms = RefToFroms,
    ref_to_transfer_ids = RefToTransferIds,
    from_to_transfer_id = FromToTransferId
} = State) ->
    fslogic_cache:set_local_change(true),
    {ok, _} = replica_updater:update(FileCtx, [Block], undefined, false),
    fslogic_cache:set_local_change(false),

    % Race where `complete` msg is send before `active` one is possible because
    % of rtransfer. That's why association from ref to transfer ids is created
    % and kept for some time after handling `complete` msg despite other
    % associations being cleared. Thanks to that transfer statistics can be
    % updated correctly.
    AffectedFroms = maps:get(Ref, RefToFroms, []),
    TransferIds = case maps:values(maps:with(AffectedFroms, FromToTransferId)) of
        [] ->
            maps:get(Ref, RefToTransferIds, [undefined]);
        Values ->
            lists:usort(Values)
    end,
    {noreply, cache_stats_and_blocks(TransferIds, ProviderId, Block, State), ?DIE_AFTER};

handle_info(?FLUSH_STATS, State) ->
    {noreply, flush_stats(State, true), ?DIE_AFTER};

handle_info(?FLUSH_BLOCKS, State) ->
    {_, State2} = flush_blocks(State),
    {noreply, flush_events(State2), ?DIE_AFTER};

handle_info(?FLUSH_EVENTS, State) ->
    {noreply, flush_events(State), ?DIE_AFTER};

handle_info({Ref, complete, {ok, _} = _Status}, #state{retries_number = Retries, file_ctx = FileCtx} = State) ->
    {Block, _Priority, _AffectedFroms, FinishedFroms, State1} =
        disassociate_ref(Ref, State),
    {FinishedBlocks, ExcludeSessions, EndedTransfers, State2} =
        disassociate_froms(FinishedFroms, State1),

    flush_archive_helper_buffer_if_applicable(FileCtx),

    {Ans, State3} = flush_blocks(State2, ExcludeSessions, FinishedBlocks,
        EndedTransfers =/= []),

    % Transfer on the fly statistics are being kept under `undefined` key in
    % `state.cached_stats` map, so take it from it and flush only jobs stats;
    % on the lfy transfer stats are flushed only on cache timer timeout
    State4 = case maps:take(undefined, State2#state.cached_stats) of
        error ->
            flush_stats(State3, true);
        {OnfStats, JobsStats} ->
            TempState = flush_stats(State3#state{cached_stats = JobsStats}, false),
            TempState#state{cached_stats = #{undefined => OnfStats}}
    end,

    case Block of
        undefined ->
            [gen_server2:reply(From, {error, cancelled}) || From <- FinishedFroms];
        _ ->
            [transfer:increment_files_replicated_counter(TID) || TID <- EndedTransfers],
            [gen_server2:reply(From, {ok, Message}) || {From, Message} <- Ans]
    end,

    State5 = associate_ref_with_tids(Ref, EndedTransfers, State4),
    {noreply, State5#state{retries_number = maps:remove(Ref, Retries)}, ?DIE_AFTER};

handle_info({FailedRef, complete, {error, disconnected} = ErrorStatus}, State) ->
    NewState = handle_retry(FailedRef, ErrorStatus, State),
    {noreply, NewState, ?DIE_AFTER};

handle_info({replace_failed_transfer, FailedRef}, #state{retries_number = RetriesMap} = State) ->
    try
        {Block, Priority, AffectedFroms, _FinishedFroms, State1} =
            disassociate_ref(FailedRef, State),

        case Block of
            undefined ->
                ?error("Failed transfer ~p not found in state", [FailedRef]),
                {noreply, State1};
            _ ->
                NewTransfers = start_transfers([Block], undefined, State1, Priority),
                ?warning("Replaced failed transfer ~p (~p) with new transfers ~p", [
                    FailedRef, Block, NewTransfers
                ]),
                {_, NewRefs, _} = lists:unzip3(NewTransfers),
                State2 = associate_froms_with_refs(AffectedFroms, NewRefs, State1),
                State3 = add_in_progress(NewTransfers, State2),

                RetriesNum = maps:get(FailedRef, RetriesMap, 0),
                NewRetriesMap = lists:foldl(fun(Ref, Acc) ->
                    Acc#{Ref => RetriesNum + 1}
                end, maps:remove(FailedRef, RetriesMap), NewRefs),

                {noreply, State3#state{retries_number = NewRetriesMap}, ?DIE_AFTER}
        end
    catch
        E1:E2:Stacktrace ->
            ?error_stacktrace("Unable to restart transfer due to error ~p:~p",
                [E1, E2], Stacktrace),
            handle_info({FailedRef, complete, {error, restart_failed}}, State)
    end;

handle_info({Ref, complete, {error, {connection, <<"canceled">>}}}, #state{
    ref_to_froms = RTFs
} = State) ->
    ?debug("Transfer ~p cancelled", [Ref]),

    State2 = case maps:get(Ref, RTFs, undefined) of
        undefined ->
            % If there is no association then replication must have been cancelled
            % by transfer job (all associations were already cleared)
            State;
        AffectedFroms ->
            % If there is association then replication must have been cancelled
            % by rtransfer due to exceeded quota. In that case associated
            % replications should be also cancelled and registry/state cleared.
            try
                cancel_froms(AffectedFroms, State)
            catch _:Reason:Stacktrace ->
                ?error_stacktrace("Unable to cancel transfers associated with ~p due to ~p", [
                    Ref, Reason
                ], Stacktrace),
                State
            end
    end,
    {noreply, State2, ?DIE_AFTER};

handle_info({FailedRef, complete, {error, {connection, <<"No such file or directory">>}} = ErrorStatus},
    #state{file_ctx = FileCtx} = State) ->
    ?info("restoring storage file ~p", [file_ctx:get_logical_guid_const(FileCtx)]),
    sd_utils:restore_storage_file(FileCtx, user_ctx:new(?ROOT_SESS_ID)),
    NewState = handle_retry(FailedRef, ErrorStatus, State),
    {noreply, NewState, ?DIE_AFTER};

handle_info({Ref, complete, ErrorStatus}, State) ->
    {noreply, handle_error(Ref, ErrorStatus, State), ?DIE_AFTER};

handle_info(fslogic_cache_check_flush, State) ->
    fslogic_cache:check_flush(),

    case op_worker:get_env(synchronizer_gc, on_flush_location) of
        on_flush_location ->
            erlang:garbage_collect();
        _ ->
            ok
    end,

    {noreply, State, ?DIE_AFTER};

handle_info({fslogic_cache_flushed, Key, Check1, Check2}, State) ->
    fslogic_cache:verify_flush_ans(Key, Check1, Check2),
    {noreply, State, ?DIE_AFTER};

handle_info(terminate, State) ->
    {stop, normal, State};

handle_info({check_and_terminate_slave, ReportTo}, #state{file_ctx = Ctx} = State) ->
    Uuid = file_ctx:get_logical_uuid_const(Ctx),
    LocalNode = node(),
    case datastore_key:any_responsible_node(Uuid) of
        LocalNode -> ok;
        _ -> cancel_all_transfers(State)
    end,
    ReportTo ! {slave_checked, self()},
    {stop, normal, State};

handle_info({file_truncated, Ans, EmitEvents, RequestedBy}, State) ->
    {FinalAns, State2} = finish_blocks_clearing(Ans, EmitEvents, State),
    gen_server2:reply(RequestedBy, FinalAns),
    {noreply, State2, ?DIE_AFTER};

handle_info(Msg, State) ->
    ?log_bad_request(Msg),
    {noreply, State, ?DIE_AFTER}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, State) ->
    {_, State2} = flush_blocks(State),
    flush_stats(State2, true),
    flush_events(State2),
    % TODO VFS-4691 - should not terminate when flush failed
    fslogic_cache:flush(terminate),
    ignore.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec handle_error(fetch_ref(), Error :: term(), #state{}) -> #state{}.
handle_error(Ref, ErrorStatus, State) ->
    ?error("Transfer ~p failed: ~p", [Ref, ErrorStatus]),
    {_Block, _Priority, AffectedFroms, FinishedFroms, State1} =
        disassociate_ref(Ref, State),
    {_FailedBlocks, _ExcludeSessions, FailedTransfers, State2} =
        disassociate_froms(FinishedFroms, State1),

    %% Cancel TransferIds that are still left (i.e. the failed ref was only a part of the transfer)
    AffectedTransferIds = maps:values(maps:with(AffectedFroms, State2#state.from_to_transfer_id)),
    [gen_server2:reply(From, ErrorStatus) || From <- FinishedFroms],
    State3 = lists:foldl(fun cancel_transfer_id/2, State2, AffectedTransferIds),

    associate_ref_with_tids(Ref, FailedTransfers, State3).

%% @private
-spec handle_retry(fetch_ref(), Error :: term(), #state{}) -> #state{}.
handle_retry(FailedRef, ErrorStatus, #state{retries_number = Retries} = State) ->
    RetriesNum = maps:get(FailedRef, Retries, 0),
    MaxRetriesNum = ?MAX_RETRIES,
    case RetriesNum >= MaxRetriesNum of
        true ->
            handle_error(FailedRef, ErrorStatus, State);
        false ->
            Delay = min(round(?MIN_BACKOFF * math:pow(?BACKOFF_RATE, RetriesNum)), ?MAX_BACKOFF),
            ?warning("Failed transfer ~p, replacing with new transfers after ~p seconds", [
                FailedRef, Delay
            ]),
            erlang:send_after(round(Delay), self(), {replace_failed_transfer, FailedRef}),
            State
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes a transfer request from most of the internal state and
%% returns From tuples for further handling.
%% @end
%%--------------------------------------------------------------------
-spec disassociate_ref(fetch_ref(), #state{}) -> {block() | undefined, priority() | undefined,
    AffectedFroms :: [from()], FinishedFroms :: [from()], #state{}}.
disassociate_ref(Ref, State = #state{
    in_progress = InProgress,
    ref_to_froms = RTFs,
    from_to_refs = FTRs
}) ->
    {Block, Priority, InProgress2} = case lists:keytake(Ref, 2, InProgress) of
        {value, {B, _, P}, IP} ->
            {B, P, IP};
        false ->
            {undefined, undefined, InProgress}
    end,

    {AffectedFroms, RTFs2} = case maps:take(Ref, RTFs) of
        error ->
            {[], RTFs};
        Res ->
            Res
    end,

    {FinishedFroms, FTRs2} = lists:foldl(fun(From, {FF, TmpFTRs}) ->
        WaitingForRefs = maps:get(From, TmpFTRs, []),
        case lists:delete(Ref, WaitingForRefs) of
            [] -> {[From | FF], maps:remove(From, TmpFTRs)};
            Remaining -> {FF, maps:put(From, Remaining, TmpFTRs)}
        end
    end, {[], FTRs}, AffectedFroms),

    {Block, Priority, AffectedFroms, FinishedFroms, State#state{
        in_progress = InProgress2,
        ref_to_froms = RTFs2,
        from_to_refs = FTRs2
    }}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes a transfer request froms associations with everything beside refs.
%% @end
%%--------------------------------------------------------------------
-spec disassociate_froms([from()], #state{}) -> {
    FinishedBlocks :: [{from(), block(), request_type()}],
    FinishedSessions :: [session:id()],
    FinishedTransferIds :: [transfer:id()], #state{}}.
disassociate_froms([], State) ->
    {[], [], [], State};
disassociate_froms(Froms, State = #state{
    requested_blocks = RB,
    from_requests_types = RT,
    from_to_session = FTS,
    session_to_froms = STFs,
    from_to_transfer_id = FTT,
    transfer_id_to_from = TTF
}) ->
    {FinishedBlocks, FinishedSessions, RB2, RT2, FTS2, STFs2} = lists:foldr(
        fun(From, {BlocksAcc, SessionsAcc, TmpRB, TmpRT, TmpFTS, TmpSTFs}) ->
            {Block, TmpRB2} = maps:take(From, TmpRB),
            {ReqType, TmpRT2} = maps:take(From, TmpRT),
            BlocksAcc1 = [{From, Block, ReqType} | BlocksAcc],

            {SessionsAcc2, TmpFTS2, TmpSTFs2} = case maps:take(From, TmpFTS) of
                error ->
                    {SessionsAcc, TmpFTS, TmpSTFs};
                {SessionId, TmpFTS1} ->
                    SessionsAcc1 = [SessionId | SessionsAcc -- [SessionId]],
                    SessionFroms = maps:get(SessionId, TmpSTFs, sets:new()),
                    TmpSTFs1 = TmpSTFs#{
                        SessionId => sets:del_element(From, SessionFroms)
                    },
                    {SessionsAcc1, TmpFTS1, TmpSTFs1}
            end,

            {BlocksAcc1, SessionsAcc2, TmpRB2, TmpRT2, TmpFTS2, TmpSTFs2}
        end,
    {[], [], RB, RT, FTS, STFs}, Froms),

    FinishedTransfers = maps:values(maps:with(Froms, FTT)),

    lists:foreach(fun(TransferId) ->
        gproc:unreg({c, l, TransferId})
    end, FinishedTransfers),

    {FinishedBlocks, FinishedSessions, FinishedTransfers, State#state{
        requested_blocks = RB2,
        from_requests_types = RT2,
        from_to_session = FTS2,
        session_to_froms = STFs2,
        from_to_transfer_id = maps:without(Froms, FTT),
        transfer_id_to_from = maps:without(FinishedTransfers, TTF)
    }}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Cancels a transfer by TransferId.
%% @end
%%--------------------------------------------------------------------
-spec cancel_transfer_id(transfer:id(), #state{}) -> #state{}.
cancel_transfer_id(TransferId, State) ->
    From = maps:get(TransferId, State#state.transfer_id_to_from),
    cancel_froms([From], State).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Cancels transfers by SessionId.
%% @end
%%--------------------------------------------------------------------
-spec cancel_session(session:id(), #state{}) -> #state{}.
cancel_session(SessionId, State) ->
    try
        case maps:take(SessionId, State#state.session_to_froms) of
            error ->
                State;
            {Froms, STFs} ->
                cancel_froms(sets:to_list(Froms), State#state{
                    session_to_froms = STFs
                })
        end
    catch
        _:Reason:Stacktrace ->
            ?error_stacktrace("Unable to cancel transfers of ~p: ~p", [
                SessionId, Reason
            ], Stacktrace),
            State
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Cancels a transfers by synchronization request froms.
%% @end
%%--------------------------------------------------------------------
-spec cancel_froms([from()], #state{}) -> #state{}.
cancel_froms([], State) ->
    State;
cancel_froms(Froms, State = #state{
    in_progress = InProgress,
    from_to_refs = FTRs,
    ref_to_froms = RTFs
}) ->
    AffectedRefs = lists:flatmap(fun(From) -> maps:get(From, FTRs, []) end, Froms),

    FromsSet = gb_sets:from_list(Froms),

    {RTFs2, OrphanedRefs} = maps:fold(fun(Ref, RefFroms, {TmpRTFs, TmpRefs}) ->
        RemainingFroms = gb_sets:subtract(gb_sets:from_list(RefFroms), FromsSet),
        case gb_sets:is_empty(RemainingFroms) of
            true ->
                cancel_ref(Ref, 3),
                {maps:remove(Ref, TmpRTFs), sets:add_element(Ref, TmpRefs)};
            false ->
                {TmpRTFs#{Ref => gb_sets:to_list(RemainingFroms)}, TmpRefs}
        end
    end, {RTFs, sets:new()}, maps:with(AffectedRefs, RTFs)),

    lists:foreach(fun(From) ->
        gen_server2:reply(From, {error, cancelled})
    end, Froms),

    InProgress2 = lists:filter(fun({_Block, Ref, _Priority}) ->
        not sets:is_element(Ref, OrphanedRefs)
    end, InProgress),

    {_CancelledBlocks, _ExcludeSessions, _CancelledTransfers, State2} =
        disassociate_froms(Froms, State#state{
            in_progress = InProgress2,
            from_to_refs = maps:without(Froms, FTRs),
            ref_to_froms = RTFs2
        }),

    State2.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Cancels transfer via reference.
%% @end
%%--------------------------------------------------------------------
-spec cancel_ref(fetch_ref(), non_neg_integer()) -> ok | failed.
cancel_ref(_Ref, 0) ->
    failed;
cancel_ref(Ref, RetryNum) ->
    try
        rtransfer_link:cancel(Ref)
    catch
        %% The process we called was already terminating because of idle timeout,
        %% there's nothing to worry about.
        exit:{{shutdown, timeout}, _} ->
            ?warning("Transfer cancel failed because of a timeout, "
            "retrying with a new one"),
            cancel_ref(Ref, RetryNum - 1);
        _:{noproc, _} ->
            ?warning("Transfer cancel failed because of noproc, "
            "retrying with a new one"),
            cancel_ref(Ref, RetryNum - 1);
        exit:{normal, _} ->
            ?warning("Transfer cancel failed because of exit:normal, "
            "retrying with a new one"),
            cancel_ref(Ref, RetryNum - 1)
    end.

-spec cancel_all_transfers(#state{}) -> ok.
cancel_all_transfers(#state{in_progress = InProgress, from_to_refs = Froms}) ->
    lists:foreach(fun({_, FetchRef, _}) ->
        cancel_ref(FetchRef, 3)
    end, ordsets:to_list(InProgress)),

    lists:foreach(fun(From) ->
        gen_server2:reply(From, {error, node_changed})
    end, maps:keys(Froms)).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Associates synchronization requests with existing rtransfer
%% requests.
%% @end
%%--------------------------------------------------------------------
-spec associate_froms_with_refs(Froms :: [from()], Refs :: [fetch_ref()], #state{}) -> #state{}.
associate_froms_with_refs(Froms, Refs, State) when is_list(Froms) ->
    lists:foldl(fun(From, S) -> associate_from_with_refs(From, Refs, S) end,
        State, Froms).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Associates a new synchronization request with existing rtransfer
%% requests.
%% @end
%%--------------------------------------------------------------------
-spec associate_from_with_refs(From :: from(), Refs :: [fetch_ref()], #state{}) -> #state{}.
associate_from_with_refs(From, Refs, State) when is_list(Refs) ->
    OldFromToRefs = maps:get(From, State#state.from_to_refs, []),
    FromToRefs = maps:put(From, OldFromToRefs ++ Refs, State#state.from_to_refs),
    RefToFroms =
        lists:foldl(
            fun(Ref, Acc) ->
                OldList = maps:get(Ref, Acc, []),
                maps:put(Ref, [From | OldList], Acc)
            end,
            State#state.ref_to_froms,
            Refs),
    State#state{from_to_refs = FromToRefs, ref_to_froms = RefToFroms}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Associates a new synchronization with a transfer id.
%% @end
%%--------------------------------------------------------------------
-spec associate_from_with_tid(From :: from(), transfer:id(), #state{}) -> #state{}.
associate_from_with_tid(_From, undefined, State) -> State;
associate_from_with_tid(From, TransferId, State) ->
    FromToTransferId = maps:put(From, TransferId, State#state.from_to_transfer_id),
    TransferIdToFrom = maps:put(TransferId, From, State#state.transfer_id_to_from),
    State#state{from_to_transfer_id = FromToTransferId,
        transfer_id_to_from = TransferIdToFrom}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Associates a fetch ref with a transfer ids and sets timer after which
%% msg to clear such association will be send.
%% @end
%%--------------------------------------------------------------------
-spec associate_ref_with_tids(Ref :: fetch_ref(), [transfer:id()], #state{}) ->
    #state{}.
associate_ref_with_tids(_From, [], State) -> State;
associate_ref_with_tids(Ref, TransferIds, State) ->
    RefToTransferIds = maps:put(Ref, TransferIds, State#state.ref_to_transfer_ids),
    erlang:send_after(?REF_TO_TIDS_CLEARING_DELAY, self(), ?REF_TO_TIDS_CLEARING_MSG(Ref)),
    State#state{ref_to_transfer_ids = RefToTransferIds}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Associates a new synchronization request with a session.
%% @end
%%--------------------------------------------------------------------
-spec associate_from_with_session(from(), session:id(), #state{}) -> #state{}.
associate_from_with_session(From, SessionId, State = #state{
    from_to_session = FTS,
    session_to_froms = STFs
}) ->
    Froms = maps:get(SessionId, STFs, sets:new()),
    State#state{
        from_to_session = FTS#{From => SessionId},
        session_to_froms = STFs#{SessionId => sets:add_element(From, Froms)}
    }.

-spec add_in_progress([{block(), fetch_ref(), priority()}], #state{}) -> #state{}.
add_in_progress(NewTransfers, State) ->
    InProgress = ordsets:union(State#state.in_progress, ordsets:from_list(NewTransfers)),
    State#state{in_progress = InProgress}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Increases or resets the in-sequence counter for prefetching.
%% The counter is only moved when the new synchronization request
%% actually results in a rtransfer request.
%% @end
%%--------------------------------------------------------------------
-spec adjust_sequence_hits(block(), [fetch_ref()], #state{}) -> #state{}.
adjust_sequence_hits(_Block, [], State) ->
    State;
adjust_sequence_hits(Block, _NewRefs, #state{in_sequence_hits = Hits} = State) ->
    NewState =
        case is_sequential(Block, State) of
            true -> State#state{in_sequence_hits = Hits + 1};
            false -> State#state{in_sequence_hits = 0}
        end,
    NewState#state{last_transfer = Block}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether a new request is in-sequence.
%% @end
%%--------------------------------------------------------------------
-spec is_sequential(block(), #state{}) -> boolean().
is_sequential(#file_block{offset = NextOffset, size = NewSize},
    #state{last_transfer = #file_block{offset = O, size = S}})
    when NextOffset =< O + S andalso NextOffset + NewSize > O + S ->
    true;
is_sequential(_, _State) ->
    false.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Starts new rtransfer transfers.
%% @end
%%--------------------------------------------------------------------
-spec start_transfers([block()], transfer:id() | undefined,
    #state{}, priority()) ->
    NewRequests :: [{block(), fetch_ref(), priority()}].
start_transfers(InitialBlocks, TransferId, State, Priority) ->
    LocationDocs = fslogic_cache:get_all_locations(),
    ProvidersAndBlocks = replica_finder:get_blocks_for_sync(LocationDocs, InitialBlocks),
    TotalSize = lists:foldl(fun({_P, Blocks, _SD}, Acc) ->
        fslogic_blocks:size(Blocks) + Acc
    end, 0, ProvidersAndBlocks),

    SpaceId = State#state.space_id,
    assert_smaller_than_local_support_size(TotalSize, SpaceId),

    FileGuid = State#state.file_guid,
    DestStorageId = State#state.dest_storage_id,
    DestFileId = State#state.dest_file_id,
    lists:flatmap(
        fun({ProviderId, Blocks, {SrcStorageId, SrcFileId}}) ->
            lists:map(
                fun(#file_block{offset = O, size = S} = FetchedBlock) ->
                    Request = #{
                        provider_id => ProviderId,
                        file_guid => FileGuid,
                        src_storage_id => SrcStorageId,
                        src_file_id => SrcFileId,
                        dest_storage_id => DestStorageId,
                        dest_file_id => DestFileId,
                        space_id => SpaceId,
                        offset => O,
                        size => S,
                        priority => Priority
                    },
                    Self = self(),
                    NotifyFun = make_notify_fun(Self, ProviderId),
                    CompleteFun = make_complete_fun(Self),
                    {ok, NewRef} = rtransfer_config:fetch(Request, NotifyFun, CompleteFun,
                        TransferId, SpaceId, FileGuid),
                    {FetchedBlock, NewRef, Priority}
                end, Blocks)
        end, ProvidersAndBlocks).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates a new notification function called on transfer update.
%% @end
%%--------------------------------------------------------------------
-spec make_notify_fun(Self :: pid(), od_provider:id()) -> rtransfer_link:notify_fun().
make_notify_fun(Self, ProviderId) ->
    fun(Ref, Offset, Size) ->
        Self ! {Ref, active, ProviderId, #file_block{offset = Offset, size = Size}}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates a new completion function called on transfer completion.
%% @end
%%--------------------------------------------------------------------
-spec make_complete_fun(Self :: pid()) -> rtransfer_link:on_complete_fun().
make_complete_fun(Self) ->
    fun(Ref, Status) ->
        Self ! {Ref, complete, Status}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Schedules a prefetch transfer.
%% @end
%%--------------------------------------------------------------------
-spec prefetch(NewTransfers :: list(), transfer:id() | undefined,
    #state{}) -> #state{}.
prefetch([], _TransferId, State) -> State;
prefetch(_NewTransfers, _TransferId, #state{in_sequence_hits = 0} = State) ->
    State;
prefetch(_, TransferId, #state{in_sequence_hits = Hits, last_transfer = Block} = State) ->
    case op_worker:get_env(synchronizer_prefetch, true) of
        true ->
            #file_block{offset = O, size = S} = Block,
            Offset = O + S,
            Size = min(?MINIMAL_SYNC_REQUEST * round(math:pow(2, Hits)), ?PREFETCH_SIZE),
            PrefetchBlock = #file_block{offset = Offset, size = Size},
            % TODO VFS-4693 - filter already started blocks
            NewTransfers = start_transfers([PrefetchBlock], TransferId, State, ?PREFETCH_PRIORITY),
            InProgress = ordsets:union(State#state.in_progress, ordsets:from_list(NewTransfers)),
            State#state{last_transfer = PrefetchBlock, in_progress = InProgress};
        _ ->
            State
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Enlarges a block to minimal synchronization size.
%% @end
%%--------------------------------------------------------------------
-spec enlarge_block(block(), Prefetch :: boolean()) -> block().
enlarge_block(Block = #file_block{size = RequestedSize}, _Prefetch = true) ->
    Block#file_block{size = max(RequestedSize, ?MINIMAL_SYNC_REQUEST)};
enlarge_block(Block, _Prefetch) ->
    Block.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Finds parts of a block that are already in progress (only blocks with
%% priorities equal or higher (numerically lower ones) are taken in account).
%% @end
%%--------------------------------------------------------------------
-spec find_overlapping(block(), priority(), Blocks) -> Overlapping when
    Blocks :: ordsets:ordset([{block(), fetch_ref(), priority()}]),
    Overlapping :: [{block(), fetch_ref(), priority()}].
find_overlapping(#file_block{offset = Begin, size = Size}, Priority, Blocks) ->
    End = Begin + Size,
    lists:filter(fun({#file_block{offset = O, size = S}, _Ref, P}) ->
        P =< Priority andalso O < End andalso O + S > Begin
    end, ordsets:to_list(Blocks)).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Finds parts of request that are not yet being fulfilled.
%% @end
%%--------------------------------------------------------------------
-spec get_holes(block(), Existing :: [block()]) -> Holes :: [block()].
get_holes(#file_block{offset = Offset, size = Size}, ExistingBlocks) ->
    get_holes(Offset, Size, ExistingBlocks, []).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Finds parts of request that are not yet being fulfilled.
%% @end
%%--------------------------------------------------------------------
-spec get_holes(Offset :: non_neg_integer(), Size :: non_neg_integer(),
    Existing :: [block()], Acc :: [block()]) -> Holes :: [block()].
get_holes(_Offset, Size, _Existing, Acc) when Size =< 0 ->
    lists:reverse(Acc);
get_holes(Offset, Size, [], Acc) ->
    get_holes(Offset + Size, 0, [], [?BLOCK(Offset, Size) | Acc]);
get_holes(Offset, Size, [?BLOCK(O, S) | Blocks], Acc) when Offset < O ->
    NewOffset = O + S,
    NewSize = Offset + Size - NewOffset,
    get_holes(NewOffset, NewSize, Blocks, [?BLOCK(Offset, O - Offset) | Acc]);
get_holes(Offset, Size, [?BLOCK(O, S) | Blocks], Acc) when O =< Offset andalso O + S > Offset ->
    NewOffset = O + S,
    NewSize = Offset + Size - NewOffset,
    get_holes(NewOffset, NewSize, Blocks, Acc);
get_holes(Offset, Size, [_ | Blocks], Acc) ->
    get_holes(Offset, Size, Blocks, Acc).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Cache transferred block in state to avoid updating transfer documents
%% hundred of times per second and set timeout after which cached stats will
%% be flushed.
%% @end
%%--------------------------------------------------------------------
-spec cache_stats_and_blocks([transfer:id() | undefined], od_provider:id(), block(),
    #state{}) -> #state{}.
cache_stats_and_blocks(TransferIds, ProviderId, Block, State) ->
    #file_block{size = Size} = Block,

    UpdatedStats = lists:foldl(fun(TransferId, StatsPerTransfer) ->
        BytesPerProvider = maps:get(TransferId, StatsPerTransfer, #{}),
        NewTransferStats = BytesPerProvider#{
            ProviderId => Size + maps:get(ProviderId, BytesPerProvider, 0)
        },
        StatsPerTransfer#{TransferId => NewTransferStats}
    end, State#state.cached_stats, TransferIds),

    CachedBlocks = case op_worker:get_env(synchronizer_in_progress_events, transfers_only) of
        transfers_only ->
            case TransferIds of
                [undefined] ->
                    [];
                _ ->
                    [Block | State#state.cached_blocks]
            end;
        all ->
            [Block | State#state.cached_blocks];
        off ->
            []
    end,
    NewState = State#state{
        cached_stats = UpdatedStats,
        cached_blocks = CachedBlocks
    },
    set_caching_timers(NewState).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% @equiv flush_blocks(State, [], [], false)
%% @end
%%--------------------------------------------------------------------
-spec flush_blocks(#state{}) ->
    {[], #state{}}.
flush_blocks(State) ->
    flush_blocks(State, [], [], false).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Flush aggregated so far file blocks.
%% @end
%%--------------------------------------------------------------------
-spec flush_blocks(#state{}, [session:id()], [{from(), block(), request_type()}],
    boolean()) -> {[{from(), #file_location_changed{}}], #state{}}.
flush_blocks(#state{cached_blocks = Blocks, file_ctx = FileCtx} = State, ExcludeSessions,
    FinalBlocks, IsTransfer) ->

    FlushFinalBlocks = case IsTransfer of
        true ->
            op_worker:get_env(synchronizer_transfer_finished_events, all);
        _ ->
            op_worker:get_env(synchronizer_on_fly_finished_events, all)
    end,

    Ans = lists:foldl(fun({From, FinalBlock, Type}, Acc) ->
        case Type of
                sync ->
                    [{From, flush_blocks_list([FinalBlock], ExcludeSessions,
                        FlushFinalBlocks)} | Acc];
                async ->
                    flush_blocks_list([FinalBlock], [], all),
                    Acc
        end
    end, [], FinalBlocks),

    case op_worker:get_env(synchronizer_in_progress_events, transfers_only) of
        off ->
            false;
        _ ->
            ToInvalidate = [FinalBlock || {_From, FinalBlock, _Type} <- FinalBlocks],
            Blocks2 = fslogic_blocks:consolidate(fslogic_blocks:invalidate(Blocks, ToInvalidate)),
            lists:foreach(fun(Block) ->
                flush_blocks_list([Block], ExcludeSessions, all)
            end, Blocks2)
    end,

    case op_worker:get_env(synchronizer_gc, on_flush_location) of
        on_flush_blocks ->
            erlang:garbage_collect();
        _ ->
            ok
    end,

    case fslogic_cache:get_local_location() of
        #document{value = #file_location{size = Size}} = LocationDoc ->
            case fslogic_location_cache:get_blocks(LocationDoc, #{count => 2}) of
                [#file_block{offset = 0, size = Size}] ->
                    fslogic_event_emitter:emit_file_attr_changed_with_replication_status(FileCtx, false, []);
                _ ->
                    ok
            end;
        _ ->
            % There can be race with file deletion
            ok
    end,

    {Ans, set_events_timer(cancel_caching_blocks_timer(
        State#state{cached_blocks = []}))}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Flush aggregated so far file blocks.
%% @end
%%--------------------------------------------------------------------
-spec flush_blocks_list([block()], [session:id()], all | off
    | {threshold, non_neg_integer()}) -> #file_location_changed{}.
flush_blocks_list(AllBlocks, ExcludeSessions, Flush) ->
    #file_location{blocks = FinalBlocks} = Location =
        fslogic_location:get_local_blocks_and_fill_location_gaps(AllBlocks, fslogic_cache:get_local_location(),
            fslogic_cache:get_all_locations(), fslogic_cache:get_uuid()),
    {ChangeOffset, ChangeSize} = fslogic_location_cache:get_blocks_range(Location, AllBlocks),

    case Flush of
        off ->
            ok;
        all ->
            fslogic_cache:cache_location_change(ExcludeSessions,
                {Location, ChangeOffset, ChangeSize});
        {threshold, Bytes} ->
            BlocksSize = fslogic_blocks:size(FinalBlocks),
            case BlocksSize >= Bytes of
                true ->
                    fslogic_cache:cache_location_change(ExcludeSessions,
                        {Location, ChangeOffset, ChangeSize});
                _ ->
                    ok
            end
    end,

    #file_location_changed{file_location = Location,
        change_beg_offset = ChangeOffset, change_end_offset = ChangeSize}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Flush aggregated so far transfer stats and optionally cancels timer.
%% @end
%%--------------------------------------------------------------------
-spec flush_stats(#state{}, boolean()) -> #state{}.
flush_stats(#state{cached_stats = Stats} = State, _) when map_size(Stats) == 0 ->
    State;
flush_stats(#state{
    space_id = SpaceId,
    transfer_id_to_stats_callback_module = TidToStatsCallback
} = State, CancelTimer) ->
    lists:foreach(fun({TransferId, BytesPerProvider}) ->
        StatsCallbackModule = maps:get(TransferId, TidToStatsCallback),
        try
            case StatsCallbackModule:flush_stats(SpaceId, TransferId, BytesPerProvider) of
                ok -> ok;
                {error, Error} ->
                    ?error(
                        "Failed to update transfer statistics using callback module ~p
                        for ~p transfer due to ~p", [StatsCallbackModule, TransferId, Error]
                    )
            end
        catch _:Reason:Stacktrace ->
            ?error_stacktrace(
                "Failed to update transfer statistics using callback module ~p for ~p transfer "
                "due to ~p", [StatsCallbackModule, TransferId, Reason], Stacktrace
            )
        end
    end, maps:to_list(State#state.cached_stats)),

    % TODO VFS-4412 emit rtransfer statistics
%%    monitoring_event_emitter:emit_rtransfer_statistics(
%%        SpaceId, UserId, get_summarized_blocks_size(AllBlocks)
%%    ),

    NewState = case CancelTimer of
        true ->
            cancel_caching_stats_timer(State#state{cached_stats = #{}});
        false ->
            State#state{cached_stats = #{}}
    end,

    case op_worker:get_env(synchronizer_gc, on_flush_location) of
        on_flush_stats ->
            erlang:garbage_collect();
        _ ->
            ok
    end,

    NewState.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Flush aggregated events and cancels timer.
%% @end
%%--------------------------------------------------------------------
-spec flush_events(#state{}) -> #state{}.
flush_events(State) ->
    lists:foreach(fun({ExcludedSessions, LocationChanges}) ->
        % TODO VFS-7396 catch error and repeat
        ok = fslogic_event_emitter:emit_file_locations_changed(
            lists:reverse(LocationChanges), ExcludedSessions)
    end, lists:reverse(fslogic_cache:clear_location_changes())),
    cancel_events_timer(State).

-spec set_caching_timers(#state{}) -> #state{}.
set_caching_timers(#state{caching_stats_timer = undefined} = State) ->
    TimerRef = erlang:send_after(?STATS_AGGREGATION_TIME, self(), ?FLUSH_STATS),
    set_caching_timers(State#state{caching_stats_timer = TimerRef});
set_caching_timers(#state{caching_blocks_timer = undefined} = State) ->
    TimerRef = erlang:send_after(?BLOCKS_AGGREGATION_TIME, self(), ?FLUSH_BLOCKS),
    State#state{caching_blocks_timer = TimerRef};
set_caching_timers(State) ->
    State.


-spec cancel_caching_stats_timer(#state{}) -> #state{}.
cancel_caching_stats_timer(#state{caching_stats_timer = undefined} = State) ->
    State;
cancel_caching_stats_timer(#state{caching_stats_timer = TimerRef} = State) ->
    erlang:cancel_timer(TimerRef, [{async, true}, {info, false}]),
    State#state{caching_stats_timer = undefined}.


-spec cancel_caching_blocks_timer(#state{}) -> #state{}.
cancel_caching_blocks_timer(#state{caching_blocks_timer = undefined} = State) ->
    State;
cancel_caching_blocks_timer(#state{caching_blocks_timer = TimerRef} = State) ->
    erlang:cancel_timer(TimerRef, [{async, true}, {info, false}]),
    State#state{caching_blocks_timer = undefined}.


-spec set_events_timer(#state{}) -> #state{}.
set_events_timer(#state{caching_events_timer = undefined} = State) ->
    TimerRef = erlang:send_after(?EVENTS_CACHING_TIME, self(), ?FLUSH_EVENTS),
    State#state{caching_events_timer = TimerRef};
set_events_timer(State) ->
    State.


-spec cancel_events_timer(#state{}) -> #state{}.
cancel_events_timer(#state{caching_events_timer = undefined} = State) ->
    State;
cancel_events_timer(#state{caching_events_timer = TimerRef} = State) ->
    erlang:cancel_timer(TimerRef, [{async, true}, {info, false}]),
    State#state{caching_events_timer = undefined}.

% TODO VFS-4412 emit rtransfer statistics
%%-spec get_summarized_blocks_size([block()]) -> non_neg_integer().
%%get_summarized_blocks_size(Blocks) ->
%%    lists:foldl(fun(#file_block{size = Size}, Acc) ->
%%        Acc + Size end, 0, Blocks).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Requests termination of all synchronizers.
%% @end
%%--------------------------------------------------------------------
-spec request_terminate(term()) -> [pid()].
request_terminate('$end_of_table') ->
    [];
request_terminate({Match, Continuation}) ->
    request_terminate(Match) ++ request_terminate(gproc:select(Continuation));
request_terminate(List) ->
    lists:map(fun([_, Pid, _]) -> Pid end, List).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Waits for synchronizers termination.
%% @end
%%--------------------------------------------------------------------
-spec wait_for_terminate(pid() | [pid()]) -> ok.
wait_for_terminate([]) ->
    ok;
wait_for_terminate([Pid | Pids]) ->
    wait_for_terminate(Pid),
    wait_for_terminate(Pids);
wait_for_terminate(Pid) ->
    case erlang:is_process_alive(Pid) of
        true ->
            timer:sleep(500),
            wait_for_terminate(Pid);
        _ ->
            ok
    end.

-spec wait_for_slave_check(pid() | [pid()]) -> ok.
wait_for_slave_check([]) ->
    ok;
wait_for_slave_check([Pid | Pids]) ->
    wait_for_slave_check(Pid),
    wait_for_slave_check(Pids);
wait_for_slave_check(Pid) ->
    receive
        {slave_checked, Pid} ->
            ok
    after
        1000 ->
            case erlang:is_process_alive(Pid) of
                true ->
                    wait_for_slave_check(Pid);
                _ ->
                    ok
            end
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Deletes replica of file.
%% Before deletion checks whether version of local replica is identical
%% or lesser to allowed.
%% NOTE!!! This function is not responsible for checking whether
%% given replica is unique.
%% @end
%%--------------------------------------------------------------------
-spec delete_whole_file_replica_internal(version_vector:version_vector(), from(), #state{}) ->
    {helper_process_spawned, pid()} | {error, term()}.
delete_whole_file_replica_internal(AllowedVV, RequestedBy, #state{file_ctx = FileCtx}) ->
    FileUuid = file_ctx:get_logical_uuid_const(FileCtx),
    try
        LocalFileLocId = file_location:local_id(FileUuid),
        ProviderId = oneprovider:get_id(),
        {ok, LocationDoc} = fslogic_location_cache:get_location(LocalFileLocId, FileUuid),
        CurrentVV = file_location:get_version_vector(LocationDoc),
        AllowedLocalV = version_vector:get_version(LocalFileLocId, ProviderId, AllowedVV),
        CurrentLocalV = version_vector:get_version(LocalFileLocId, ProviderId, CurrentVV),
        case AllowedLocalV =:= CurrentLocalV of
            true ->
                % Emmit events only when list of blocks is not empty
                % (nothing changes from clients perspective if blocks list is empty)
                EmitEvents = fslogic_location_cache:get_blocks(LocationDoc) =/= [],
                fslogic_location_cache:clear_blocks(FileCtx, LocalFileLocId),
                spawn_block_clearing(FileCtx, EmitEvents, RequestedBy);
            _ ->
                {error, file_modified_locally}
        end
    catch
        Error:Reason:Stacktrace ->
            ?error_stacktrace("Deletion of replica of file ~p failed due to ~p",
                [FileUuid, {Error, Reason}], Stacktrace),
            {error, Reason}
    end.

-spec spawn_block_clearing(file_ctx:ctx(), boolean(), from()) -> {helper_process_spawned, pid()}.
spawn_block_clearing(FileCtx, EmitEvents, RequestedBy) ->
    Master = self(),
    % Spawn file truncate on storage to prevent replica_synchronizer blocking.
    % Although spawning file truncate on storage can result in races,
    % similar races are possible truncating file via oneclient. Thus,
    % method of truncating file that does not block synchronizer is preferred.
    Pid = spawn_link(fun() ->
        Ans = try
            UserCtx = user_ctx:new(?ROOT_SESS_ID),
            #fuse_response{status = #status{code = ?OK}} = truncate_req:truncate_insecure(UserCtx, FileCtx, 0, false),
            ok
        catch
            E:R:Stacktrace ->
                FileUuid = file_ctx:get_logical_uuid_const(FileCtx),
                ?error_stacktrace("Unexpected error ~p:~p when truncating file ~p.", [E, R, FileUuid], Stacktrace),
                {error, {E, R}}
        end,
        Master ! {file_truncated, Ans, EmitEvents, RequestedBy}
    end),
    {helper_process_spawned, Pid}.

-spec finish_blocks_clearing(ok | {error, term()}, boolean(), #state{}) -> {ok | {error, term()}, #state{}}.
finish_blocks_clearing(ok = _Ans, true = _EmitEvents, #state{file_ctx = FileCtx} = State) ->
    State2 = flush_events(State),
    fslogic_event_emitter:emit_file_attr_changed_with_replication_status(FileCtx, false, []),
    {fslogic_event_emitter:emit_file_location_changed(FileCtx, []), State2};
finish_blocks_clearing(Ans, _EmitEvents, State) ->
    State2 = flush_events(State),
    {Ans, State2}.


-spec assert_smaller_than_local_support_size(non_neg_integer(), od_space:id()) -> ok.
assert_smaller_than_local_support_size(TotalSize, SpaceId) ->
    {ok, LocalSupportSize} = space_logic:get_support_size(SpaceId, oneprovider:get_id()),
    case TotalSize > LocalSupportSize of
        true -> throw(?ENOSPC);
        false -> ok
    end.


-spec flush_archive_helper_buffer_if_applicable(file_ctx:ctx()) -> ok.
flush_archive_helper_buffer_if_applicable(FileCtx) ->
    {Storage, FileCtx2} = file_ctx:get_storage(FileCtx),
    case storage:is_archive(Storage) of
        true ->
            {CanonicalPath, FileCtx3} = file_ctx:get_canonical_path(FileCtx2),
            case archivisation_tree:is_in_archive(CanonicalPath) of
                true ->
                    {SDHandle, FileCtx4} = storage_driver:new_handle(?ROOT_SESS_ID, FileCtx3),
                    {FileSize, _} = file_ctx:get_local_storage_file_size(FileCtx4),
                    ok = storage_driver:flushbuffer(SDHandle, FileSize);
                false ->
                    ok
            end;
        false ->
            ok
    end.