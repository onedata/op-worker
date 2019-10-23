%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% gen_server responsible for requesting support for deletion of files,
%%% in the given space, from other providers.
%%% It is responsible for limiting number of
%%% concurrent requests so that remote provider won't be flooded with them.
%%% One such server is started in the cluster.
%%% @end
%%%-------------------------------------------------------------------
-module(replica_deletion_master).
-author("Jakub Kudzia").

-behaviour(gen_server).

-include("global_definitions.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include("modules/datastore/transfer.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/1,
    enqueue_task/7, enqueue_task_internal/7,
    cancel/2, cancel_internal/2,
    cancelling_finished/2, cancelling_finished_internal/2,
    notify_finished_task/1, notify_finished_task_internal/1,
    notify_finished_task_async/1, notify_finished_task_async_internal/1,
    process_result/5, get_setting_for_deletion_task/1]).

%% function exported for monitoring performance
-export([check/1 , check_internal/1]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(SERVER(SpaceId), {global, {?MODULE, SpaceId}}).

% messages
-define(TASK(Task, ReportId), #task{task = Task, id = ReportId}).
-define(DELETE(FileUuid, ProviderId, Blocks, Version, Type), #deletion_task{
    uuid = FileUuid,
    supporting_provider = ProviderId,
    blocks = Blocks,
    version = Version,
    type = Type
}).
-define(CANCEL, cancel).
-define(CANCEL(ReportId), {?CANCEL, ReportId}).
-define(CANCEL_DONE, cancel_done).
-define(CANCEL_DONE(ReportId), {?CANCEL_DONE, ReportId}).
-define(FINISHED, finished).

%% The process is supposed to die after ?DIE_AFTER time of idling (no requests in flight)
-define(DIE_AFTER, timer:minutes(1)).
-define(MAX_ACTIVE_TASKS, application:get_env(?APP_NAME, max_active_deletion_tasks, 2000)).

-record(state, {
    space_id :: od_space:id(),
    active_tasks = 0 :: non_neg_integer(),
    queues = #{} :: #{replica_deletion:report_id() => queue:queue()},
    ids_queue = queue:new() :: queue:queue(),
    ids_to_cancel = gb_sets:new() :: gb_sets:set(),
    max_active_tasks = ?MAX_ACTIVE_TASKS
}).

-record(deletion_task, {
    uuid :: file_meta:uuid(),
    file :: file_ctx:ctx() | undefined,
    supporting_provider :: od_provider:id(),
    blocks :: fslogic_blocks:blocks(),
    version :: version_vector:version_vector(),
    type :: replica_deletion:type()
}).

-record(task, {
    task :: deletion_task(),
    id :: replica_deletion:report_id()
}).


-type task() :: #task{}.
-type deletion_task() :: #deletion_task{}.

%%TODO VFS-4625 handle too long queue of tasks and return error

%%%===================================================================
%%% API
%%%===================================================================


%%-------------------------------------------------------------------
%% @doc
%% @equiv enqueue_internal(FileUuid, ProviderId, Blocks, Version,
%%        ReportId, Type, SpaceId).  on chosen node.
%% @end
%%-------------------------------------------------------------------
-spec enqueue_task(file_meta:uuid(), od_provider:id(), fslogic_blocks:blocks(),
    version_vector:version_vector(), replica_deletion:report_id(),
    replica_deletion:type(), od_space:id()) -> ok.
enqueue_task(FileUuid, ProviderId, Blocks, Version, ReportId, Type, SpaceId) ->
    Node = consistent_hashing:get_node(SpaceId),
    rpc:call(Node, ?MODULE, enqueue_task_internal,
        [FileUuid, ProviderId, Blocks, Version, ReportId, Type, SpaceId]).

%%-------------------------------------------------------------------
%% @doc
%% Casts task for sending new replica_deletion request.
%% @end
%%-------------------------------------------------------------------
-spec enqueue_task_internal(file_meta:uuid(), od_provider:id(),
    fslogic_blocks:blocks(), version_vector:version_vector(),
    replica_deletion:report_id(), replica_deletion:type(), od_space:id()) -> ok.
enqueue_task_internal(FileUuid, ProviderId, Blocks, Version, ReportId, Type, SpaceId) ->
    call(SpaceId, ?TASK(?DELETE(FileUuid, ProviderId,
        Blocks, Version, Type), ReportId)).

%%-------------------------------------------------------------------
%% @doc
%% @equiv cancel_internal(ReportId, SpaceId) on chosen node.
%% @end
%%-------------------------------------------------------------------
-spec cancel(replica_deletion:report_id(), od_space:id()) -> ok.
cancel(ReportId, SpaceId) ->
    Node = consistent_hashing:get_node(SpaceId),
    rpc:call(Node, ?MODULE, cancel_internal, [ReportId, SpaceId]).

%%-------------------------------------------------------------------
%% @doc
%% Adds given ReportId to list of ids to bo cancelled.
%% All new arriving tasks and those popped from queue associated
%% with given ReportId will be cancelled.
%% NOTE!!! Queue is not searched for tasks to be cancelled.
%%         They just won't be executed when they reach head of queue.
%% @end
%%-------------------------------------------------------------------
-spec cancel_internal(replica_deletion:report_id(), od_space:id()) -> ok.
cancel_internal(ReportId, SpaceId) ->
    call(SpaceId, ?CANCEL(ReportId)).

%%-------------------------------------------------------------------
%% @doc
%% @equiv cancelling_finished_internal(ReportId, SpaceId) on chosen node
%% @end
%%-------------------------------------------------------------------
-spec cancelling_finished(replica_deletion:report_id(), od_space:id()) -> ok.
cancelling_finished(ReportId, SpaceId) ->
    Node = consistent_hashing:get_node(SpaceId),
    rpc:call(Node, ?MODULE, cancelling_finished_internal, [ReportId, SpaceId]).

%%-------------------------------------------------------------------
%% @doc
%% Removes given if from ids to be cancelled.
%% @end
%%-------------------------------------------------------------------
-spec cancelling_finished_internal(replica_deletion:report_id(), od_space:id()) -> ok.
cancelling_finished_internal(ReportId, SpaceId) ->
    call(SpaceId, ?CANCEL_DONE(ReportId)).

%%-------------------------------------------------------------------
%% @doc
%% @equiv notify_finished_task_internal(SpaceId) on chosen node.
%% @end
%%-------------------------------------------------------------------
-spec notify_finished_task(od_space:id()) -> ok.
notify_finished_task(SpaceId) ->
    Node = consistent_hashing:get_node(SpaceId),
    rpc:call(Node, ?MODULE, notify_finished_task_internal, [SpaceId]).

%%-------------------------------------------------------------------
%% @doc
%% Sends message to server to notify about finished task.
%% @end
%%-------------------------------------------------------------------
-spec notify_finished_task_internal(od_space:id()) -> ok.
notify_finished_task_internal(SpaceId) ->
    call(SpaceId, ?FINISHED).

%%-------------------------------------------------------------------
%% @doc
%% @equiv notify_finished_task_async_internal(SpaceId) on chosen node.
%% @end
%%-------------------------------------------------------------------
-spec notify_finished_task_async(od_space:id()) -> ok.
notify_finished_task_async(SpaceId) ->
    Node = consistent_hashing:get_node(SpaceId),
    rpc:call(Node, ?MODULE, notify_finished_task_async_internal, [SpaceId]).

%%-------------------------------------------------------------------
%% @doc
%% Sends message to server to notify about finished task.
%% @end
%%-------------------------------------------------------------------
-spec notify_finished_task_async_internal(od_space:id()) -> ok.
notify_finished_task_async_internal(SpaceId) ->
    gen_server2:cast(?SERVER(SpaceId), ?FINISHED).

%%-------------------------------------------------------------------
%% @doc
%% @equiv check_internal(SpaceId)
%% @end
%%-------------------------------------------------------------------
-spec check(od_space:id()) -> ok.
check(SpaceId) ->
    Node = consistent_hashing:get_node(SpaceId),
    rpc:call(Node, ?MODULE, check_internal, [SpaceId]).

%%-------------------------------------------------------------------
%% @doc
%% Function used to cast task for printing length of queue and number
%% of active tasks.
%% @end
%%-------------------------------------------------------------------
-spec check_internal(od_space:id()) -> ok.
check_internal(SpaceId) ->
    call(SpaceId, check).

%%-------------------------------------------------------------------
%% @doc
%% Delegates processing of deletion results to appropriate model
%% @end
%%-------------------------------------------------------------------
-spec process_result(replica_deletion:type(), od_space:id(), file_meta:uuid(),
    replica_deletion:result(), replica_deletion:report_id()) -> ok.
process_result(autocleaning, SpaceId, FileUuid, Result, ARId) ->
    autocleaning_controller:process_replica_deletion_result(Result, SpaceId, FileUuid, ARId);
process_result(eviction, _SpaceId, FileUuid, Result, TransferId) ->
    replica_eviction_worker:process_replica_deletion_result(Result, FileUuid, TransferId).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%% @end
%%--------------------------------------------------------------------
-spec(start_link(od_space:id()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(SpaceId) ->
    Node = consistent_hashing:get_node(SpaceId),
    rpc:call(Node, gen_server2, start_link, [?SERVER(SpaceId), ?MODULE, [SpaceId], []]).

%%-------------------------------------------------------------------
%% @doc
%% Returns setting for deletion tasks.
%% Finds supporting provider and blocks it can support
%% NOTE!!! currently it finds only providers who have whole file
%%         replicated and chooses one of them.
%% @end
%%-------------------------------------------------------------------
-spec get_setting_for_deletion_task(file_ctx:ctx()) ->
    {file_meta:uuid(), od_provider:id(), fslogic_blocks:blocks(),
        version_vector:version_vector()} | undefined.
get_setting_for_deletion_task(FileCtx) ->
    case file_ctx:get_local_file_location_doc(FileCtx, false) of
        {undefined, _} ->
            undefined;
        {LocalLocation, FileCtx2} ->
            VV = file_location:get_version_vector(LocalLocation),
            case replica_finder:get_duplicated_blocks(FileCtx2, VV) of
                {[{Provider, Blocks} | _], FileCtx3} ->
                    % todo VFS-4628 handle retries to other providers
                    FileUuid = file_ctx:get_uuid_const(FileCtx3),
                    {FileUuid, Provider, Blocks, VV};
                {[], _} ->
                    undefined;
                {undefined, _} ->
                    undefined
            end
    end.


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
    {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore).
init([SpaceId]) ->
    {ok, #state{
        space_id = SpaceId
    }}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
    {reply, Reply :: term(), NewState :: #state{}} |
    {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_call(Request, From, State) ->
    gen_server2:reply(From, ok),
    handle_cast(Request, State).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_cast(check, State = #state{ids_queue = IdsQueue, active_tasks = Active}) ->
    ?critical("~nQueue: ~p~nActive: ~p~n", [queue:len(IdsQueue), Active]),
    {noreply, State, ?DIE_AFTER};
handle_cast(Task = #task{id = ReportId}, State = #state{
    active_tasks = ActiveTasks,
    ids_to_cancel = IdsToCancel,
    space_id = SpaceId,
    max_active_tasks = MaxActiveTasks
}) when ActiveTasks < MaxActiveTasks ->
    % handle task
    case gb_sets:is_element(ReportId, IdsToCancel) of
        true ->
            ok = cancel_task(Task, SpaceId);
        false ->
            ok = handle_task(Task, SpaceId)
    end,
    {noreply, State#state{active_tasks = ActiveTasks + 1}, ?DIE_AFTER};
handle_cast(Task = #task{id = ReportId}, State = #state{
    queues = Queues,
    ids_queue = IdsQueue
}) ->
    % there are too many active requests, add this one to queue
    QueuePerId = maps:get(ReportId, Queues, queue:new()),
    QueuePerId2 = queue:in(Task, QueuePerId),
    {noreply, State#state{
        queues = Queues#{ReportId => QueuePerId2},
        ids_queue = queue:in(ReportId, IdsQueue)
    }, ?DIE_AFTER};
handle_cast(?FINISHED, State = #state{
    queues = Queues,
    ids_queue = IdsQueue,
    active_tasks = ActiveTasks
}) ->
    case queue:out(IdsQueue) of
        {empty, IdsQueue} ->
            State2 = State#state{
                active_tasks = ActiveTasks - 1
            },
            {noreply, State2, ?DIE_AFTER};
        {{value, ReportId}, IdsQueue2} ->
            QueuePerId = maps:get(ReportId, Queues),
            {{value, Task}, QueuePerId2} = queue:out(QueuePerId),
            State2 = State#state{
                ids_queue = IdsQueue2,
                queues = Queues#{ReportId => QueuePerId2},
                active_tasks = ActiveTasks - 1
            },
            handle_cast(Task, State2)
    end;
handle_cast(?CANCEL(ReportId), State = #state{ids_to_cancel = CancelledIds}) ->
    {noreply, State#state{ids_to_cancel = gb_sets:add(ReportId, CancelledIds)}, ?DIE_AFTER};
handle_cast(?CANCEL_DONE(ReportId), State = #state{ids_to_cancel = CancelledIds}) ->
    {noreply, State#state{ids_to_cancel = gb_sets:delete_any(ReportId, CancelledIds)}, ?DIE_AFTER};
handle_cast(Request, State) ->
    ?log_bad_request(Request),
    {noreply, State, ?DIE_AFTER}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%% @end
%%--------------------------------------------------------------------
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term()).
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
    {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Performs call to replica_deletion_master for given SpaceId.
%% If server is not started, this function starts it and performs
%% call one more time.
%% @end
%%-------------------------------------------------------------------
-spec call(od_space:id(), term()) -> ok.
call(SpaceId, Request) ->
    try
        gen_server2:call(?SERVER(SpaceId), Request)
    catch
        exit:{noproc, _} ->
            start_link(SpaceId),
            call(SpaceId, Request);
        exit:{normal, _} ->
            start_link(SpaceId),
            call(SpaceId, Request);
        exit:{{shutdown, timeout}, _} ->
            start_link(SpaceId),
            call(SpaceId, Request)
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Handles given task.
%% @end
%%-------------------------------------------------------------------
-spec handle_task(task(), od_space:id()) -> ok.
handle_task(#task{
    task = #deletion_task{
        uuid = FileUuid,
        supporting_provider = ProviderId,
        blocks = Blocks,
        version = Version,
        type = Type
    },
    id = ReportId
} = Task, SpaceId) ->
    % TODO: VFS-5573 use actual storage id
    StorageId = oneprovider:get_id(),
    case file_qos:is_replica_protected(FileUuid, StorageId) of
        false ->
            {ok, _} = request_deletion_support(FileUuid, ProviderId, Blocks, Version, ReportId,
                Type, SpaceId),
            ok;
        true ->
            % This is needed to avoid deadlock as cancel_task
            % makes a synchronous call to this process.
            cancel_task(Task, SpaceId)
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Requests deletion support
%% @end
%%-------------------------------------------------------------------
-spec request_deletion_support(file_meta:uuid(), od_provider:id(),
    fslogic_blocks:blocks(), version_vector:version_vector(),
    replica_deletion:report_id(), replica_deletion:type(), od_space:id()) ->
    {ok, replica_deletion:id()} | {error, term()}.
request_deletion_support(FileUuid, ProviderId, Blocks, Version, ReportId,
    Type, SpaceId
) ->
    replica_deletion:request(FileUuid, Blocks, Version, ProviderId, SpaceId,
        Type, ReportId).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Cancels task.
%% Notification tasks are just ignored. Deletion tasks are handled
%% by functions appropriate to deletion_task type.
%% @end
%%-------------------------------------------------------------------
-spec cancel_task(task(), od_space:id()) -> ok.
cancel_task(#task{task = #deletion_task{type = Type}, id = ReportId}, SpaceId) ->
    mark_processed_file(Type, ReportId, SpaceId),
    notify_finished_task_async(SpaceId).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Delegates increasing processed_files counter to appropriate module
%% @end
%%-------------------------------------------------------------------
-spec mark_processed_file(replica_deletion:type(), replica_deletion:report_id(),
    od_space:id()) -> ok.
mark_processed_file(autocleaning, ReportId, SpaceId) ->
    autocleaning_controller:notify_processed_file(SpaceId, ReportId);
mark_processed_file(eviction, TransferId, _) ->
    {ok, _} = transfer:increment_files_processed_counter(TransferId),
    ok.
