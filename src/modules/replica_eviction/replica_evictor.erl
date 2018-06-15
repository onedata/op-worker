%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% gen_server responsible for requesting support for eviction of files
%%% from other providers. It is responsible for limiting number of
%%% concurrent requests so that remote provider won't be flooded with them.
%%% One such server is started per node.
%%% @end
%%%-------------------------------------------------------------------
-module(replica_evictor).
-author("Jakub Kudzia").

-behaviour(gen_server).

-include("global_definitions.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include("modules/datastore/transfer.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/1, notify/3, cancel/2, cancelling_finished/2,
    notify_finished_task/1, process_result/4, evict/7, get_setting_for_eviction_task/1]).

%% function exported for monitoring performance
-export([check/1]).

%%TODO do usuniecia, to nie powinno przejsc przez PR !!!
-export([fix/1]).

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
-define(EVICT(FileUuid, ProviderId, Blocks, Version, Type), #eviction_task{
    uuid = FileUuid,
    supporting_provider = ProviderId,
    blocks = Blocks,
    version = Version,
    type = Type
}).
-define(NOTIFY(NotifyFun), #notification_task{notify_fun = NotifyFun}).
-define(CANCEL(ReportId), {cancel, ReportId}).
-define(CANCEL_DONE(ReportId), {cancel_done, ReportId}).
-define(FINISHED, finished).

-define(MAX_ACTIVE_TASKS, application:get_env(?APP_NAME, max_active_eviction_requests, 2000)).

-record(state, {
    space_id :: od_space:id(),
    active_tasks = 0 :: non_neg_integer(),
    queues = #{} :: #{replica_eviction:report_id() => queue:queue()},
    ids_queue = queue:new() :: queue:queue(),
    ids_to_cancel = gb_sets:new() :: gb_sets:set(),
    max_active_tasks = ?MAX_ACTIVE_TASKS
}).

-record(eviction_task, {
    uuid :: file_meta:uuid(),
    file :: file_ctx:ctx(),
    supporting_provider :: od_provider:id(),
    blocks :: fslogic_blocks:blocks(),
    version :: version_vector:version_vector(),
    type :: replica_eviction:type()
}).

-record(notification_task, {
   notify_fun :: notify_fun()
}).

-record(task, {
    task :: eviction_task() | notification_task(),
    id :: replica_eviction:report_id()
}).


-type task() :: #task{}.
-type eviction_task() :: #eviction_task{}.
-type notification_task() :: #notification_task{}.
-type notify_fun() :: fun(() -> term()).

%%TODO
%%TODO * handle too long queue of tasks and return error

%%%===================================================================
%%% API
%%%===================================================================

%%TODO wywalić
fix(SpaceId) ->
    gen_server2:cast(?SERVER(SpaceId), fix).

%%-------------------------------------------------------------------
%% @doc
%% Casts task for sending new eviction_request.
%% @end
%%-------------------------------------------------------------------
-spec evict(file_meta:uuid(), od_provider:id(), fslogic_blocks:blocks(),
    version_vector:version_vector(), replica_eviction:report_id(),
    replica_eviction:type(), od_space:id()) -> ok.
evict(FileUuid, ProviderId, Blocks, Version, ReportId, Type, SpaceId) ->
    ensure_started(SpaceId),
    gen_server2:cast(?SERVER(SpaceId), ?TASK(?EVICT(FileUuid, ProviderId,
        Blocks, Version, Type), ReportId)).

%%-------------------------------------------------------------------
%% @doc
%% Casts notification task. 
%% NotifyFun can be literally any function.
%% It will be called by replica_evictor to notify requester.
%% @end
%%-------------------------------------------------------------------
-spec notify(function(), replica_eviction:report_id(), od_space:id()) -> ok.
notify(NotifyFun, ReportId, SpaceId) ->
    gen_server2:cast(?SERVER(SpaceId), ?TASK(?NOTIFY(NotifyFun), ReportId)).

%%-------------------------------------------------------------------
%% @doc
%% Adds given ReportId to list of ids to bo cancelled.
%% All new arriving tasks and those popped from queue associated
%% with given ReportId will be cancelled.
%% NOTE!!! Queue is not searched for tasks to be cancelled.
%%         They just won't be executed when they reach head of queue. 
%% @end
%%-------------------------------------------------------------------
-spec cancel(replica_eviction:report_id(), od_space:id()) -> ok.
cancel(ReportId, SpaceId) ->
    gen_server2:cast(?SERVER(SpaceId), ?CANCEL(ReportId)).

%%-------------------------------------------------------------------
%% @doc
%% Removes given if from ids to be cancelled.
%% @end
%%-------------------------------------------------------------------
-spec cancelling_finished(replica_eviction:report_id(), od_space:id()) -> ok.
cancelling_finished(ReportId, SpaceId) ->
    gen_server2:cast(?SERVER(SpaceId), ?CANCEL_DONE(ReportId)).

%%-------------------------------------------------------------------
%% @doc
%% Sends message to server to notify about finished task.
%% @end
%%-------------------------------------------------------------------
-spec notify_finished_task(od_space:id()) -> ok.
notify_finished_task(SpaceId) ->
    ensure_started(SpaceId),
    gen_server2:cast(?SERVER(SpaceId), ?FINISHED).

%%-------------------------------------------------------------------
%% @doc
%% Function used to cast task for printing length of queue and number
%% of active tasks.
%% @end
%%-------------------------------------------------------------------
-spec check(od_space:id()) -> ok.
check(SpaceId) ->
    gen_server2:cast(?SERVER(SpaceId), check).

%%-------------------------------------------------------------------
%% @doc
%% Delegates processing of eviction results to appropriate model
%% @end
%%-------------------------------------------------------------------
-spec process_result(replica_eviction:type(), file_meta:uuid(), 
    replica_eviction:result(), replica_eviction:report_id()) -> ok.
process_result(autocleaning, FileUuid, Result, ReportId) ->
    autocleaning_controller:process_eviction_result(Result, FileUuid, ReportId);
process_result(invalidation, FileUuid, Result, ReportId) ->
    invalidation_worker:process_eviction_result(Result, FileUuid, ReportId).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%% @end
%%--------------------------------------------------------------------
-spec(start_link(od_space:id()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(SpaceId) ->
    gen_server:start_link(?SERVER(SpaceId), ?MODULE, [SpaceId], []).

%%-------------------------------------------------------------------
%% @doc
%% Returns setting for evictions tasks.
%% Finds supporting provider and blocks it can support
%% NOTE!!! currently it finds only providers who have whole file
%%         replicated and chooses one of them.
%% @end
%%-------------------------------------------------------------------
-spec get_setting_for_eviction_task(file_ctx:ctx()) ->
    {file_meta:uuid(), od_provider:id(), fslogic_blocks:blocks(),
        version_vector:version_vector()} | undefined.
get_setting_for_eviction_task(FileCtx) ->
    case file_ctx:get_local_file_location_doc(FileCtx) of
        {undefined, _} ->
            undefined;
        {LocalLocation, FileCtx2} ->
            VV = file_location:get_version_vector(LocalLocation),
            case replica_finder:get_blocks_available_to_evict(FileCtx2, VV) of
                {[{Provider, Blocks} | _], FileCtx3} ->
                    % todo handle retries to other providers
                    FileUuid = file_ctx:get_uuid_const(FileCtx3),
                    {FileUuid, Provider, Blocks, VV};
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
handle_call(_Request, _From, State) ->
    ?log_bad_request(_Request),
    {reply, ok, State}.


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
    io:format("Queue: ~p~nActive: ~p~n", [queue:len(IdsQueue), Active]),
    {noreply, State};
handle_cast(Task = #task{id = ReportId}, State = #state{
    active_tasks = ActiveTasks,
    ids_to_cancel = IdsToCancel,
    space_id = SpaceId,
    max_active_tasks = MaxActiveTasks
}) when ActiveTasks < MaxActiveTasks ->
    % handle task
    case gb_sets:is_element(ReportId, IdsToCancel) of
        true ->
            cancel_task(Task, SpaceId);
        false ->
            handle_task(Task, SpaceId)
    end,
    {noreply, State#state{active_tasks = ActiveTasks + 1}};
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
    }};
handle_cast(finished, State = #state{
    queues = Queues,
    ids_queue = IdsQueue,
    active_tasks = ActiveEvictionsNum
}) ->
    case queue:out(IdsQueue) of
        {empty, IdsQueue} ->
            State2 = State#state{
                active_tasks = ActiveEvictionsNum - 1
            },
            {noreply, State2};
        {{value, ReportId}, IdsQueue2} ->
            QueuePerId = maps:get(ReportId, Queues),
            {{value, Task}, QueuePerId2} = queue:out(QueuePerId),
            State2 = State#state{
                ids_queue = IdsQueue2,
                queues = Queues#{ReportId => QueuePerId2},
                active_tasks = ActiveEvictionsNum - 1
            },
            handle_cast(Task, State2)
    end;
handle_cast({cancel, ReportId}, State = #state{ids_to_cancel = CancelledIds}) ->
    {noreply, State#state{ids_to_cancel = gb_sets:add(ReportId, CancelledIds)}};
handle_cast({cancel_done, ReportId}, State = #state{ids_to_cancel = CancelledIds}) ->
    {noreply, State#state{ids_to_cancel = gb_sets:delete_any(ReportId, CancelledIds)}};
handle_cast(fix, State) ->
    {noreply, State#state{active_tasks = 0, queues = #{}, ids_queue = queue:new()}};
handle_cast(Request, State) ->
    ?log_bad_request(Request),
    {noreply, State}.

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
%% Ensures that server is started.
%% @end
%%-------------------------------------------------------------------
-spec ensure_started(od_space:id()) -> ok.
ensure_started(SpaceId) ->
    case start_link(SpaceId) of
        {error,{already_started, _}} ->
            ok;
        {ok, _} ->
            ok
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Handles given task.
%% @end
%%-------------------------------------------------------------------
-spec handle_task(task(), od_space:id()) -> ok.
handle_task(#task{
    task = #eviction_task{
        uuid = FileUuid,
        supporting_provider = ProviderId,
        blocks = Blocks,
        version = Version,
        type = Type
    },
    id = ReportId
}, SpaceId) ->
    {ok, _} = request_eviction_support(FileUuid, ProviderId, Blocks, Version, ReportId,
        Type, SpaceId),
    ok;
handle_task(#task{task = #notification_task{notify_fun = NotifyFun}}, SpaceId) ->
    NotifyFun(),
    notify_finished_task(SpaceId).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Requests eviction support
%% @end
%%-------------------------------------------------------------------
-spec request_eviction_support(file_meta:uuid(), od_provider:id(),
    fslogic_blocks:blocks(), version_vector:version_vector(),
    replica_eviction:report_id(), replica_eviction:type(), od_space:id()) ->
    {ok, replica_eviction:id()} | {error, term()}.
request_eviction_support(FileUuid, ProviderId, Blocks, Version, ReportId,
    Type, SpaceId
) ->
    replica_eviction:request(FileUuid, Blocks, Version, ProviderId, SpaceId,
        Type, ReportId).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Cancels task.
%% Notification tasks are just ignored. Eviction tasks are handled
%% by functions appropriate to eviction_task type.
%% @end
%%-------------------------------------------------------------------
-spec cancel_task(task(), od_space:id()) -> ok.
cancel_task(#task{task = #notification_task{}}, SpaceId) ->
    notify_finished_task(SpaceId);
cancel_task(#task{task = #eviction_task{type = Type}, id = ReportId}, SpaceId) ->
    mark_processed_file(Type, ReportId),
    notify_finished_task(SpaceId).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Delegates increasing processed_files counter to appropriate module
%% @end
%%-------------------------------------------------------------------
-spec mark_processed_file(replica_eviction:type(), replica_eviction:report_id()) -> ok.
mark_processed_file(autocleaning, AutocleaningId) ->
    {ok, _} = autocleaning:mark_processed_file(AutocleaningId),
    ok;
mark_processed_file(invalidation, TransferId) ->
    {ok, _} = transfer:increase_files_processed_counter(TransferId),
    ok.
