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
-include("modules/replica_deletion/replica_deletion.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/1,
    request_autocleaning_deletion/3,
    request_eviction_deletion/3,
    cancel_autocleaning_request/2,
    cancel_eviction_request/2,
    notify_handled_request/3,
    notify_handled_request_async/3,
    process_result/5,
    find_supporter_and_prepare_deletion_request/1,
    prepare_deletion_request/4]).

%% functions exported for RPC
-export([
    notify_handled_request_async_internal/3, call/2, call/3
]).

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

% functions exported for tests
-export([request_deletion/4]).

-define(SERVER(SpaceId), {global, {?MODULE, SpaceId}}).

% messages
-define(REQUEST(Request, JobId, JobType), #request{request = Request, job_id = JobId, job_type = JobType}).

-define(DELETION(FileUuid, ProviderId, Blocks, Version), #deletion{
    uuid = FileUuid,
    supporting_provider = ProviderId,
    blocks = Blocks,
    version = Version
}).
-define(CANCEL, cancel).
-define(CANCEL_DONE, cancel_done).
-define(FINISHED, finished).

%% The process is supposed to die after ?DIE_AFTER time of idling (no requests in flight)
-define(DIE_AFTER, timer:minutes(1)).   % todo czy na pewno chcemy zeby umierał???
-define(MAX_REQUESTS_NUM, application:get_env(?APP_NAME, replica_deletion_max_parallel_requests, 1000)).

-record(state, {
    space_id :: od_space:id(),
    active_requests_num = 0 :: non_neg_integer(),
    max_requests_num = ?MAX_REQUESTS_NUM,
    request_counters = #{} :: #{replica_deletion:job_id() => non_neg_integer()},   % todo opisac, ze tutaj sa tez te z kolejki
    ids_to_cancel = gb_sets:new() :: gb_sets:set(),
    queue = queue:new() :: queue:queue()
}).

-record(deletion, {
    uuid :: file_meta:uuid(),
    file :: file_ctx:ctx() | undefined,
    supporting_provider :: od_provider:id(),
    blocks :: fslogic_blocks:blocks(),
    version :: version_vector:version_vector()
}).

-record(request, {
    request :: deletion() ,
    job_id :: replica_deletion:job_id(),
    job_type :: replica_deletion:job_type()
}).

-type request() :: #request{}.

-type deletion() :: #deletion{}.
%%-type cancel() ::

-type state() :: #state{}.

% TODO ogarnąć kiedy ma byc stopowany/startowany !!!!
% TODO ogarnac kiedy wywalac refy z to_cancel seta

% TODO TESTY REPLICA DELETION !!!!!

-define(COUNTER_KEY(JobType, JobId), {JobType, JobId}).

%%%===================================================================
%%% API
%%%===================================================================
% TODO ogarnac czy dawac timeout czy infinity w callach !!!!

request_autocleaning_deletion(SpaceId, DeletionRequest, AutocleaningRunId) ->
    request_deletion(SpaceId, DeletionRequest, AutocleaningRunId, ?AUTOCLEANING_JOB).

request_eviction_deletion(SpaceId, DeletionRequest, TransferId) ->
    request_deletion(SpaceId, DeletionRequest, TransferId, ?EVICTION_JOB).

cancel_autocleaning_request(SpaceId, AutocleaningRunId) ->
    cancel(SpaceId, AutocleaningRunId, ?AUTOCLEANING_JOB).

cancel_eviction_request(SpaceId, TransferId) ->
    cancel(SpaceId, TransferId, ?EVICTION_JOB).

-spec notify_handled_request(od_space:id(), replica_deletion:job_id(), replica_deletion:job_type()) -> ok.
notify_handled_request(SpaceId, JobId, JobType) ->
    Node = datastore_key:responsible_node(SpaceId),
    rpc:call(Node, ?MODULE, call, [SpaceId, ?REQUEST(?FINISHED, JobId, JobType)]).


-spec notify_handled_request_async(od_space:id(), replica_deletion:job_id(), replica_deletion:job_type()) -> ok.
notify_handled_request_async(SpaceId, JobId, JobType) ->
    Node = datastore_key:responsible_node(SpaceId),
    rpc:call(Node, ?MODULE, notify_handled_request_async_internal, [SpaceId, JobId, JobType]).


-spec check(od_space:id()) -> ok.
check(SpaceId) ->
    Node = datastore_key:responsible_node(SpaceId),
    rpc:call(Node, ?MODULE, check_internal, [SpaceId]).


%%-------------------------------------------------------------------
%% @doc
%% Delegates processing of deletion results to appropriate model
%% @end
%%-------------------------------------------------------------------
-spec process_result(od_space:id(), file_meta:uuid(), replica_deletion:result(), replica_deletion:job_id(), replica_deletion:job_type()) -> ok.
process_result(SpaceId, FileUuid, Result, ARId, autocleaning) ->
    autocleaning_controller:process_replica_deletion_result(Result, SpaceId, FileUuid, ARId);
process_result(_SpaceId, FileUuid, Result, TransferId, eviction) ->
    replica_eviction_worker:process_replica_deletion_result(Result, FileUuid, TransferId).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%% @end
%%--------------------------------------------------------------------
-spec(start_link(od_space:id()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(SpaceId) ->
    Node = datastore_key:responsible_node(SpaceId),
    rpc:call(Node, gen_server2, start_link, [?SERVER(SpaceId), ?MODULE, [SpaceId], []]).

%%-------------------------------------------------------------------
%% @doc
%% Finds supporting provider and blocks that it can support.
%% Prepares deletion request if supporting provider was found.
%% NOTE!!! currently it finds only providers who have whole file
%%         replicated and chooses one of them.
%% @end
%%-------------------------------------------------------------------
-spec find_supporter_and_prepare_deletion_request(file_ctx:ctx()) -> undefined | deletion().
find_supporter_and_prepare_deletion_request(FileCtx) ->
    case find_deletion_supporter(FileCtx) of
        {undefined, _} ->
            undefined;
        {[{Provider, Blocks} | _], FileCtx2} ->
            % todo VFS-4628 handle retries to other providers
            FileUuid = file_ctx:get_uuid_const(FileCtx2),
            {LocalLocation, _} = file_ctx:get_local_file_location_doc(FileCtx2, false),
            VV = file_location:get_version_vector(LocalLocation),
            prepare_deletion_request(FileUuid, Provider, Blocks, VV)
    end.


-spec prepare_deletion_request(file_meta:uuid(), oneprovider:id(), fslogic_blocks:blocks(),
    version_vector:version_vector()) -> deletion().
prepare_deletion_request(FileUuid, Provider, Blocks, VV) ->
    ?DELETION(FileUuid, Provider, Blocks, VV).

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
    ?alert("INIT"),
    {ok, #state{space_id = SpaceId}}.

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
handle_call(Request = ?REQUEST(#deletion{}, JobId, JobType), From, State = #state{
    active_requests_num = RequestsNum,
    max_requests_num = MaxRequestsNum,
    request_counters = RequestCounters
}) when RequestsNum < MaxRequestsNum ->
    gen_server2:reply(From, ok),
    State2 = State#state{
        active_requests_num = RequestsNum + 1,
        request_counters = increase_request_counter(JobId, JobType, RequestCounters)
    },
    handle_deletion_request(Request, State2),

    %handle_cast(check, State2),

    {noreply, State2};
% todo musze dobrze ogarnac kiedy podbijac counter total, a keidy counter per ref w RequestCounters
% uwaga, bo w tej chwili podbijam chybaRequestCounters 2 razy
handle_call(Request = ?REQUEST(#deletion{}, JobId, JobType), From, State = #state{
    queue = Queue,
    request_counters = RequestCounters,
    active_requests_num = MaxRequestsNum,
    max_requests_num = MaxRequestsNum
}) ->
%%    ?alert("FULL: ~p", [queue:len(Queue)]),
%%    Q2 = queue:in({From, Request}, Queue),
%%    ?alert("FULL: ~p", [queue:len(Q2)]),
    % queue is full, don't reply so that the calling process is blocked

    State2 = State#state{
        queue = queue:in({From, Request}, Queue),
        request_counters = increase_request_counter(JobId, JobType, RequestCounters)
    },

    %handle_cast(check, State2),

    {noreply, State2};
%%handle_call(Request = ?REQUEST(#deletion{}, Ref, Type), From, State = #state{
%%    queue = Queue,
%%    request_counters = RequestCounters,
%%    active_requests_num = Total,
%%    max_requests_num = Max
%%}) ->
%%    ?alert("QUEUE is full: ~p", [{Total, Max}]),
%%    % queue is full, don't reply so that the calling process is blocked
%%    {noreply, State#state{
%%        queue = queue:in({From, Request}, Queue),
%%        request_counters = increase_request_counter(Ref, Type, RequestCounters)
%%    }};
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
handle_cast(check, State = #state{queue = Queue, active_requests_num = Active, request_counters = RequestCounters}) ->
    % TODO wywalić ten clause przed końcem ticketa !!!
    ?critical("~nQueue: ~p~nActive: ~p~nReqCounters: ~p~n", [queue:len(Queue), Active, map_size(RequestCounters)]),
    {noreply, State, ?DIE_AFTER};
%%handle_cast(Request = ?REQUEST(#deletion{}, _, _), State = #state{
%%    active_requests_num = TotalRequestsNum,
%%    max_requests_num = MaxRequestsNum
%%}) when TotalRequestsNum < MaxRequestsNum ->
%%    State2 = handle_deletion_request(Request, State),
%%    {noreply, State2};
handle_cast(?REQUEST(?FINISHED, JobId, JobType), State = #state{
    active_requests_num = TotalRequestsNum,
    request_counters = RequestCounters,
    queue = Queue
}) ->
%%    ?alert("Q LEN: ~p", [queue:len(Queue)]),
%%    ?alert("ACTIVE REQS: ~p", [TotalRequestsNum]),
%%    ?alert("COUNTERS: ~p", [map_size(RequestCounters)]),
%%    ?alert("FINITO"),

    case queue:out(Queue) of
        {empty, Queue} ->
            State2 = State#state{
                active_requests_num = TotalRequestsNum - 1,
                request_counters = decrease_request_counter(JobId, JobType, RequestCounters)
            },

            %handle_cast(check, State2),

            case State2#state.active_requests_num =:= 0 andalso map_size(State2#state.request_counters) =:= 0 of
                true ->
                    {stop, normal, State2};
                false ->
                    {noreply, State2}
            end;
        {{value, {From, Request}}, Queue2} ->
            gen_server2:reply(From, ok),
            State2 = State#state{
                queue = Queue2,
                request_counters = decrease_request_counter(JobId, JobType, RequestCounters)
            },

            %handle_cast(check, State2),


            handle_deletion_request(Request, State2),
            {noreply, State2}
    end;
handle_cast(?REQUEST(?CANCEL, JobId, _Type), State = #state{ids_to_cancel = CancelledIds}) ->
    {noreply, State#state{ids_to_cancel = gb_sets:add(JobId, CancelledIds)}, ?DIE_AFTER};
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
    ?alert("TERMINATE"),
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
%%% Internal functions exported for RPC
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% Sends asynchronous message to server to notify about finished task.
%% @end
%%-------------------------------------------------------------------
-spec notify_handled_request_async_internal(od_space:id(), replica_deletion:job_id(), replica_deletion:job_type()) -> ok.
notify_handled_request_async_internal(SpaceId, JobId, JobType) ->
    % todo delete this funciton???
    gen_server2:cast(?SERVER(SpaceId), ?REQUEST(?FINISHED, JobId, JobType)).

%%-------------------------------------------------------------------
%% @doc
%% Function used to cast task for printing length of queue and number
%% of active tasks.
%% @end
%%-------------------------------------------------------------------
-spec check_internal(od_space:id()) -> ok.
check_internal(SpaceId) ->
    call(SpaceId, check).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec request_deletion(od_space:id(), deletion(), replica_deletion:job_id(), replica_deletion:job_type()) -> ok.
request_deletion(SpaceId, DeletionRequest, JobId, JobType) ->
    Node = consistent_hashing:get_node(SpaceId),
    rpc:call(Node, ?MODULE, call, [SpaceId, ?REQUEST(DeletionRequest, JobId, JobType), infinity]).

%%-------------------------------------------------------------------
%% @doc
%% Adds given JobId to list of ids to bo cancelled.
%% All new arriving tasks and those popped from queue associated
%% with given JobId will be cancelled.
%% NOTE!!! Queue is not searched for tasks to be cancelled.
%%         They just won't be executed when they reach head of queue.
%% @end
%%-------------------------------------------------------------------
-spec cancel(od_space:id(), replica_deletion:job_id(), replica_deletion:job_type()) -> ok.
cancel(SpaceId, JobId, JobType) ->
    Node = consistent_hashing:get_node(SpaceId),
    rpc:call(Node, ?MODULE, call, [SpaceId, ?REQUEST(?CANCEL, JobId, JobType)]).


%% @private
-spec call(od_space:id(), term()) -> ok.
call(SpaceId, Request) ->
    call(SpaceId, Request, 5000).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Performs call to replica_deletion_master for given SpaceId.
%% If server is not started, this function starts it and performs
%% call one more time.
%% @end
%%-------------------------------------------------------------------
-spec call(od_space:id(), term(), non_neg_integer() | infinity) -> ok.
call(SpaceId, Request, Timeout) ->
    try
        gen_server2:call(?SERVER(SpaceId), Request, Timeout)
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

handle_deletion_request(Request = #request{request=#deletion{}, job_id = JobId}, #state{
    active_requests_num = TotalRequestsNum,
    max_requests_num = MaxRequestsNum,
    ids_to_cancel = IdsToCancel,
    space_id = SpaceId
}) when TotalRequestsNum =< MaxRequestsNum ->
    case gb_sets:is_element(JobId, IdsToCancel) of
    true ->
        cancel_request(Request, SpaceId);
    false ->
        maybe_send_request(Request, SpaceId)
    end.

%%handle_finished_request()

-spec maybe_send_request(request(), od_space:id()) -> state().
maybe_send_request(Request = #request{
    request = #deletion{
        uuid = FileUuid,
        supporting_provider = ProviderId,
        blocks = Blocks,
        version = Version
    },
    job_id = JobId,
    job_type = JobType
}, SpaceId
) ->
    % TODO: VFS-5573 use actual storage id
    StorageId = oneprovider:get_id(),
    case file_qos:is_replica_protected(FileUuid, StorageId) of
        false ->
            {ok, _} = request_deletion_support(SpaceId, FileUuid, ProviderId, Blocks, Version, JobId, JobType);
        true ->
            % This is needed to avoid deadlock as cancel_task
            % makes a synchronous call to this process.
            cancel_request(Request, SpaceId)
    end.


-spec request_deletion_support(od_space:id(), file_meta:uuid(), od_provider:id(),
    fslogic_blocks:blocks(), version_vector:version_vector(),
    replica_deletion:job_id(), replica_deletion:job_type()) ->
    {ok, replica_deletion:id()} | {error, term()}.
request_deletion_support(SpaceId, FileUuid, ProviderId, Blocks, Version, JobId, JobType) ->
    replica_deletion:request(FileUuid, Blocks, Version, ProviderId, SpaceId, JobType, JobId).


-spec cancel_request(request(), od_space:id()) -> ok.
cancel_request(#request{request = #deletion{uuid = FileUuid}, job_type = JobType, job_id = JobId}, SpaceId) ->
    process_result(SpaceId, FileUuid, {error, canceled}, JobId, JobType),
    notify_handled_request_async(SpaceId, JobId, JobType).


increase_request_counter(JobId, JobType, RequestCounters) ->
    maps:update_with(?COUNTER_KEY(JobId, JobType), fun(V) -> V + 1 end, 1, RequestCounters).

decrease_request_counter(JobId, JobType, RequestCounters) ->
    RequestCounters2 = maps:update_with(?COUNTER_KEY(JobId, JobType), fun(V) -> V - 1 end, RequestCounters),
%%    ?alert("RequestCounters2: ~p", [RequestCounters2]),
    case maps:get(?COUNTER_KEY(JobId, JobType), RequestCounters2) =:= 0 of
        true ->
            maps:remove(?COUNTER_KEY(JobId, JobType), RequestCounters2);
        false ->
            RequestCounters2
    end.

-spec find_deletion_supporter(file_ctx:ctx()) ->
    {undefined | [{od_provider:id(), fslogic_blocks:blocks()}], file_ctx:ctx()}.
find_deletion_supporter(FileCtx) ->
    {LocationDocs, FileCtx2} = file_ctx:get_file_location_docs(FileCtx, skip_local_blocks),
    case replica_finder:get_remote_duplicated_blocks(LocationDocs) of
        undefined ->
            {undefined, FileCtx2};
        [] ->
            {undefined, FileCtx2};
        DuplicatedBlocks ->
            {DuplicatedBlocks, FileCtx2}
    end.
