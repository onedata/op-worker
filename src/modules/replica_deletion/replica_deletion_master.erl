%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% gen_server responsible for requesting support for deletion of files,
%%% in the given space, from other providers.
%%% One such server is started in the cluster.
%%% It is responsible for limiting number of
%%% concurrent requests so that DBSync won't be flooded with them.
%%% The mechanism of limiting number of requests works as follows:
%%%   * when number of currently processed deletion requests is lesser
%%%     than ?MAX_REQUESTS_NUM, the requesting process is replied immediately
%%%     with an 'ok' (similarly to "cast")
%%%   * when number of currently processed deletion requests is equal to
%%%     ?MAX_REQUESTS_NUM, the requesting process is not replied, and the
%%%     request is queued. As a result, the requesting process is blocked.
%%%     As both mechanisms that use replica_deletion
%%%     (autocleaning and replica_eviction) use pool of workers, all processes
%%%     in the corresponding pools will be blocked and DBSync won't be flooded.
%%%     Blocked request will be popped from the queue when any of currently
%%%     processed requests is finished. When request is popped from the
%%%     queue, requesting process is replied and is no longer blocked.
%%%
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
-export([start/1,
    request_autocleaning_deletion/3,
    request_eviction_deletion/3,
    cancel_autocleaning_request/2,
    cancel_eviction_request/2,
    notify_handled_request/3,
    process_result/5,
    find_supporter_and_prepare_deletion_request/1,
    prepare_deletion_request/4]).

%% functions exported for RPC
-export([call/2]).

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

% request
-define(REQUEST(RequestType, JobId, JobType), #request{type = RequestType, job_id = JobId, job_type = JobType}).

% message types
-define(DELETION(FileUuid, ProviderId, Blocks, Version), #deletion{
    uuid = FileUuid,
    supporting_provider = ProviderId,
    blocks = Blocks,
    version = Version
}).
-define(CANCEL, cancel).
-define(FINISHED, finished).


-define(MAX_REQUESTS_NUM, application:get_env(?APP_NAME, replica_deletion_max_parallel_requests, 1000)).

-record(state, {
    space_id :: od_space:id(),
    active_requests_num = 0 :: non_neg_integer(),
    max_requests_num = ?MAX_REQUESTS_NUM,
    % counters of scheduled deletion requests associated with given job_id
    % queued requests are also included
    request_counters = #{} :: #{job_key() => non_neg_integer()},
    jobs_to_cancel = gb_sets:new() :: gb_sets:set(job_key()),
    queue = queue:new() :: queue:queue({request(), From :: {pid(), Tag :: term()}})
}).

-record(request, {
    type :: request_type(),
    job_id :: replica_deletion:job_id(),
    job_type :: replica_deletion:job_type()
}).
-record(deletion, {
    uuid :: file_meta:uuid(),
    supporting_provider :: od_provider:id(),
    blocks :: fslogic_blocks:blocks(),
    version :: version_vector:version_vector()
}).


-type request() :: #request{}.
-type request_type() :: deletion() | ?CANCEL | ?FINISHED.
-type deletion() :: #deletion{}.
-type job_key() :: {replica_deletion:job_id(), replica_deletion:job_type()}.

-type state() :: #state{}.

-define(JOB_KEY(JobId, JobType), {JobId, JobType}).

%%%===================================================================
%%% API
%%%===================================================================

-spec(start(od_space:id()) -> {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start(SpaceId) ->
    Node = datastore_key:responsible_node(SpaceId),
    rpc:call(Node, gen_server2, start, [?SERVER(SpaceId), ?MODULE, [SpaceId], []]).

-spec request_autocleaning_deletion(od_space:id(), deletion(), autocleaning_run_controller:batch_id()) -> ok.
request_autocleaning_deletion(SpaceId, DeletionRequest, BatchId) ->
    request_deletion(SpaceId, DeletionRequest, BatchId, ?AUTOCLEANING_JOB).

-spec request_eviction_deletion(od_space:id(), deletion(), transfer:id()) -> ok.
request_eviction_deletion(SpaceId, DeletionRequest, TransferId) ->
    request_deletion(SpaceId, DeletionRequest, TransferId, ?EVICTION_JOB).

-spec cancel_autocleaning_request(od_space:id(), autocleaning_run_controller:batch_id()) -> ok.
cancel_autocleaning_request(SpaceId, BatchId) ->
    cancel(SpaceId, BatchId, ?AUTOCLEANING_JOB).

-spec cancel_eviction_request(od_space:id(), transfer:id()) -> ok.
cancel_eviction_request(SpaceId, TransferId) ->
    cancel(SpaceId, TransferId, ?EVICTION_JOB).

-spec notify_handled_request(od_space:id(), replica_deletion:job_id(), replica_deletion:job_type()) -> ok.
notify_handled_request(SpaceId, JobId, JobType) ->
    call(SpaceId, ?REQUEST(?FINISHED, JobId, JobType)).

%%-------------------------------------------------------------------
%% @doc
%% Delegates processing of deletion results to appropriate model
%% @end
%%-------------------------------------------------------------------
-spec process_result(od_space:id(), file_meta:uuid(), replica_deletion:result(), replica_deletion:job_id(),
    replica_deletion:job_type()) -> ok.
process_result(SpaceId, FileUuid, Result, BatchId, ?AUTOCLEANING_JOB) ->
    autocleaning_run_controller:process_replica_deletion_result(Result, SpaceId, FileUuid, BatchId);
process_result(_SpaceId, FileUuid, Result, TransferId, ?EVICTION_JOB) ->
    replica_eviction_worker:process_replica_deletion_result(Result, FileUuid, TransferId).

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

-spec(init([od_space:id()]) -> {ok, state()} |{stop, Reason :: term()} | ignore).
init([SpaceId]) ->
    {ok, #state{space_id = SpaceId}}.


-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()}, state()) ->
    {reply, Reply :: term(), state()} |
    {reply, Reply :: term(), state(), timeout() | hibernate} |
    {noreply, state()} |
    {noreply, state(), timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), state()} |
    {stop, Reason :: term(), state()}).
handle_call(Request = ?REQUEST(#deletion{}, JobId, JobType), From, State = #state{
    active_requests_num = RequestsNum,
    max_requests_num = MaxRequestsNum
}) when RequestsNum < MaxRequestsNum ->
    gen_server2:reply(From, ok),
    State2 = State#state{active_requests_num = RequestsNum + 1},
    State3 = increase_request_counter(JobId, JobType, State2),
    handle_deletion_request(Request, State3),
    {noreply, State3};
handle_call(Request = ?REQUEST(#deletion{}, JobId, JobType), From, State = #state{
    queue = Queue,
    active_requests_num = MaxRequestsNum,
    max_requests_num = MaxRequestsNum
}) ->
    % queue is full, don't reply so that the calling process is blocked
    State2 = State#state{queue = queue:in({From, Request}, Queue)},
    State3 = increase_request_counter(JobId, JobType, State2),
    {noreply, State3};
handle_call(Request, From, State) ->
    gen_server2:reply(From, ok),
    handle_cast(Request, State).


-spec(handle_cast(Request :: term(), state()) ->
    {noreply, state()} |
    {noreply, state(), timeout() | hibernate} |
    {stop, Reason :: term(), state()}).
handle_cast(?REQUEST(?FINISHED, JobId, JobType), State = #state{
    active_requests_num = ActiveRequestsNum,
    queue = Queue
}) ->
    case queue:out(Queue) of
        {empty, Queue} ->
            State2 = State#state{active_requests_num = ActiveRequestsNum2 = ActiveRequestsNum - 1},
            State3 = #state{request_counters = RequestCounters2} = decrease_request_counter(JobId, JobType, State2),
            case ActiveRequestsNum2 =:= 0 andalso map_size(RequestCounters2) =:= 0 of
                true ->
                    {stop, normal, State3};
                false ->
                    {noreply, State3}
            end;
        {{value, {From, Request}}, Queue2} ->
            gen_server2:reply(From, ok),
            State2 = State#state{queue = Queue2},
            State3 = decrease_request_counter(JobId, JobType, State2),
            handle_deletion_request(Request, State3),
            {noreply, State3}
    end;
handle_cast(?REQUEST(?CANCEL, JobId, JobType), State = #state{jobs_to_cancel = JobsToCancel}) ->
    {noreply, State#state{jobs_to_cancel = gb_sets:add(?JOB_KEY(JobId, JobType), JobsToCancel)}};
handle_cast(Request, State) ->
    ?log_bad_request(Request),
    {noreply, State}.


-spec(handle_info(Info :: timeout() | term(), state()) ->
    {noreply, state()} |
    {noreply, state(), timeout() | hibernate} |
    {stop, Reason :: term(), state()}).
handle_info(Info, State) ->
    ?log_bad_request(Info),
    {noreply, State}.


-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    state()) -> term()).
terminate(Reason, State) ->
    ?log_terminate(Reason, State).


-spec(code_change(OldVsn :: term() | {down, term()}, state(),
    Extra :: term()) ->
    {ok, state()} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec notify_handled_request_async(od_space:id(), replica_deletion:job_id(), replica_deletion:job_type()) -> ok.
notify_handled_request_async(SpaceId, JobId, JobType) ->
    gen_server2:cast(?SERVER(SpaceId), ?REQUEST(?FINISHED, JobId, JobType)).

-spec request_deletion(od_space:id(), deletion(), replica_deletion:job_id(), replica_deletion:job_type()) -> ok.
request_deletion(SpaceId, DeletionRequest, JobId, JobType) ->
    call(SpaceId, ?REQUEST(DeletionRequest, JobId, JobType)).

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
    call(SpaceId, ?REQUEST(?CANCEL, JobId, JobType)).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Performs call to replica_deletion_master for given SpaceId.
%% If server is not started, this function starts it and performs
%% call one more time.
%% @end
%%-------------------------------------------------------------------
-spec call(od_space:id(), request() | term()) -> ok.
call(SpaceId, Request) ->
    try
        gen_server2:call(?SERVER(SpaceId), Request, infinity)
    catch
        exit:{noproc, _} ->
            start(SpaceId),
            call(SpaceId, Request);
        exit:{normal, _} ->
            start(SpaceId),
            call(SpaceId, Request);
        exit:{{shutdown, timeout}, _} ->
            start(SpaceId),
            call(SpaceId, Request)
    end.

-spec handle_deletion_request(request(), state()) -> ok.
handle_deletion_request(Request = #request{type =#deletion{}, job_id = JobId, job_type = JobType}, #state{
    active_requests_num = ActiveRequestsNum,
    max_requests_num = MaxRequestsNum,
    jobs_to_cancel = JobsToCancel,
    space_id = SpaceId
}) when ActiveRequestsNum =< MaxRequestsNum ->
    case gb_sets:is_element(?JOB_KEY(JobId, JobType), JobsToCancel) of
        true ->
            cancel_request(Request, SpaceId);
        false ->
            maybe_send_request(Request, SpaceId)
    end.

-spec maybe_send_request(request(), od_space:id()) -> ok.
maybe_send_request(Request = #request{
    type = #deletion{
        uuid = FileUuid,
        supporting_provider = ProviderId,
        blocks = Blocks,
        version = Version
    },
    job_id = JobId,
    job_type = JobType
}, SpaceId
) ->
    {StorageId, _} = file_ctx:get_storage_id(file_ctx:new_by_guid(file_id:pack_guid(FileUuid, SpaceId))),
    case file_qos:is_replica_required_on_storage(FileUuid, StorageId) of
        false ->
            {ok, _} = request_deletion_support(SpaceId, FileUuid, ProviderId, Blocks, Version, JobId, JobType),
            ok;
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
cancel_request(#request{type = #deletion{uuid = FileUuid}, job_type = JobType, job_id = JobId}, SpaceId) ->
    process_result(SpaceId, FileUuid, {error, canceled}, JobId, JobType),
    notify_handled_request_async(SpaceId, JobId, JobType).

-spec increase_request_counter(replica_deletion:job_id(), replica_deletion:job_type(), state()) -> state().
increase_request_counter(JobId, JobType, State = #state{request_counters = RequestCounters}) ->
    State#state{
        request_counters = maps:update_with(?JOB_KEY(JobId, JobType), fun(V) -> V + 1 end, 1, RequestCounters)
    }.

-spec decrease_request_counter(replica_deletion:job_id(), replica_deletion:job_type(), state()) -> state().
decrease_request_counter(JobId, JobType, State = #state{
    request_counters = RequestCounters,
    jobs_to_cancel = JobsToCancel
}) ->
    RequestCounters2 = maps:update_with(?JOB_KEY(JobId, JobType), fun(V) -> V - 1 end, RequestCounters),
    JobKey = ?JOB_KEY(JobId, JobType),
    case maps:get(JobKey, RequestCounters2) =:= 0 of
        true ->
            State#state{
                request_counters = maps:remove(JobKey, RequestCounters2),
                jobs_to_cancel = gb_sets:del_element(JobKey, JobsToCancel)
            };
        false ->
            State#state{request_counters = RequestCounters2}
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
