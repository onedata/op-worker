%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Main space strategy worker. Checks all set strategies in system
%%% and runs all scheduled strategies.
%%% @end
%%%-------------------------------------------------------------------
-module(space_sync_worker).
-author("Rafal Slota").
-behavior(worker_plugin_behaviour).

-include("modules/datastore/datastore_models.hrl").
-include("modules/storage_sync/strategy_config.hrl").
-include("global_definitions.hrl").
-include_lib("cluster_worker/include/elements/worker_host/worker_protocol.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/api_errors.hrl").
-include("modules/fslogic/fslogic_common.hrl").

-define(SYNC_JOB_TIMEOUT, timer:hours(24)).
-define(ASYNC_JOB_TIMEOUT, timer:seconds(10)).

%% Interval between successive check strategies.
-define(SPACE_STRATEGIES_CHECK_INTERVAL, check_strategies_interval).


%%%===================================================================
%%% Types
%%%===================================================================

%%%===================================================================
%%% Exports
%%%===================================================================

%% Types
-export_type([]).

%% Callbacks
-export([init/1, handle/1, cleanup/0]).

%% API
-export([init/4, run/1]).

%%%===================================================================
%%% worker_plugin_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback init/1.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) -> Result when
    Result :: {ok, State :: worker_host:plugin_state()} | {error, Reason :: term()}.
init(_Args) ->
    start_pools(),
    storage_sync_monitoring:start_lager_reporter(),
    storage_sync_monitoring:start_ets_reporter(),
    schedule_check_strategy(),
    {ok, #{}}.

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback handle/1.
%% @end
%%--------------------------------------------------------------------
-spec handle(Request) -> Result when
    Request :: ping | healthcheck | term(),
    Result :: nagios_handler:healthcheck_response() | ok | {ok, Response} |
    {error, Reason} | pong,
    Response :: term(),
    Reason :: term().
handle(ping) ->
    pong;
handle(healthcheck) ->
    ok;
handle(check_strategies) ->
    ?debug("Check strategies"),
    check_strategies(),
    schedule_check_strategy();
handle(Request = {run_job, _, Job = #space_strategy_job{}}) ->
    ?debug("Run job: ~p", [Request]),
    run_job(Job);
handle(_Request) ->
    ?log_bad_request(_Request),
    {error, wrong_request}.

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback cleanup/0
%% @end
%%--------------------------------------------------------------------
-spec cleanup() -> Result when
    Result :: ok | {error, Error},
    Error :: timeout | term().
cleanup() ->
    stop_pools(),
    storage_sync_monitoring:delete_lager_reporter(),
    storage_sync_monitoring:delete_ets_reporter(),
    ok.

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Initializes strategy operation. Context returned by this function
%% can be used to run given strategy with run/1 function.
%% @end
%%--------------------------------------------------------------------
-spec init(StrategyType :: space_strategy:type(), SpaceId :: od_space:id(),
    StorageId :: storage:id() | undefined,
    InitData :: space_strategy:job_data()) -> space_strategy:runnable().
init(StrategyType, SpaceId, StorageId, InitData) ->
    {ok, #document{value = SpaceStrategies}} = space_strategies:get(SpaceId),
    {Strategy, Args} = strategy_config(StrategyType, StorageId, SpaceStrategies),
    {
        merge_type(StrategyType, Strategy),
        StrategyType:strategy_init_jobs(Strategy, Args, InitData)
    }.

%%--------------------------------------------------------------------
%% @doc
%% Executes initialized with init/4 strategy operation. It's possible to
%% pass multiple initialized operations to this functions to get list
%% of results.
%% @end
%%--------------------------------------------------------------------
-spec run([space_strategy:runnable()] | space_strategy:runnable()) ->
    [space_strategy:job_result()] | space_strategy:job_result().
run(JobsWithMerge) when is_list(JobsWithMerge) ->
    [run(JobWithMerge) || JobWithMerge <- JobsWithMerge];

run({_, []}) ->
    ok;
run({merge_all, Jobs}) ->
    run_and_merge_all(Jobs);
run({return_first, Jobs}) ->
    run_and_return_first(Jobs);
run({return_none, Jobs}) ->
    run_and_return_none(Jobs).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is responsible for checking and optionally starting
%% storage_import and storage_update strategies for given provider.
%% @end
%%--------------------------------------------------------------------
-spec check_strategies() -> ok.
check_strategies() ->
    try
        ProviderId = oneprovider:get_id(),
        case provider_logic:get_spaces(ProviderId) of
            {ok, Spaces} ->
                check_strategies(Spaces);
            {error, _} ->
                ok
        end
    catch
        throw:?ERROR_UNREGISTERED_PROVIDER ->
            ok;
        _:TReason ->
            ?error_stacktrace("Unable to check space strategies due to: ~p", [TReason])
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is responsible for checking and optionally starting
%% storage_import and storage_update strategies for each space supported
%% by provider.
%% @end
%%--------------------------------------------------------------------
-spec check_strategies([od_space:id()]) -> ok.
check_strategies(SpaceIds) ->
    lists:foreach(fun(SpaceId) ->
        case space_storage:get(SpaceId) of
            {ok, #document{value = #space_storage{storage_ids = StorageIds}}} ->
                check_strategies(SpaceId, StorageIds);
            _ ->
                storage_sync_monitoring:ensure_all_metrics_stopped(SpaceId)
        end
    end, SpaceIds).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is responsible for checking and optionally starting
%% storage_import and storage_update strategies for given space
%% supported by given storages
%% @end
%%--------------------------------------------------------------------
-spec check_strategies(od_space:id(), [storage:id()]) -> ok.
check_strategies(SpaceId, StorageIds) ->
    lists:foreach(fun(StorageId) ->
        case space_strategies:get(SpaceId) of
            {ok, #document{value = #space_strategies{
                storage_strategies = StorageStrategies
            }}} ->
                maybe_start_storage_import_and_update(SpaceId, StorageId,
                    StorageStrategies);
            _ ->
                ok
        end
    end, StorageIds).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This functions starts storage_import and storage_update strategies
%% if they are set for given storage supporting given space
%% @end
%%--------------------------------------------------------------------
-spec maybe_start_storage_import_and_update(od_space:id(), storage:id(),
    maps:map()) -> ok.
maybe_start_storage_import_and_update(SpaceId, StorageId, StorageStrategies) ->
    case maps:find(StorageId, StorageStrategies) of
        {ok, #storage_strategies{
            import_finish_time = ImportFinishTime,
            import_start_time = ImportStartTime,
            last_update_start_time = LastUpdateStartTime,
            last_update_finish_time = LastUpdateFinishTime
        }} ->
            start_storage_import_and_update(SpaceId, StorageId, ImportStartTime,
                ImportFinishTime, LastUpdateStartTime, LastUpdateFinishTime);
        error ->
            ok
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This functions starts storage_import and storage_update strategies
%% for given storage supporting given space
%% @end
%%--------------------------------------------------------------------
-spec start_storage_import_and_update(od_space:id(), storage:id(),
    space_strategy:timestamp(), space_strategy:timestamp(),
    space_strategy:timestamp(), space_strategy:timestamp()) -> ok.
start_storage_import_and_update(SpaceId, StorageId, ImportStartTime, ImportFinishTime,
    LastUpdateStartTime, LastUpdateFinishTime
) ->
    RootDirCtx = file_ctx:new_root_ctx(),
    ImportAns = storage_import:start(SpaceId, StorageId, ImportStartTime, ImportFinishTime,
        RootDirCtx, SpaceId),
    log_import_answer(ImportAns, SpaceId, StorageId),

    UpdateAns = storage_update:start(SpaceId, StorageId, ImportFinishTime,
        LastUpdateStartTime, LastUpdateFinishTime, RootDirCtx, SpaceId),
    log_update_answer(UpdateAns, SpaceId, StorageId).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% @equiv log_answer(Answer, SpaceId, StorageId, import).
%% @end
%%--------------------------------------------------------------------
-spec log_import_answer([space_strategy:job_result()] | space_strategy:job_result(),
    od_space:id(), storage:id()) -> ok.
log_import_answer(Answer, SpaceId, StorageId) ->
    log_answer(Answer, SpaceId, StorageId, import).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% @equiv log_answer(Answer, SpaceId, StorageId, update).
%% @end
%%--------------------------------------------------------------------
-spec log_update_answer([space_strategy:job_result()] | space_strategy:job_result(),
    od_space:id(), storage:id()) -> ok.
log_update_answer(Answer, SpaceId, StorageId) ->
    log_answer(Answer, SpaceId, StorageId, update).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is responsible for logging output of storage_import:start
%% or storage_
%% @end
%%--------------------------------------------------------------------
-spec log_answer([space_strategy:job_result()] | space_strategy:job_result(),
    od_space:id(), storage:id(), atom()) -> ok.
log_answer({error, Reason}, SpaceId, StorageId, Strategy) ->
    ?error("~p of storage: ~p  supporting space: ~p failed due to: ~p",
        [Strategy, StorageId, SpaceId, Reason]);
log_answer(Answer, SpaceId, StorageId, Strategy) ->
    ?debug("~p of storage: ~p  supporting space: ~p finished with: ~p",
        [Strategy, StorageId, SpaceId, Answer]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Runs given job and all its subjobs.
%% @end
%%--------------------------------------------------------------------
-spec run_job(space_strategy:job()) -> space_strategy:job_result().
run_job(Job = #space_strategy_job{strategy_type = StrategyType}) ->
    MergeType = merge_type(Job),
    JobResult = try
        StrategyType:strategy_handle_job(Job)
    catch
        Type:Reason ->
            ?error_stacktrace("Job: ~p failed with ~p:~p", [Job, Type, Reason]),
            {{error, Reason}, []}
    end,
    handle_job_result(Job, StrategyType, MergeType, JobResult).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Runs given job and all its subjobs.
%% @end
%%--------------------------------------------------------------------
-spec handle_job_result(space_strategy:job(), space_strategy:type(),
    space_strategy:job_merge_type(),
    {space_strategy:job_result(), [space_strategy:job()]}) ->
    space_strategy:job_result().
handle_job_result(Job, StrategyType, MergeType, {LocalResult, NextJobs}) ->
    ChildrenResult = run({MergeType, NextJobs}),
    StrategyType:strategy_merge_result(Job, LocalResult, ChildrenResult).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Runs given jobs. Each job is performed by worker from appropriate
%% pool of workers. This function awaits for all jobs to be handled.
%% Available worker pools are:
%%  * ?STORAGE_IMPORT_POOL_NAME - for jobs associated with
%%                                storage_import strategy
%%  * ?STORAGE_UPDATE_POOL_NAME - for jobs associated with
%%                                storage_update strategy
%%  * ?GENERIC_STRATEGY_POOL_NAME- for jobs associated with other
%%                                 strategies
%% Number of workers in each pool is defined in app.config under
%% keys (consecutively): ?STORAGE_IMPORT_WORKERS_NUM,
%% ?STORAGE_UPDATE_WORKERS_NUM, ?GENERIC_STRATEGY_WORKERS_NUM.
%% @end
%%--------------------------------------------------------------------
-spec run_and_merge_all([space_strategy:job()]) -> space_strategy:job_result().
run_and_merge_all([]) -> ok;
run_and_merge_all(Jobs = [
    #space_strategy_job{
        strategy_type = StrategyType}
    | _]) ->
    PoolName = StrategyType:main_worker_pool(),
    Responses = utils:pmap(fun(Job) ->
        case (StrategyType =:= storage_import) or (StrategyType =:= storage_update) of
            true ->
                worker_proxy:call_pool(?MODULE, {run_job, undefined, Job},
                    PoolName, ?SYNC_JOB_TIMEOUT);
            false ->
                worker_proxy:call_direct(?MODULE, {run_job, undefined, Job}, ?SYNC_JOB_TIMEOUT)
        end
    end, Jobs),
    StrategyType:strategy_merge_result(Jobs, Responses).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Runs given jobs. Each job is performed by worker from appropriate
%% pool of workers. This function returns result of first job that was
%% handled.
%% Worker pools are described in doc of run_and_merge_all/1 function.
%% @end
%%--------------------------------------------------------------------
-spec run_and_return_first([space_strategy:job()]) ->
    space_strategy:job_result().
run_and_return_first(Jobs) ->
    ReplyTo = {proc, self()},
    utils:pforeach(fun(Job = #space_strategy_job{strategy_type = StrategyType}) ->
        PoolName = StrategyType:main_worker_pool(),
        worker_proxy:cast_pool(?MODULE, {run_job, undefined, Job},
            PoolName, ReplyTo)
    end, Jobs),
    receive
        Response ->
            Response
    after ?ASYNC_JOB_TIMEOUT ->
        {error, timeout}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Runs given jobs asynchronously. Each job is performed by worker
%% from appropriate pool of workers.
%% Worker pools are described in doc of run_and_merge_all/1 function.
%% @end
%%--------------------------------------------------------------------
-spec run_and_return_none([space_strategy:job()]) ->
    space_strategy:job_result().
run_and_return_none(Jobs) ->
    lists:foreach(fun(Job = #space_strategy_job{strategy_type = StrategyType}) ->
        PoolName = StrategyType:main_worker_pool(),
        worker_proxy:cast_pool(?MODULE, {run_job, undefined, Job}, PoolName)
    end, Jobs),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% For given strategy job, returns job result's merge type.
%% @end
%%--------------------------------------------------------------------
-spec merge_type(space_strategy:job()) ->
    space_strategy:job_merge_type().
merge_type(#space_strategy_job{
    strategy_type = StrategyType,
    strategy_name = StrategyName
}) ->
    merge_type(StrategyType, StrategyName).

%%--------------------------------------------------------------------
%% @doc
%% For given strategy type and name, returns job result's merge type.
%% @end
%%--------------------------------------------------------------------
-spec merge_type(space_strategy:type(), space_strategy:name()) ->
    space_strategy:job_merge_type().
merge_type(StrategyType, StrategyName) ->
    [#space_strategy{result_merge_type = MergeType}] =
        lists:filter(fun(#space_strategy{name = Name}) ->
            Name == StrategyName
        end, StrategyType:available_strategies()),
    MergeType.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% For given strategy type extracts and returns strategy name and
%% strategy arguments (aka strategy config).
%% @end
%%--------------------------------------------------------------------
-spec strategy_config(space_strategy:type(), storage:id() | undefined,
    #space_strategies{}) -> space_strategy:config() | [space_strategy:config()].
strategy_config(StrategyType, StorageId, SpaceStrategies =
    #space_strategies{storage_strategies = StorageStrategies}
) ->

    case StrategyType of
        filename_mapping ->
            #storage_strategies{filename_mapping = {Strategy, Args}} =
                maps:get(StorageId, StorageStrategies),
            {Strategy, Args};
        storage_import ->
            #storage_strategies{storage_import = {Strategy, Args}} =
                maps:get(StorageId, StorageStrategies),
            {Strategy, Args};
        storage_update ->
            #storage_strategies{storage_update = {Strategy, Args}} =
                maps:get(StorageId, StorageStrategies),
            {Strategy, Args};
        file_conflict_resolution ->
            #space_strategies{
                file_conflict_resolution = {Strategy, Args}} = SpaceStrategies,
            {Strategy, Args};
        file_caching ->
            #space_strategies{
                file_caching = {Strategy, Args}} = SpaceStrategies,
            {Strategy, Args};
        enoent_handling ->
            #space_strategies{
                enoent_handling = {Strategy, Args}} = SpaceStrategies,
            {Strategy, Args}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Starts worker pools used by ?MODULE.
%% @end
%%--------------------------------------------------------------------
-spec start_pools() -> ok.
start_pools() ->
    PoolsConfigs = lists:flatmap(fun(StrategyType) ->
        StrategyType:worker_pools_config()
    end, space_strategy:types()),

    lists:foreach(fun({PoolName, WorkersNum}) ->
        start_pool(PoolName, WorkersNum)
    end, sets:to_list(sets:from_list(PoolsConfigs))).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Starts worker pool.
%% @end
%%--------------------------------------------------------------------
-spec start_pool(atom(), non_neg_integer()) -> {ok, pid()}.
start_pool(PoolName, WorkersNum) ->
    {ok, _} = worker_pool:start_sup_pool(PoolName, [{workers, WorkersNum}, {queue_type, lifo}]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Stops worker pools used by ?MODULE.
%% @end
%%--------------------------------------------------------------------
-spec stop_pools() -> ok.
stop_pools() ->
    PoolsConfigs = lists:flatmap(fun(StrategyType) ->
        StrategyType:worker_pools_config()
    end, space_strategy:types()),
    Pools = [PoolName || {PoolName, _} <- PoolsConfigs],
    lists:foreach(fun(PoolName) ->
        true = worker_pool:stop_pool(PoolName)
    end, sets:to_list(sets:from_list(Pools))).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Maps space strategy name to worker pool name.
%% @end
%%--------------------------------------------------------------------
-spec schedule_check_strategy() -> {ok, term()}.
schedule_check_strategy() ->
    {ok, Interval} = application:get_env(?APP_NAME, ?SPACE_STRATEGIES_CHECK_INTERVAL),
    timer:apply_after(timer:seconds(Interval), worker_proxy, cast,
        [?MODULE, check_strategies]).