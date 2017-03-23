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

-include("modules/datastore/datastore_specific_models_def.hrl").
-include("modules/storage_sync/strategy_config.hrl").
-include("global_definitions.hrl").
-include("modules/storage_sync/space_sync_worker.hrl").
-include_lib("cluster_worker/include/elements/worker_host/worker_protocol.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").

-define(INFINITY, 9999999999999999999999).
-define(SYNC_JOB_TIMEOUT,  timer:hours(24)).
-define(ASYNC_JOB_TIMEOUT,  timer:seconds(10)).

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
    timer:apply_after(?SPACE_STRATEGIES_CHECK_INTERVAL, worker_proxy, cast,
        [?MODULE, check_strategies]),
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
    timer:apply_after(?SPACE_STRATEGIES_CHECK_INTERVAL, worker_proxy, cast,
        [?MODULE, check_strategies]);
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
        {ok, #document{value = #od_provider{spaces = SpaceIds}}} =
            od_provider:get_or_fetch(oneprovider:get_provider_id()),
        check_strategies(SpaceIds)
    catch
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
        {ok, #document{value = #space_storage{storage_ids = StorageIds}}} =
            space_storage:get(SpaceId),
        check_strategies(SpaceId, StorageIds)
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
        {ok, #storage_strategies{last_import_time = LastImportTime}} ->
            start_storage_import_and_update(SpaceId, StorageId, LastImportTime);
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
    integer() | undefined) -> ok.
start_storage_import_and_update(SpaceId, StorageId, LastImportTime) ->
    StorageLogicalFileId = <<"/", SpaceId/binary>>,
    ImportRes = storage_import:start(SpaceId, StorageId, LastImportTime,
        StorageLogicalFileId, ?INFINITY),
    %% @todo: do smth with this result and save new last_import_time
    ?debug("space_sync_worker ImportRes ~p", [ImportRes]),


    UpdateRes = storage_update:start(SpaceId, StorageId, LastImportTime,
        StorageLogicalFileId, ?INFINITY),
    %% @todo: do smth with this result
    ?debug("space_sync_worker UpdateRes ~p", [UpdateRes]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Runs given job and all its subjobs.
%% @end
%%--------------------------------------------------------------------
-spec run_job(space_strategy:job()) -> space_strategy:job_result().
run_job(Job = #space_strategy_job{strategy_type = StrategyType}) ->
    MergeType = merge_type(Job),
    {LocalResult, NextJobs} =
        try StrategyType:strategy_handle_job(Job) of
            {LocalResult0, NextJobs0} ->
                {LocalResult0, NextJobs0}
        catch
            _:Reason ->
                {{error, Reason}, []}
        end,
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
run_and_merge_all(Jobs = [
    #space_strategy_job{
        strategy_type = StrategyType}
| _]) ->
    Responses = utils:pmap(fun(Job) ->
        PoolName = pool_name(Job#space_strategy_job.strategy_type),
        worker_proxy:call_pool(?MODULE, {run_job, undefined, Job},
            PoolName, ?SYNC_JOB_TIMEOUT)
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
    utils:pforeach(fun(Job) ->
        PoolName = pool_name(Job#space_strategy_job.strategy_type),
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
    utils:pforeach(fun(Job) ->
        PoolName = pool_name(Job#space_strategy_job.strategy_type),
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
    lists:foreach(fun({PoolName, WorkersNumKey}) ->
        start_pool(PoolName, WorkersNumKey)
    end, ?SPACE_SYNC_WORKER_POOLS).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Starts worker pool.
%% @end
%%--------------------------------------------------------------------
-spec start_pool(atom(), atom()) -> {ok, pid()}.
start_pool(PoolName, WorkersNumKey) ->
    {ok, WorkersNum} = application:get_env(?APP_NAME, WorkersNumKey),
    {ok, _ } = wpool:start_sup_pool(PoolName, [{workers, WorkersNum}]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Stops worker pools used by ?MODULE.
%% @end
%%--------------------------------------------------------------------
-spec stop_pools() -> ok.
stop_pools() ->
    lists:foreach(fun({PoolName, _}) ->
        worker_pool:stop_pool(PoolName)
    end, ?SPACE_SYNC_WORKER_POOLS).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Maps space strategy name to worker pool name.
%% @end
%%--------------------------------------------------------------------
-spec pool_name(space_strategy:type()) -> atom().
pool_name(storage_import) -> ?STORAGE_IMPORT_POOL_NAME;
pool_name(storage_update) -> ?STORAGE_UPDATE_POOL_NAME;
pool_name(_) -> ?GENERIC_STRATEGY_POOL_NAME.