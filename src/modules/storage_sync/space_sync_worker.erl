%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Main space strategy worker. Checks all set strategies in system and runs all scheduled
%%  strategies.
%%% @end
%%%-------------------------------------------------------------------
-module(space_sync_worker).
-author("Rafal Slota").
-behavior(worker_plugin_behaviour).

-include("modules/datastore/datastore_specific_models_def.hrl").
-include("modules/storage_sync/strategy_config.hrl").
-include_lib("cluster_worker/include/elements/worker_host/worker_protocol.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").

-define(INFINITY, 9999999999999999999999).
-define(SPACE_STRATEGIES_CHECK_INTERVAL, timer:seconds(10)).

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
    timer:apply_after(?SPACE_STRATEGIES_CHECK_INTERVAL, worker_proxy, cast, [?MODULE, check_strategies]),
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
handle(check_strategies = Request) ->
    ?debug("Check strategies ~p", [Request]),
    try
        {ok, #document{value = #od_provider{spaces = SpaceIds}}} =
            od_provider:get_or_fetch(oneprovider:get_provider_id()),
        [worker_proxy:cast(?MODULE, {check_strategies, SpaceId}) || SpaceId <- SpaceIds]
    catch
        _:TReason ->
            ?error_stacktrace("Unable to check space strategies due to: ~p", [TReason])
    end,
    timer:apply_after(?SPACE_STRATEGIES_CHECK_INTERVAL, worker_proxy, cast, [?MODULE, check_strategies]),
    ok;
handle({check_strategies, SpaceId} = Request) ->
    ?debug("Check strategies ~p", [Request]),

    {ok, #document{value = #space_storage{storage_ids = StorageIds}}} = space_storage:get(SpaceId),
    [worker_proxy:cast(?MODULE, {check_strategies, SpaceId, StorageId}) || StorageId <- StorageIds];
handle({check_strategies, SpaceId, StorageId} = Request) ->
    ?debug("Check strategies ~p", [Request]),

    {ok, #document{value = #space_strategies{
        storage_strategies = StorageStrategies
    }}} = space_strategies:get(SpaceId),
    case maps:find(StorageId, StorageStrategies) of
        {ok, #storage_strategies{last_import_time = LastImportTime}} ->
            InitialImportJobData =
                #{
                    last_import_time => LastImportTime,
                    space_id => SpaceId,
                    storage_id => StorageId,
                    storage_file_id => <<"/", SpaceId/binary>>,
                    max_depth => ?INFINITY
                },

            %% Handle initial import
            Import = init(storage_import, SpaceId, StorageId, InitialImportJobData),
            ImportRes = run(Import),
            %% @todo: do smth with this result and save new last_import_time
            ?debug("space_sync_worker ImportRes ~p", [ImportRes]),

            Update = init(storage_update, SpaceId, StorageId, InitialImportJobData),
            UpdateRes = run(Update),
            %% @todo: do smth with this result
            ?debug("space_sync_worker UpdateRes ~p", [UpdateRes]);
        error ->
            ok
    end;

handle({run_job, _, Job = #space_strategy_job{strategy_type = StrategyType}}) ->
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
    StrategyType:strategy_merge_result(Job, LocalResult, ChildrenResult);
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
    ok.

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Initializes strategy operation. Context returned by this function can be used to run
%% given strategy with run/1 function.
%% @end
%%--------------------------------------------------------------------
-spec init(StrategyType :: space_strategy:type(), SpaceId :: od_space:id(),
    StorageId :: storage:id() | undefined, InitData :: space_strategy:job_data()) ->
    space_strategy:runnable().
init(StrategyType, SpaceId, StorageId, InitData) ->
    {ok, #document{value = SpaceStrategies}} = space_strategies:get(SpaceId),

    case strategy_config(StrategyType, StorageId, SpaceStrategies) of
        {Strategy, Args} ->
            {merge_type(StrategyType, Strategy), StrategyType:strategy_init_jobs(Strategy, Args, InitData)};
        Strategies ->
            [{merge_type(StrategyType, Strategy), StrategyType:strategy_init_jobs(Strategy, Args, InitData)}
                || {Strategy, Args} <- Strategies]
    end.

%%--------------------------------------------------------------------
%% @doc
%% Executes initialized with init/4 strategy operation. It's possible to
%% pass multiple initialized operations to this functions to get list of results.
%% @end
%%--------------------------------------------------------------------
-spec run([space_strategy:runnable()] | space_strategy:runnable()) ->
    [space_strategy:job_result()] | space_strategy:job_result().
run(JobsWithMerge) when is_list(JobsWithMerge) ->
    [run(JobWithMerge) || JobWithMerge <- JobsWithMerge];

run({_, []}) ->
    ok;
run({merge_all, [#space_strategy_job{strategy_type = StrategyType} | _ ] = Jobs}) ->
    Responses = utils:pmap(fun(Job) -> worker_proxy:call(?MODULE, {run_job, undefined, Job}, timer:hours(24)) end, Jobs),
    StrategyType:strategy_merge_result(Jobs, Responses);
run({return_first, Jobs}) ->
    [worker_proxy:cast(?MODULE, {run_job, undefined, Job}, {proc, self()}) || Job <- Jobs],
    receive
        Response ->
            Response
    after timer:seconds(10) ->
        {error, timeout}
    end;
run({return_none, Jobs}) ->
    [worker_proxy:cast(?MODULE, {run_job, undefined, Job}) || Job <- Jobs],
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% For given strategy job, returns job result's merge type.
%% @end
%%--------------------------------------------------------------------
-spec merge_type(space_strategy:job()) ->
    space_strategy:job_merge_type().
merge_type(#space_strategy_job{strategy_type = StrategyType, strategy_name = StrategyName}) ->
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
        [Strategy || #space_strategy{name = Name}
            = Strategy <- StrategyType:available_strategies(), Name == StrategyName],
    MergeType.

%%--------------------------------------------------------------------
%% @doc
%% For given strategy type extracts and returns strategy name and strategy arguments (aka strategy config).
%% @end
%%--------------------------------------------------------------------
-spec strategy_config(space_strategy:type(), storage:id() | undefined, #space_strategies{}) ->
    space_strategy:config() | [space_strategy:config()].
strategy_config(StrategyType, StorageId, SpaceStrategies = #space_strategies{storage_strategies = StorageStrategies}) ->
    case StrategyType of
        filename_mapping ->
            #storage_strategies{filename_mapping = {Strategy, Args}} = maps:get(StorageId, StorageStrategies),
            {Strategy, Args};
        storage_import ->
            #storage_strategies{storage_import = {Strategy, Args}} = maps:get(StorageId, StorageStrategies),
            {Strategy, Args};
        storage_update ->
            #storage_strategies{storage_update = Strategies} = maps:get(StorageId, StorageStrategies),
            Strategies;
        file_conflict_resolution ->
            #space_strategies{file_conflict_resolution = {Strategy, Args}} = SpaceStrategies,
            {Strategy, Args};
        file_caching ->
            #space_strategies{file_caching = {Strategy, Args}} = SpaceStrategies,
            {Strategy, Args};
        enoent_handling ->
            #space_strategies{enoent_handling = {Strategy, Args}} = SpaceStrategies,
            {Strategy, Args}
    end.