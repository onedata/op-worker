%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Strategy for resolving file configs.
%%% @end
%%%-------------------------------------------------------------------
-module(file_conflict_resolution).
-author("Rafal Slota").
-behavior(space_strategy_behaviour).

-include("modules/storage_sync/strategy_config.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%%%===================================================================
%%% Types
%%%===================================================================

%%%===================================================================
%%% Exports
%%%===================================================================

%% Types
-export_type([]).

%% space_strategy_behaviour callbacks
-export([available_strategies/0, strategy_init_jobs/3, strategy_handle_job/1,
    main_worker_pool/0, strategy_merge_result/2, strategy_merge_result/3,
    worker_pools_config/0
]).

%% API
-export([]).

%%%===================================================================
%%% space_strategy_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link space_strategy_behaviour} callback available_strategies/0.
%% @end
%%--------------------------------------------------------------------
-spec available_strategies() -> [space_strategy:definition()].
available_strategies() ->
    [
        #space_strategy{name = ignore_conflicts, arguments = [],
            description = <<"Ignore all file conflicts">>}
    ].

%%--------------------------------------------------------------------
%% @doc
%% {@link space_strategy_behaviour} callback strategy_init_jobs/3.
%% @end
%%--------------------------------------------------------------------
-spec strategy_init_jobs(space_strategy:name(), space_strategy:arguments(), space_strategy:job_data()) ->
    [space_strategy:job()].
strategy_init_jobs(StrategyName, StartegyArgs, InitData) ->
    [
        #space_strategy_job{strategy_name = StrategyName, strategy_args = StartegyArgs, data = InitData}
    ].

%%--------------------------------------------------------------------
%% @doc
%% {@link space_strategy_behaviour} callback strategy_handle_job/1.
%% @end
%%--------------------------------------------------------------------
-spec strategy_handle_job(space_strategy:job()) -> {space_strategy:job_result(), [space_strategy:job()]}.
strategy_handle_job(#space_strategy_job{strategy_name = ignore_conflicts}) ->
    {ok, []}.

%%--------------------------------------------------------------------
%% @doc
%% {@link space_strategy_behaviour} callback strategy_merge_result/2.
%% @end
%%--------------------------------------------------------------------
-spec strategy_merge_result(ChildrenJobs :: [space_strategy:job()],
    ChildrenResults :: [space_strategy:job_result()]) ->
    space_strategy:job_result().
strategy_merge_result([_Job | _], [Result | _]) ->
    Result.

%%--------------------------------------------------------------------
%% @doc
%% {@link space_strategy_behaviour} callback strategy_merge_result/3.
%% @end
%%--------------------------------------------------------------------
-spec strategy_merge_result(space_strategy:job(), LocalResult :: space_strategy:job_result(),
    ChildrenResult :: space_strategy:job_result()) ->
    space_strategy:job_result().
strategy_merge_result(#space_strategy_job{}, LocalResult, _ChildrenResult) ->
    LocalResult.

%%--------------------------------------------------------------------
%% @doc
%% {@link space_strategy_behaviour} callback worker_pools_config/0.
%% @end
%%--------------------------------------------------------------------
-spec worker_pools_config() -> [{worker_pool:name(), non_neg_integer()}].
worker_pools_config() ->
    space_strategy:default_worker_pool_config().

%%--------------------------------------------------------------------
%% @doc
%% {@link space_strategy_behaviour} callback main_worker_pool/0.
%% @end
%%--------------------------------------------------------------------
-spec main_worker_pool() -> worker_pool:name().
main_worker_pool() ->
    space_strategy:default_main_worker_pool().

%%%===================================================================
%%% API functions
%%%===================================================================

%%%===================================================================
%%% Internal functions
%%%===================================================================