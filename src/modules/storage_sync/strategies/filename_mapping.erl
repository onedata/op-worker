%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Strategy for mapping filename.
%%% @end
%%%-------------------------------------------------------------------
-module(filename_mapping).
-author("Rafal Slota").
-behavior(space_strategy_behaviour).

-include("modules/datastore/datastore_specific_models_def.hrl").
-include("modules/storage_sync/strategy_config.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").

%%%===================================================================
%%% Types
%%%===================================================================

%%%===================================================================
%%% Exports
%%%===================================================================

%% Types
-export_type([]).

%% Callbacks
-export([available_strategies/0, strategy_init_jobs/3, strategy_handle_job/1]).
-export([strategy_merge_result/2, strategy_merge_result/3]).

%% API
-export([to_storage_path/3, to_logical_path/3]).

%%%===================================================================
%%% space_strategy_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc {@link space_strategy_behaviour} callback available_strategies/0.
%%--------------------------------------------------------------------
-spec available_strategies() -> [space_strategy:definition()].
available_strategies() ->
    [
        #space_strategy{result_merge_type = merge_all, name = simple, arguments = [], description = <<"TODO">>}
    ].

%%--------------------------------------------------------------------
%% @doc {@link space_strategy_behaviour} callback strategy_init_jobs/3.
%%--------------------------------------------------------------------
-spec strategy_init_jobs(space_strategy:name(), space_strategy:arguments(), space_strategy:job_data()) ->
    [space_strategy:job()].
strategy_init_jobs(StrategyName, StartegyArgs, InitData) ->
    [
        #space_strategy_job{strategy_name = StrategyName, strategy_args = StartegyArgs, data = InitData}
    ].

%%--------------------------------------------------------------------
%% @doc {@link space_strategy_behaviour} callback strategy_handle_job/1.
%%--------------------------------------------------------------------
-spec strategy_handle_job(space_strategy:job()) -> {space_strategy:job_result(), [space_strategy:job()]}.
strategy_handle_job(#space_strategy_job{strategy_name = simple, data = #{storage_path := FilePath}}) ->
    {FilePath, []};
strategy_handle_job(#space_strategy_job{strategy_name = simple, data = #{logical_path := FilePath}}) ->
    {FilePath, []}.

%%--------------------------------------------------------------------
%% @doc {@link space_strategy_behaviour} callback strategy_merge_result/2.
%%--------------------------------------------------------------------
-spec strategy_merge_result(ChildrenJobs :: [space_strategy:job()],
    ChildrenResults :: [space_strategy:job_result()]) ->
    space_strategy:job_result().
strategy_merge_result([#space_strategy_job{strategy_name = simple}], [Result]) ->
    Result.

%%--------------------------------------------------------------------
%% @doc {@link space_strategy_behaviour} callback strategy_merge_result/3.
%%--------------------------------------------------------------------
-spec strategy_merge_result(space_strategy:job(), LocalResult :: space_strategy:job_result(),
    ChildrenResult :: space_strategy:job_result()) ->
    space_strategy:job_result().
strategy_merge_result(#space_strategy_job{strategy_name = simple}, LocalResult, _ChildrenResult) ->
    LocalResult.

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Convert given logical path to storage path
%% @end
%%--------------------------------------------------------------------
-spec to_storage_path(od_space:id(), storage:id(), file_meta:path()) -> file_meta:path().
to_storage_path(SpaceId, StorageId, FilePath) ->
    Init = space_sync_worker:init(?MODULE, SpaceId, StorageId, #{logical_path => FilePath}),
    space_sync_worker:run(Init).

%%--------------------------------------------------------------------
%% @doc
%% Convert given storage path to logical path
%% @end
%%--------------------------------------------------------------------
-spec to_logical_path(od_space:id(), storage:id(), file_meta:path()) -> file_meta:path().
to_logical_path(SpaceId, StorageId, FilePath) ->
    Init = space_sync_worker:init(?MODULE, SpaceId, StorageId, #{storage_path => FilePath}),
    space_sync_worker:run(Init).

%%%===================================================================
%%% Internal functions
%%%===================================================================