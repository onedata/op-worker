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

-include("modules/datastore/datastore_models.hrl").
-include("modules/storage_sync/strategy_config.hrl").
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
-export([to_storage_path/3, to_storage_logical_path/3]).

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
        #space_strategy{
            name = simple,
            result_merge_type = merge_all,
            arguments = [],
            description = <<"Simple strategy, does not modify path.">>
        },
        #space_strategy{
            name = root,
            result_merge_type = merge_all,
            arguments = [],
            description = <<
                "Strategy used when space is mounted in storage's root. "
                "Filters SpaceId from storage_path."
            >>
        }
    ].

%%--------------------------------------------------------------------
%% @doc
%% {@link space_strategy_behaviour} callback strategy_init_jobs/3.
%% @end
%%--------------------------------------------------------------------
-spec strategy_init_jobs(space_strategy:name(), space_strategy:arguments(),
    space_strategy:job_data()) -> [space_strategy:job()].
strategy_init_jobs(StrategyName, StrategyArgs, InitData) ->
    [
        #space_strategy_job{
            strategy_name = StrategyName,
            strategy_args = StrategyArgs,
            data = InitData
        }
    ].

%%--------------------------------------------------------------------
%% @doc
%% {@link space_strategy_behaviour} callback strategy_handle_job/1.
%% @end
%%--------------------------------------------------------------------
-spec strategy_handle_job(space_strategy:job()) ->
    {space_strategy:job_result(), [space_strategy:job()]}.
strategy_handle_job(#space_strategy_job{
    strategy_name = simple,
    data = #{storage_path := FilePath}
}) ->
    {FilePath, []};
strategy_handle_job(#space_strategy_job{
    strategy_name = simple,
    data = #{storage_logical_path := FilePath}
}) ->
    {FilePath, []};
strategy_handle_job(#space_strategy_job{
    strategy_name = root,
    data = #{
        storage_path := FilePath,
        space_id := SpaceId
    }
}) ->
     {add_space_id(SpaceId, FilePath), []};
strategy_handle_job(#space_strategy_job{
    strategy_name = root,
    data = #{
        storage_logical_path := FilePath,
        space_id := SpaceId
    }
}) ->
    {filter_space_id(SpaceId, FilePath), []}.


%%--------------------------------------------------------------------
%% @doc
%% {@link space_strategy_behaviour} callback strategy_merge_result/2.
%% @end
%%--------------------------------------------------------------------
-spec strategy_merge_result(ChildrenJobs :: [space_strategy:job()],
    ChildrenResults :: [space_strategy:job_result()]) ->
    space_strategy:job_result().
strategy_merge_result([#space_strategy_job{}], [Result]) ->
    Result.

%%--------------------------------------------------------------------
%% @doc
%% {@link space_strategy_behaviour} callback strategy_merge_result/3.
%% @end
%%--------------------------------------------------------------------
-spec strategy_merge_result(space_strategy:job(),
    LocalResult :: space_strategy:job_result(),
    ChildrenResult :: space_strategy:job_result()) -> space_strategy:job_result().
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

%%--------------------------------------------------------------------
%% @doc
%% Convert given logical path to storage path
%% @end
%%--------------------------------------------------------------------
-spec to_storage_path(od_space:id(), storage:id(), file_meta:path()) ->
    file_meta:path().
to_storage_path(SpaceId, StorageId, FilePath) ->
    Init = space_sync_worker:init(?MODULE, SpaceId, StorageId, #{
        storage_logical_path => FilePath,
        space_id => SpaceId,
        storage_id => StorageId
    }),
    space_sync_worker:run(Init).

%%--------------------------------------------------------------------
%% @doc
%% Convert given storage path to logical path
%% @end
%%--------------------------------------------------------------------
-spec to_storage_logical_path(od_space:id(), storage:id(), file_meta:path()) ->
    file_meta:path().
to_storage_logical_path(SpaceId, StorageId, FilePath) ->
    Init = space_sync_worker:init(?MODULE, SpaceId, StorageId, #{
        storage_path => FilePath,
        space_id => SpaceId,
        storage_id => StorageId
    }),
    space_sync_worker:run(Init).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if space is mounted in root on given storage.
%% @end
%%--------------------------------------------------------------------
-spec filter_space_id(od_space:id(), file_meta:path()) -> file_meta:path().
filter_space_id(_SpaceId, FilePath) ->
    FilePath.
%%    [Sep, SpaceId | Path] = fslogic_path:split(FilePath),
%%    fslogic_path:join([Sep | Path]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if space is mounted in root on given storage.
%% @end
%%--------------------------------------------------------------------
-spec add_space_id(od_space:id(), file_meta:path()) -> file_meta:path().
add_space_id(SpaceId, FilePath) ->
    [Sep | Path] = fslogic_path:split(FilePath),
    fslogic_path:join([Sep, SpaceId | Path]).