%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Strategy for storage import.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_import).
-author("Rafal Slota").

-include("modules/storage_sync/strategy_config.hrl").
-include("global_definitions.hrl").
-include("modules/storage_sync/storage_sync.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/logging.hrl").


%%%===================================================================
%%% Types
%%%===================================================================
-type state() :: not_started | in_progress | finished.
%%%===================================================================
%%% Exports
%%%===================================================================

%% Types
-export_type([state/0]).

%% space_strategy_behaviour callbacks
-export([available_strategies/0, strategy_init_jobs/3, strategy_handle_job/1,
    main_worker_pool/0, strategy_merge_result/2, strategy_merge_result/3,
    worker_pools_config/0
]).

%% API
-export([start/6]).

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
            name = simple_scan,
            result_merge_type = return_none,
            arguments = [
                #space_strategy_argument{
                    name = max_depth,
                    type = integer,
                    description = <<"Max depth of file tree that will be scanned">>
                },
                #space_strategy_argument{
                    name = sync_acl,
                    type = boolean,
                    description = <<"Enables synchronization of NFSv4 ACLs">>
                }
            ],
            description = <<"Simple full filesystem scan">>
        },
        #space_strategy{
            name = no_import,
            arguments = [],
            description = <<"Don't perform any storage import">>
        }
    ].

%%--------------------------------------------------------------------
%% @doc
%% {@link space_strategy_behaviour} callback strategy_init_jobs/3.
%% @end
%%--------------------------------------------------------------------
-spec strategy_init_jobs(space_strategy:name(), space_strategy:arguments(),
    space_strategy:job_data()) -> [space_strategy:job()].
strategy_init_jobs(no_import, _, _) ->
    [];
strategy_init_jobs(_, _, #{import_start_time := ImportStartTime})
    when is_integer(ImportStartTime) -> [];
strategy_init_jobs(simple_scan, Args = #{
    max_depth := MaxDepth
},
    Data = #{
        import_start_time := undefined,
        space_id := SpaceId,
        storage_id := StorageId
}) ->
    CurrentTimestamp = time_utils:cluster_time_seconds(),
    storage_sync_monitoring:reset_sync_counters(SpaceId),
    space_strategies:update_import_start_time(SpaceId, StorageId,CurrentTimestamp),
    storage_sync_monitoring:update_queue_length_spirals(SpaceId, 1),
    storage_sync_monitoring:update_files_to_sync_counter(SpaceId, 1),
    ?debug("Starting storage_import for space: ~p at time ~p", [SpaceId, CurrentTimestamp]),
    [#space_strategy_job{
        strategy_name = simple_scan,
        strategy_args = Args,
        data = Data#{max_depth => MaxDepth}
    }];
strategy_init_jobs(StrategyName, StrategyArgs, InitData) ->
    ?error("Invalid import strategy init: ~p", [{StrategyName, StrategyArgs, InitData}]).

%%--------------------------------------------------------------------
%% @doc
%% {@link space_strategy_behaviour} callback strategy_handle_job/1.
%% @end
%%--------------------------------------------------------------------
-spec strategy_handle_job(space_strategy:job()) ->
    {space_strategy:job_result(), [space_strategy:job()]}.
strategy_handle_job(Job = #space_strategy_job{strategy_name = simple_scan}) ->
    ok = datastore_throttling:throttle(import),
    simple_scan:run(Job);
strategy_handle_job(#space_strategy_job{strategy_name = no_import}) ->
    {ok, []}.

%%--------------------------------------------------------------------
%% @doc
%% {@link space_strategy_behaviour} callback strategy_merge_result/2.
%% @end
%%--------------------------------------------------------------------
-spec strategy_merge_result(ChildrenJobs :: [space_strategy:job()],
    ChildrenResults :: [space_strategy:job_result()]) ->
    space_strategy:job_result().
strategy_merge_result(_Jobs, Results) ->
    Reasons = [Reason || {error, Reason} <- Results],
    case Reasons of
        [] -> ok;
        _ ->
            {error, Reasons}
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link space_strategy_behaviour} callback strategy_merge_result/3.
%% @end
%%--------------------------------------------------------------------
-spec strategy_merge_result(space_strategy:job(),
    LocalResult :: space_strategy:job_result(),
    ChildrenResult :: space_strategy:job_result()) ->
    space_strategy:job_result().
strategy_merge_result(#space_strategy_job{strategy_name = no_import}, ok, ok) ->
    ok;
strategy_merge_result(#space_strategy_job{
    data = #{
        space_id := SpaceId,
        storage_id := StorageId
}}, ok, ok) ->
    update_import_finish_time_if_import_is_finished(SpaceId, StorageId);
strategy_merge_result(#space_strategy_job{
    data = #{
        space_id := SpaceId,
        storage_id := StorageId
}}, Error, ok) ->
    update_import_finish_time_if_import_is_finished(SpaceId, StorageId),
    Error;
strategy_merge_result(#space_strategy_job{
    data = #{
        space_id := SpaceId,
        storage_id := StorageId
}}, ok, Error) ->
    update_import_finish_time_if_import_is_finished(SpaceId, StorageId),
    Error;
strategy_merge_result(#space_strategy_job{
    data = #{
        space_id := SpaceId,
        storage_id := StorageId
}}, {error, Reason1}, {error, Reason2}) ->
    update_import_finish_time_if_import_is_finished(SpaceId, StorageId),
    {error, [Reason1, Reason2]}.

%%--------------------------------------------------------------------
%% @doc
%% {@link space_strategy_behaviour} callback worker_pools_config/0.
%% @end
%%--------------------------------------------------------------------
-spec worker_pools_config() -> [{worker_pool:name(), non_neg_integer()}].
worker_pools_config() -> [
    {?STORAGE_SYNC_DIR_POOL_NAME, ?STORAGE_SYNC_DIR_WORKERS_NUM},
    {?STORAGE_SYNC_FILE_POOL_NAME, ?STORAGE_SYNC_FILE_WORKERS_NUM}
].

%%--------------------------------------------------------------------
%% @doc
%% {@link space_strategy_behaviour} callback main_worker_pool/0.
%% @end
%%--------------------------------------------------------------------
-spec main_worker_pool() -> worker_pool:name().
main_worker_pool() ->
    ?STORAGE_SYNC_DIR_POOL_NAME.

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Function responsible for starting storage import.
%% @end
%%--------------------------------------------------------------------
-spec start(od_space:id(), storage:id(), space_strategy:timestamp(),
    space_strategy:timestamp(), file_ctx:ctx(), file_meta:path()) ->
    [space_strategy:job_result()] | space_strategy:job_result().
start(SpaceId, StorageId, ImportStartTime, ImportFinishTime, ParentCtx, FileName) ->
    InitialImportJobData = #{
        import_start_time => ImportStartTime,
        import_finish_time => ImportFinishTime,
        space_id => SpaceId,
        storage_id => StorageId,
        file_name => FileName,
        parent_ctx => ParentCtx
    },
    ImportInit = space_sync_worker:init(?MODULE, SpaceId, StorageId, InitialImportJobData),
    space_sync_worker:run(ImportInit).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Checks if import has finished. If true, updates import_finish_time.
%% @end
%%-------------------------------------------------------------------
-spec update_import_finish_time_if_import_is_finished(od_space:id(), storage:id()) -> ok.
update_import_finish_time_if_import_is_finished(SpaceId, StorageId) ->
    case storage_sync_monitoring:get_unhandled_files_value(SpaceId) of
        0 ->
            {ok, _} = space_strategies:update_import_finish_time(SpaceId,
                StorageId, time_utils:cluster_time_seconds()),
            ok;
        _ ->
            ok
    end.
