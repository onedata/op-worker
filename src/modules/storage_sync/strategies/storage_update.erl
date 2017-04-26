%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Strategy for updating storage.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_update).
-author("Rafal Slota").

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

%% Callbacks
-export([available_strategies/0, strategy_init_jobs/3, strategy_handle_job/1]).
-export([strategy_merge_result/2, strategy_merge_result/3]).

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
    storage_import:available_strategies().

%%--------------------------------------------------------------------
%% @doc
%% {@link space_strategy_behaviour} callback strategy_init_jobs/3.
%% @end
%%--------------------------------------------------------------------
-spec strategy_init_jobs(space_strategy:name(), space_strategy:arguments(), space_strategy:job_data()) ->
    [space_strategy:job()].
strategy_init_jobs(no_import, _, _) ->
    [];
strategy_init_jobs(_, _, #{last_import_time := undefined}) ->
    [];
strategy_init_jobs(bfs_scan, #{scan_interval := ScanIntervalSeconds} = Args,
    #{last_import_time := LastImportTime} = Data) ->
    case LastImportTime + timer:seconds(ScanIntervalSeconds) < os:system_time(milli_seconds) of
        true ->
            [#space_strategy_job{strategy_name = bfs_scan, strategy_args = Args, data = Data}];
        false ->
            []
    end;
strategy_init_jobs(StrategyName, StrategyArgs, InitData) ->
    ?error("Invalid import strategy init: ~p", [{StrategyName, StrategyArgs, InitData}]).

%%--------------------------------------------------------------------
%% @doc
%% {@link space_strategy_behaviour} callback strategy_handle_job/1.
%% @end
%%--------------------------------------------------------------------
-spec strategy_handle_job(space_strategy:job()) -> {space_strategy:job_result(), [space_strategy:job()]}.
strategy_handle_job(Job) ->
    storage_import:strategy_handle_job(Job).

%%--------------------------------------------------------------------
%% @doc
%% {@link space_strategy_behaviour} callback strategy_merge_result/2.
%% @end
%%--------------------------------------------------------------------
-spec strategy_merge_result(ChildrenJobs :: [space_strategy:job()],
    ChildrenResults :: [space_strategy:job_result()]) ->
    space_strategy:job_result().
strategy_merge_result(Jobs, Results) ->
    storage_import:strategy_merge_result(Jobs, Results).

%%--------------------------------------------------------------------
%% @doc
%% {@link space_strategy_behaviour} callback strategy_merge_result/3.
%% @end
%%--------------------------------------------------------------------
-spec strategy_merge_result(space_strategy:job(), LocalResult :: space_strategy:job_result(),
    ChildrenResult :: space_strategy:job_result()) ->
    space_strategy:job_result().
strategy_merge_result(Job, LocalResult, ChildrenResult) ->
    storage_import:strategy_merge_result(Job, LocalResult, ChildrenResult).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Function responsible for starting storage update.
%% @end
%%--------------------------------------------------------------------
-spec start(od_space:id(), storage:id(), integer() | undefined, file_ctx:ctx(),
    file_meta:path(), non_neg_integer()) ->
    [space_strategy:job_result()] | space_strategy:job_result().
start(SpaceId, StorageId, LastImportTime, ParentCtx, FileName, MaxDepth) ->
    InitialImportJobData = #{
        last_import_time => LastImportTime,
        space_id => SpaceId,
        storage_id => StorageId,
        file_name => FileName,
        max_depth => MaxDepth,
        parent_ctx => ParentCtx
    },
    ImportInit = space_sync_worker:init(?MODULE, SpaceId, StorageId, InitialImportJobData),
    space_sync_worker:run(ImportInit).