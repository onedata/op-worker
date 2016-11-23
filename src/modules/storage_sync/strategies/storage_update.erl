%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% @todo: write me!
%%% @end
%%%-------------------------------------------------------------------
-module(storage_update).
-author("Rafal Slota").

-include("modules/storage_sync/strategy_config.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/logging.hrl").


-define(DIR_BATCH, 2).

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
-export([]).



%%%===================================================================
%%% space_strategy_behaviour callbacks
%%%===================================================================


-spec available_strategies() -> [space_strategy:definition()].
available_strategies() ->
    storage_import:available_strategies().


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
strategy_init_jobs(StrategyName, StartegyArgs, InitData) ->
    ?error("Invalid import strategy init: ~p", [{StrategyName, StartegyArgs, InitData}]).


-spec strategy_handle_job(space_strategy:job()) -> {space_strategy:job_result(), [space_strategy:job()]}.
strategy_handle_job(Job) ->
    storage_import:strategy_handle_job(Job).


strategy_merge_result(Job, LocalResult, ChildrenResult) ->
    storage_import:strategy_merge_result(Job, LocalResult, ChildrenResult).

strategy_merge_result(Jobs, Results) ->
    storage_import:strategy_merge_result(Jobs, Results).

%%%===================================================================
%%% API functions
%%%===================================================================

