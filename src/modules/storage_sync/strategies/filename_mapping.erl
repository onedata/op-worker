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
-module(filename_mapping).
-author("Rafal Slota").
-behavior(space_strategy_behaviour).

-include("modules/storage_sync/strategy_config.hrl").

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

%% API
-export([]).


%%%===================================================================
%%% space_strategy_behaviour callbacks
%%%===================================================================


-spec available_strategies() -> [space_strategy:definition()].
available_strategies() ->
    [
        #space_strategy{name = simple, arguments = [], description = <<"TODO">>}
    ].


-spec strategy_init_jobs(space_strategy:name(), space_strategy:arguments(), space_strategy:job_data()) ->
    [space_strategy:job()].
strategy_init_jobs(StrategyName, StartegyArgs, InitData) ->
    [
        #space_strategy_job{strategy_name = StrategyName, strategy_args = StartegyArgs, data = InitData}
    ].


-spec strategy_handle_job(space_strategy:job()) -> {space_strategy:job_result(), [space_strategy:job()]}.
strategy_handle_job(#space_strategy_job{strategy_name = simple, data = EntryPath}) ->
    {EntryPath, []}.


%%%===================================================================
%%% API functions
%%%===================================================================


%%%===================================================================
%%% Internal functions
%%%===================================================================
