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
-module(storage_import).
-author("Rafal Slota").

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
        #space_strategy{name = bfs_scan, arguments = [
            #space_strategy_argument{name = frequency, type = integer, description =
                <<"Frequency of running the scan in secodns">>}
        ], description = <<"Simple BFS-like full filesystem scan">>},
        #space_strategy{name = no_import, arguments = [],
            description = <<"Don't perform any storage import">>
        }
    ].


-spec strategy_init_jobs(space_strategy:name(), space_strategy:arguments(), space_strategy:job_data()) ->
    [space_strategy:job()].
strategy_init_jobs(StrategyName, StartegyArgs, InitData) ->
    [
        #space_strategy_job{strategy_name = StrategyName, strategy_args = StartegyArgs, data = InitData}
    ].


-spec strategy_handle_job(space_strategy:job()) -> {space_strategy:job_result(), [space_strategy:job()]}.
strategy_handle_job(#space_strategy_job{strategy_name = no_import}) ->
    {ok, []}.


%%%===================================================================
%%% API functions
%%%===================================================================


%%%===================================================================
%%% Internal functions
%%%===================================================================

