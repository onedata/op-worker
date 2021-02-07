%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions manipulating time used in ct tests.
%%% @end
%%%-------------------------------------------------------------------
-module(time_test_utils).
-author("Bartosz Walkowicz").

-include_lib("ctool/include/test/test_utils.hrl").


-export([
    freeze_time/1, unfreeze_time/1,
    get_frozen_time_seconds/0,
    get_frozen_time_hours/0,
    simulate_seconds_passing/1,
    set_current_time_seconds/1
]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Stops the clock at one value and allows to manually simulate time passing.
%% @end
%%--------------------------------------------------------------------
-spec freeze_time(Config :: term()) -> ok.
freeze_time(Config) ->
    Nodes = ?config(oz_worker_nodes, Config) ++ ?config(op_worker_nodes, Config),
    clock_freezer_mock:setup_on_nodes(Nodes, [global_clock]).


-spec unfreeze_time(Config :: term()) -> ok.
unfreeze_time(Config) ->
    Nodes = ?config(oz_worker_nodes, Config) ++ ?config(op_worker_nodes, Config),
    clock_freezer_mock:teardown_on_nodes(Nodes).


-spec get_frozen_time_seconds() -> time:seconds().
get_frozen_time_seconds() ->
    clock_freezer_mock:current_time_seconds().


-spec get_frozen_time_hours() -> time:hours().
get_frozen_time_hours() ->
    clock_freezer_mock:current_time_hours().


-spec simulate_seconds_passing(time:seconds()) -> time:seconds().
simulate_seconds_passing(Seconds) ->
    clock_freezer_mock:simulate_seconds_passing(Seconds).


-spec set_current_time_seconds(time:seconds()) -> ok.
set_current_time_seconds(Seconds) ->
    clock_freezer_mock:set_current_time_millis(Seconds * 1000).
