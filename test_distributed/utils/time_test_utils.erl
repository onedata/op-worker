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
    clock_freezer_mock:setup_on_nodes(?config(op_worker_nodes, Config), [global_clock]).


-spec unfreeze_time(Config :: term()) -> ok.
unfreeze_time(Config) ->
    clock_freezer_mock:teardown_on_nodes(?config(op_worker_nodes, Config)).


-spec get_frozen_time_seconds() -> time:seconds().
get_frozen_time_seconds() ->
    clock_freezer_mock:current_time_seconds().


-spec simulate_seconds_passing(time:seconds()) -> time:seconds().
simulate_seconds_passing(Seconds) ->
    clock_freezer_mock:simulate_seconds_passing(Seconds).


-spec set_current_time_seconds(time:seconds()) -> ok.
set_current_time_seconds(Seconds) ->
    clock_freezer_mock:set_current_time_millis(Seconds * 1000).
