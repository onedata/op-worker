%% ===================================================================
%% @author Krzysztof Trzepla
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This module tests the functionality of rt_priority_queue module.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================
-module(rt_map_tests).

-ifdef(TEST).

-include("oneprovider_modules/rtransfer/rt_container.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(TEST_MAP, test_map).

%% ===================================================================
%% Tests description
%% ===================================================================

rt_priority_queue_test_() ->
    {foreach,
        fun setup/0,
        fun teardown/1,
        [
        ]
    }.

%% ===================================================================
%% Setup/teardown functions
%% ===================================================================

setup() ->
    {ok, _} = rt_map:new({local, ?TEST_MAP}, "../").

teardown(_) ->
    ok = rt_priority_queue:delete(?TEST_MAP).

%% ===================================================================
%% Tests functions
%% ===================================================================

-endif.