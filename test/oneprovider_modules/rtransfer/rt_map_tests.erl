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

-include("registered_names.hrl").
-include("oneprovider_modules/rtransfer/rt_container.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(TEST_MAP, test_map).
-define(TEST_RT_BLOCK_SIZE, 1024).

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
    application:set_env(?APP_Name, rt_nif_prefix, "../c_lib"),
    {ok, _} = rt_priority_queue:new({local, ?TEST_MAP}, ?TEST_RT_BLOCK_SIZE).

teardown(_) ->
    ok = rt_priority_queue:delete(?TEST_MAP),
    application:set_env(?APP_Name, rt_nif_prefix, "c_lib").

%% ===================================================================
%% Tests functions
%% ===================================================================

-endif.