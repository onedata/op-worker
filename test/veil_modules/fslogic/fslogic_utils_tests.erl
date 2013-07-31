%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of fslogic_utils module.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================
-module(fslogic_utils_tests).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").


strip_path_leaf_test() ->
    ?assertMatch("/some/path", fslogic_utils:strip_path_leaf("/some/path/leaf")),
    ?assertMatch("/", fslogic_utils:strip_path_leaf("/leaf")),
    ?assertMatch("/base", fslogic_utils:strip_path_leaf("/base/leaf/")).

basename_test() ->
    ?assertMatch("leaf", fslogic_utils:basename("/root/dir/leaf")),
    ?assertMatch("leaf", fslogic_utils:basename("leaf")).

-endif.