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
-include("veil_modules/fslogic/fslogic.hrl").
-include_lib("eunit/include/eunit.hrl").

get_group_owner_test() ->
    [GroupName0] = fslogic_utils:get_group_owner("/" ++ ?SPACES_BASE_DIR_NAME ++ "/name0/file"),
    ?assertMatch("name0", GroupName0),

    [GroupName1] = fslogic_utils:get_group_owner("/" ++ ?SPACES_BASE_DIR_NAME ++ "/name1/"),
    ?assertMatch("name1", GroupName1),

    [GroupName2] = fslogic_utils:get_group_owner("/" ++ ?SPACES_BASE_DIR_NAME ++ "/name2"),
    ?assertMatch("name2", GroupName2),

    ?assertMatch([], fslogic_utils:get_group_owner("/dir")),
    ?assertMatch([], fslogic_utils:get_group_owner("/dir/file")),
    ?assertMatch([], fslogic_utils:get_group_owner("/dir/file/")).


-endif.