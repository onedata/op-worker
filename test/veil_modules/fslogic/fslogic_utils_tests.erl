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
-include("veil_modules/dao/dao_spaces.hrl").
-include_lib("eunit/include/eunit.hrl").


get_space_info_for_path_test_() ->
    {foreach, fun setup/0, fun teardown/1, [ fun get_space_info_for_path/0 ]}.

setup() ->
    meck:new([fslogic_objects, cluster_manager_lib]).

teardown(_) ->
    meck:unload().

get_space_info_for_path() ->
    meck:expect(fslogic_objects, get_space, fun(SpaceName) -> {ok, #space_info{name = SpaceName, space_id = <<"test">>}} end),

    Resp0 = fslogic_utils:get_space_info_for_path("/" ++ ?SPACES_BASE_DIR_NAME ++ "/name0/file"),
    ?assertMatch({ok, #space_info{name = "name0", space_id = <<"test">>}}, Resp0),

    Resp1 = fslogic_utils:get_space_info_for_path("/" ++ ?SPACES_BASE_DIR_NAME ++ "/name1/"),
    ?assertMatch({ok, #space_info{name = "name1", space_id = <<"test">>}}, Resp1),

    Resp2 = fslogic_utils:get_space_info_for_path("/" ++ ?SPACES_BASE_DIR_NAME ++ "/name2"),
    ?assertMatch({ok, #space_info{name = "name2", space_id = <<"test">>}}, Resp2),

    meck:expect(cluster_manager_lib, get_provider_id, fun() -> <<"provider_id">> end),

    ?assertMatch({ok, #space_info{name = "root", providers = [<<"provider_id">>]}}, fslogic_utils:get_space_info_for_path("/dir")),
    ?assertMatch({ok, #space_info{name = "root", providers = [<<"provider_id">>]}}, fslogic_utils:get_space_info_for_path("/dir/file")),
    ?assertMatch({ok, #space_info{name = "root", providers = [<<"provider_id">>]}}, fslogic_utils:get_space_info_for_path("/dir/file/")),

    ?assert(meck:validate([fslogic_objects, cluster_manager_lib])).

-endif.