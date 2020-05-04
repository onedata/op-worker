%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains tests of space_unsupport procedure
%%% @end
%%%-------------------------------------------------------------------
-module(dummy_test_SUITE).
-author("Michal Stanisz").

-include("global_definitions.hrl").
-include("proto/oneclient/common_messages.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([all/0]).
-export([init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).
-export([foo_test/1]).

all() -> [
    foo_test
].

%%%===================================================================
%%% API
%%%===================================================================

foo_test(Config) ->
    [P1 | _] = test_config_utils:get_providers(Config),
    [User1] = test_config_utils:get_provider_users(Config, P1),
    SessId = fun(P) -> test_config_utils:get_user_session_id_on_provider(Config, User1, P) end,
    [Worker1P1 | _] = test_config_utils:get_provider_nodes(Config, P1),
    
    [SpaceId | _] = test_config_utils:get_provider_spaces(Config, P1),
    SpaceGuid = rpc:call(Worker1P1, fslogic_uuid, spaceid_to_space_dir_guid, [SpaceId]),
    
    File = generator:gen_name(),
    {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(Worker1P1, SessId(P1), SpaceGuid, File, 8#755)),
    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(Worker1P1, SessId(P1), {guid, FileGuid}, rdwr)),
    {ok, _} = ?assertMatch({ok, _}, lfm_proxy:write(Worker1P1, Handle, 0, <<"sadsadsa">>)),
    ?assertEqual(ok, lfm_proxy:close(Worker1P1, Handle)),
    ok.

init_per_suite(Config) ->
    Config1 = test_config_utils:add_envs(Config, op_worker, op_worker, [{dupa, osiem}]),
    Config2 = test_config_utils:add_envs(Config1, op_worker, cluster_worker, [{ble, xd}]),
    Config3 = test_config_utils:add_envs(Config2, cluster_manager, cluster_manager, [{lol, sadsad}]),
    test_config_utils:set_onenv_scenario(Config3, "1op").

init_per_testcase(_Case, Config) ->
    lfm_proxy:init(Config).


end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config),
    Config.

end_per_suite(_Config) ->
    ok.
