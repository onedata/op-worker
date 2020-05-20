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
% fixme delete this suite

%%%===================================================================
%%% API
%%%===================================================================

foo_test(Config) ->
    [P1 | _] = test_config:get_providers(Config),
    [User1] = test_config:get_provider_users(Config, P1),
    SessId = fun(P) -> test_config:get_user_session_id_on_provider(Config, User1, P) end,
    [Worker1P1 | _] = test_config:get_provider_nodes(Config, P1),
    
    [SpaceId | _] = test_config:get_provider_spaces(Config, P1),
    SpaceGuid = rpc:call(Worker1P1, fslogic_uuid, spaceid_to_space_dir_guid, [SpaceId]),
    
    File = generator:gen_name(),
    {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(Worker1P1, SessId(P1), SpaceGuid, File, 8#755)),
    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(Worker1P1, SessId(P1), {guid, FileGuid}, rdwr)),
    {ok, _} = ?assertMatch({ok, _}, lfm_proxy:write(Worker1P1, Handle, 0, <<"sadsadsa">>)),
    ?assertEqual(ok, lfm_proxy:close(Worker1P1, Handle)),
    ct:print("sleeping now"),
    timer:sleep(timer:hours(10)),
    ok.

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        onenv_test_utils:prepare_base_test_config(NewConfig)
    end,
    
    test_config:set_many(Config, [
        {add_envs, [op_worker, op_worker, [{dupa, osiem}]]},
        {add_envs, [op_worker, cluster_worker, [{ble, xd}]]},
        {add_envs, [oz_worker, cluster_worker, [{trolololo, xd}]]},
        {add_envs, [cluster_manager, cluster_manager, [{lol, sadsad}]]},
        {set_onenv_scenario, ["1op"]},
        [?ENV_UP_POSTHOOK, Posthook]
    ]).

init_per_testcase(_Case, Config) ->
    lfm_proxy:init(Config).


end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config),
    Config.

end_per_suite(_Config) ->
    ok.
