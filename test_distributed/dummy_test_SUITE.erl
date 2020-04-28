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
-export([init_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).
-export([foo_test/1]).

all() -> [
    foo_test
].

%%%===================================================================
%%% API
%%%===================================================================

foo_test(Config) ->
    [P1, _P2] = kv_utils:get(providers, Config),
    [User1] = kv_utils:get([users, P1], Config),
    SessId = fun(P) -> kv_utils:get([sess_id, P, User1], Config) end,
    [Worker1P1 | _] = kv_utils:get([provider_nodes, P1], Config),
    
    [SpaceId | _] = kv_utils:get([provider_spaces, P1], Config),
    SpaceGuid = rpc:call(Worker1P1, fslogic_uuid, spaceid_to_space_dir_guid, [SpaceId]),
    
    File = generator:gen_name(),
    {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(Worker1P1, SessId(P1), SpaceGuid, File, 8#755)),
    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(Worker1P1, SessId(P1), {guid, FileGuid}, rdwr)),
    {ok, _} = ?assertMatch({ok, _}, lfm_proxy:write(Worker1P1, Handle, 0, <<"sadsadsa">>)),
    ?assertEqual(ok, lfm_proxy:close(Worker1P1, Handle)),
    ok.

init_per_suite(Config) ->
    [{scenario, "2op-2nodes"} | Config].

init_per_testcase(_Case, Config) ->
    lfm_proxy:init(Config).


end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config),
    Config.
