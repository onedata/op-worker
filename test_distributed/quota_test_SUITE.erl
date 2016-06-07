%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Quota tests
%%% @end
%%%-------------------------------------------------------------------
-module(quota_test_SUITE).
-author("Rafal Slota").

-include("global_definitions.hrl").
-include("http/rest/cdmi/cdmi_capabilities.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/posix/errors.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

-export([
    write_bigger_then_quota_should_fail/1,
    write_smaller_then_quota_should_not_fail/1,
    truncate_bigger_then_quota_should_fail/1,
    truncate_smaller_then_quota_should_not_fail/1,
    incremental_write_bigger_then_quota_should_fail/1,
    incremental_write_smaller_then_quota_should_not_fail/1,
    unlink_should_unlock_space/1,
    rename_should_unlock_space/1,
    rename_bigger_then_quota_should_fail/1,
    soft_quota_should_allow_bigger_writes/1

]).

all() ->
    ?ALL([
        write_bigger_then_quota_should_fail,
        write_smaller_then_quota_should_not_fail,
        truncate_bigger_then_quota_should_fail,
        truncate_smaller_then_quota_should_not_fail,
        incremental_write_bigger_then_quota_should_fail,
        incremental_write_smaller_then_quota_should_not_fail,
        unlink_should_unlock_space,
        rename_should_unlock_space,
        rename_bigger_then_quota_should_fail,
        soft_quota_should_allow_bigger_writes
    ]).

-define(TIMEOUT, timer:seconds(5)).
-define(DEFAULT_FILE_MODE, 8#664).
-define(FILE_BEGINNING, 0).
-define(INFINITY, 9999).

-record(env, {
    p1, p2, user1, user2, file1, file2, file3, dir1
}).

%% Spaces support:
%%  p1 -> space_id0: 20 bytes
%%  p1 -> space_id1: 30 bytes
%%  p2 -> space_id2: 50 bytes
%%  p1 -> space_id3: 44 bytes
%%  p2 -> space_id3: 22 bytes

%%%===================================================================
%%% Test functions
%%%===================================================================


write_bigger_then_quota_should_fail(Config) ->
    #env{p1 = P1, p2 = _P2, user1 = User1, user2 = User2, file1 = File1, file2 = File2} =
        gen_test_env(Config),

    {ok, _} = create_file(P1, User1, f(<<"space1">>, File1)),
    {ok, _} = create_file(P1, User2, f(<<"space1">>, File2)),
    {ok, _} = create_file(P1, User1, f(<<"space2">>, File1)),
    {ok, _} = create_file(P1, User2, f(<<"space2">>, File2)),

    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space1">>, File1), 0, crypto:rand_bytes(31))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space1">>, File1), 0, crypto:rand_bytes(38))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space1">>, File1), 0, crypto:rand_bytes(3131))),

    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space1">>, File2), 0, crypto:rand_bytes(31))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space1">>, File2), 0, crypto:rand_bytes(38))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space1">>, File2), 0, crypto:rand_bytes(3131))),

    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space2">>, File1), 0, crypto:rand_bytes(51))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space2">>, File1), 0, crypto:rand_bytes(58))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space2">>, File1), 0, crypto:rand_bytes(3131))),

    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space2">>, File2), 0, crypto:rand_bytes(51))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space2">>, File2), 0, crypto:rand_bytes(58))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space2">>, File2), 0, crypto:rand_bytes(3131))),

    ok.

write_smaller_then_quota_should_not_fail(Config) ->
    #env{p1 = P1, p2 = P2, user1 = User1, user2 = User2, file1 = File1, file2 = File2} =
        gen_test_env(Config),

    {ok, _} = create_file(P1, User1, f(<<"space1">>, File1)),
    {ok, _} = create_file(P1, User1, f(<<"space2">>, File1)),

    ?assertMatch({ok, _}, write_to_file(P1, User1, f(<<"space1">>, File1), 0, crypto:rand_bytes(29))),
    ?assertMatch({ok, _}, write_to_file(P1, User1, f(<<"space1">>, File1), 0, crypto:rand_bytes(5))),
    ?assertMatch({ok, _}, write_to_file(P1, User1, f(<<"space1">>, File1), 0, crypto:rand_bytes(30))),

    ?assertMatch({ok, _}, write_to_file(P1, User2, f(<<"space1">>, File1), 0, crypto:rand_bytes(29))),
    ?assertMatch({ok, _}, write_to_file(P1, User2, f(<<"space1">>, File1), 0, crypto:rand_bytes(5))),
    ?assertMatch({ok, _}, write_to_file(P1, User2, f(<<"space1">>, File1), 0, crypto:rand_bytes(30))),

    ?assertMatch({ok, _}, write_to_file(P1, User1, f(<<"space2">>, File1), 0, crypto:rand_bytes(49))),
    ?assertMatch({ok, _}, write_to_file(P1, User1, f(<<"space2">>, File1), 0, crypto:rand_bytes(4))),
    ?assertMatch({ok, _}, write_to_file(P1, User1, f(<<"space2">>, File1), 0, crypto:rand_bytes(50))),

    ?assertMatch({ok, _}, write_to_file(P1, User2, f(<<"space2">>, File1), 0, crypto:rand_bytes(49))),
    ?assertMatch({ok, _}, write_to_file(P1, User2, f(<<"space2">>, File1), 0, crypto:rand_bytes(4))),
    ?assertMatch({ok, _}, write_to_file(P1, User2, f(<<"space2">>, File1), 0, crypto:rand_bytes(50))),

    ok.

truncate_bigger_then_quota_should_fail(Config) ->
    #env{p1 = P1, p2 = P2, user1 = User1, user2 = User2, file1 = File1, file2 = File2} =
        gen_test_env(Config),

    {ok, _} = create_file(P1, User1, f(<<"space1">>, File1)),
    {ok, _} = create_file(P1, User2, f(<<"space1">>, File2)),
    {ok, _} = create_file(P1, User1, f(<<"space2">>, File1)),
    {ok, _} = create_file(P1, User2, f(<<"space2">>, File2)),

    ?assertMatch({error, ?ENOSPC}, truncate(P1, User1, f(<<"space1">>, File1), 31)),
    ?assertMatch({error, ?ENOSPC}, truncate(P1, User1, f(<<"space1">>, File1), 38)),
    ?assertMatch({error, ?ENOSPC}, truncate(P1, User1, f(<<"space1">>, File1), 3131)),
    ?assertMatch({error, ?ENOSPC}, truncate(P1, User2, f(<<"space1">>, File2), 31)),
    ?assertMatch({error, ?ENOSPC}, truncate(P1, User2, f(<<"space1">>, File2), 38)),
    ?assertMatch({error, ?ENOSPC}, truncate(P1, User2, f(<<"space1">>, File2), 3131)),
    ?assertMatch({error, ?ENOSPC}, truncate(P1, User1, f(<<"space2">>, File1), 51)),
    ?assertMatch({error, ?ENOSPC}, truncate(P1, User1, f(<<"space2">>, File1), 58)),
    ?assertMatch({error, ?ENOSPC}, truncate(P1, User1, f(<<"space2">>, File1), 3131)),
    ?assertMatch({error, ?ENOSPC}, truncate(P1, User2, f(<<"space2">>, File2), 51)),
    ?assertMatch({error, ?ENOSPC}, truncate(P1, User2, f(<<"space2">>, File2), 58)),
    ?assertMatch({error, ?ENOSPC}, truncate(P1, User2, f(<<"space2">>, File2), 3131)),

    ok.

truncate_smaller_then_quota_should_not_fail(Config) ->
    #env{p1 = P1, p2 = _P2, user1 = User1, user2 = User2, file1 = File1, file2 = File2} =
        gen_test_env(Config),

    {ok, _} = create_file(P1, User1, f(<<"space1">>, File1)),
    {ok, _} = create_file(P1, User2, f(<<"space1">>, File2)),
    {ok, _} = create_file(P1, User1, f(<<"space2">>, File1)),
    {ok, _} = create_file(P1, User2, f(<<"space2">>, File2)),

    ?assertMatch(ok, truncate(P1, User1, f(<<"space1">>, File1), 30)),
    ?assertMatch(ok, truncate(P1, User1, f(<<"space1">>, File1), 19)),
    ?assertMatch(ok, truncate(P1, User1, f(<<"space1">>, File1), 5)),
    ?assertMatch(ok, truncate(P1, User2, f(<<"space1">>, File2), 10)),
    ?assertMatch(ok, truncate(P1, User2, f(<<"space1">>, File2), 15)),
    ?assertMatch(ok, truncate(P1, User2, f(<<"space1">>, File2), 25)),
    ?assertMatch(ok, truncate(P1, User1, f(<<"space2">>, File1), 50)),
    ?assertMatch(ok, truncate(P1, User1, f(<<"space2">>, File1), 35)),
    ?assertMatch(ok, truncate(P1, User1, f(<<"space2">>, File1), 5)),
    ?assertMatch(ok, truncate(P1, User2, f(<<"space2">>, File1), 10)),
    ?assertMatch(ok, truncate(P1, User2, f(<<"space2">>, File1), 35)),
    ?assertMatch(ok, truncate(P1, User2, f(<<"space2">>, File1), 45)),

    ok.

incremental_write_bigger_then_quota_should_fail(Config) ->
    #env{p1 = P1, p2 = P2, user1 = User1, user2 = User2, file1 = File1, file2 = File2} =
        gen_test_env(Config),

    {ok, _} = create_file(P1, User1, f(<<"space1">>, File1)),
    {ok, _} = create_file(P1, User2, f(<<"space1">>, File2)),
    {ok, _} = create_file(P1, User1, f(<<"space2">>, File1)),
    {ok, _} = create_file(P1, User2, f(<<"space2">>, File2)),

    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space1">>, File1), 0,  crypto:rand_bytes(5))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space1">>, File1), 2,  crypto:rand_bytes(20))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space1">>, File1), 19, crypto:rand_bytes(12))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space1">>, File1), 29, crypto:rand_bytes(5))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space1">>, File2), 0,  crypto:rand_bytes(9))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space1">>, File2), 0,  crypto:rand_bytes(134))),

    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space1">>, File1), 0,  crypto:rand_bytes(5))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space1">>, File1), 2,  crypto:rand_bytes(20))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space1">>, File1), 19, crypto:rand_bytes(12))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space1">>, File1), 29, crypto:rand_bytes(5))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space1">>, File2), 0,  crypto:rand_bytes(9))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space1">>, File2), 0,  crypto:rand_bytes(134))),

    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space2">>, File1), 0,  crypto:rand_bytes(17))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space2">>, File1), 12, crypto:rand_bytes(31))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space2">>, File1), 19, crypto:rand_bytes(32))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space2">>, File1), 49, crypto:rand_bytes(5))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space2">>, File2), 0,  crypto:rand_bytes(9))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space2">>, File2), 0,  crypto:rand_bytes(134))
    ),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space2">>, File1), 0,  crypto:rand_bytes(17))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space2">>, File1), 12, crypto:rand_bytes(31))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space2">>, File1), 19, crypto:rand_bytes(32))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space2">>, File1), 49, crypto:rand_bytes(5))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space2">>, File2), 0,  crypto:rand_bytes(9))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space2">>, File2), 0,  crypto:rand_bytes(134))),

    ok.

incremental_write_smaller_then_quota_should_not_fail(Config) ->
    #env{p1 = P1, p2 = _P2, user1 = User1, user2 = User2, file1 = File1, file2 = File2} =
        gen_test_env(Config),

    {ok, _} = create_file(P1, User1, f(<<"space1">>, File1)),
    {ok, _} = create_file(P1, User2, f(<<"space1">>, File2)),
    {ok, _} = create_file(P1, User1, f(<<"space2">>, File1)),
    {ok, _} = create_file(P1, User2, f(<<"space2">>, File2)),

    ?assertMatch({ok, _}, write_to_file(P1, User1, f(<<"space1">>, File1), 0,  crypto:rand_bytes(5))),
    ?assertMatch({ok, _}, write_to_file(P1, User1, f(<<"space1">>, File1), 2,  crypto:rand_bytes(20))),
    ?assertMatch({ok, _}, write_to_file(P1, User1, f(<<"space1">>, File1), 19, crypto:rand_bytes(1))),
    ?assertMatch({ok, _}, write_to_file(P1, User2, f(<<"space1">>, File2), 0,  crypto:rand_bytes(7))),
    ?assertMatch({ok, _}, write_to_file(P1, User2, f(<<"space1">>, File2), 7,  crypto:rand_bytes(1))),

    ?assertMatch({ok, _}, write_to_file(P1, User1, f(<<"space1">>, File1), 0,  crypto:rand_bytes(5))),
    ?assertMatch({ok, _}, write_to_file(P1, User1, f(<<"space1">>, File1), 2,  crypto:rand_bytes(20))),
    ?assertMatch({ok, _}, write_to_file(P1, User1, f(<<"space1">>, File1), 19, crypto:rand_bytes(1))),
    ?assertMatch({ok, _}, write_to_file(P1, User2, f(<<"space1">>, File2), 0,  crypto:rand_bytes(7))),
    ?assertMatch({ok, _}, write_to_file(P1, User2, f(<<"space1">>, File2), 7,  crypto:rand_bytes(1))),

    ?assertMatch({ok, _}, write_to_file(P1, User1, f(<<"space2">>, File1), 0,  crypto:rand_bytes(17))),
    ?assertMatch({ok, _}, write_to_file(P1, User1, f(<<"space2">>, File1), 12, crypto:rand_bytes(21))),
    ?assertMatch({ok, _}, write_to_file(P1, User1, f(<<"space2">>, File1), 19, crypto:rand_bytes(11))),
    ?assertMatch({ok, _}, write_to_file(P1, User2, f(<<"space2">>, File2), 0,  crypto:rand_bytes(7))),
    ?assertMatch({ok, _}, write_to_file(P1, User2, f(<<"space2">>, File2), 7,  crypto:rand_bytes(10))
    ),
    ?assertMatch({ok, _}, write_to_file(P1, User1, f(<<"space2">>, File1), 0,  crypto:rand_bytes(17))),
    ?assertMatch({ok, _}, write_to_file(P1, User1, f(<<"space2">>, File1), 12, crypto:rand_bytes(21))),
    ?assertMatch({ok, _}, write_to_file(P1, User1, f(<<"space2">>, File1), 19, crypto:rand_bytes(11))),
    ?assertMatch({ok, _}, write_to_file(P1, User2, f(<<"space2">>, File2), 0,  crypto:rand_bytes(7))),
    ?assertMatch({ok, _}, write_to_file(P1, User2, f(<<"space2">>, File2), 7,  crypto:rand_bytes(10))),

    ok.

unlink_should_unlock_space(Config) ->
    #env{p1 = P1, p2 = P2, user1 = User1, user2 = User2, file1 = File1, file2 = File2, file3 = File3} =
        gen_test_env(Config),

    {ok, _} = create_file(P1, User1, f(<<"space1">>, File1)),
    {ok, _} = create_file(P1, User1, f(<<"space1">>, File2)),
    {ok, _} = create_file(P1, User2, f(<<"space1">>, File3)),
    {ok, _} = create_file(P1, User1, f(<<"space2">>, File1)),
    {ok, _} = create_file(P1, User1, f(<<"space2">>, File2)),
    {ok, _} = create_file(P1, User2, f(<<"space2">>, File3)),

    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space1">>, File1), 0, crypto:rand_bytes(16))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space1">>, File2), 0, crypto:rand_bytes(12))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space1">>, File3), 0, crypto:rand_bytes(3))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space1">>, File3), 0, crypto:rand_bytes(18))),
    ?assertMatch(ok, unlink(P1, User1,                      f(<<"space1">>, File2))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space1">>, File3), 0, crypto:rand_bytes(3))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space1">>, File3), 0, crypto:rand_bytes(18))),
    ?assertMatch(ok, unlink(P1, User1,                      f(<<"space1">>, File1))),
    ?assertMatch({ok, _}, write_to_file(P1, User2,          f(<<"space1">>, File3), 0, crypto:rand_bytes(18))),

    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space2">>, File1), 0, crypto:rand_bytes(26))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space2">>, File2), 0, crypto:rand_bytes(22))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space2">>, File3), 0, crypto:rand_bytes(3))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space2">>, File3), 0, crypto:rand_bytes(28))),
    ?assertMatch(ok, unlink(P1, User1,                      f(<<"space2">>, File2))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space2">>, File3), 0, crypto:rand_bytes(3))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space2">>, File3), 0, crypto:rand_bytes(28))),
    ?assertMatch(ok, unlink(P1, User1,                      f(<<"space2">>, File1))),
    ?assertMatch({ok, _}, write_to_file(P1, User2,          f(<<"space2">>, File3), 0, crypto:rand_bytes(28))),

    ok.

rename_should_unlock_space(Config) ->
    #env{p1 = P1, p2 = P2, user1 = User1, user2 = User2, file1 = File1, file2 = File2, file3 = File3, dir1 = Dir1} =
        gen_test_env(Config),

    {ok, _} = create_file(P1, User1, f(<<"space1">>, File1)),
    {ok, _} = mkdir(P1, User1,       f(<<"space1">>, Dir1)),
    {ok, _} = create_file(P1, User1, f(<<"space1">>, [Dir1], File1)),
    {ok, _} = create_file(P1, User1, f(<<"space1">>, File2)),
    {ok, _} = create_file(P1, User2, f(<<"space1">>, File3)),
    {ok, _} = create_file(P1, User1, f(<<"space2">>, File1)),
    {ok, _} = mkdir(P1, User1,       f(<<"space2">>, Dir1)),
    {ok, _} = create_file(P1, User1, f(<<"space2">>, [Dir1], File1)),
    {ok, _} = create_file(P1, User1, f(<<"space2">>, File2)),
    {ok, _} = create_file(P1, User2, f(<<"space2">>, File3)),

    %% ### Space1 ###
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space1">>, File1), 0, crypto:rand_bytes(16))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space1">>, File2), 0, crypto:rand_bytes(12))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space1">>, File3), 0, crypto:rand_bytes(3))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space1">>, File3), 0, crypto:rand_bytes(18))),
    ?assertMatch(ok, rename(P1, User1,                      f(<<"space1">>, File2), f(<<"space0">>, File2))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space1">>, File3), 0, crypto:rand_bytes(3))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space1">>, File3), 0, crypto:rand_bytes(18))),
    ?assertMatch(ok, unlink(P1, User1,                      f(<<"space0">>, File2))),
    ?assertMatch(ok, rename(P1, User1,                      f(<<"space1">>, File1), f(<<"space0">>, File1))),

    %% Cleanup only
    ?assertMatch(ok, unlink(P1, User1,                      f(<<"space0">>, File1))),

    ?assertMatch({ok, _}, write_to_file(P1, User2,          f(<<"space1">>, [Dir1], File1), 0, crypto:rand_bytes(17))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space1">>, File3), 3, crypto:rand_bytes(11))),
    ?assertMatch(ok, rename(P1, User1,                      f(<<"space1">>, Dir1), f(<<"space0">>, Dir1))),
    ?assertMatch({ok, _}, write_to_file(P1, User2,          f(<<"space1">>, File3), 3, crypto:rand_bytes(11))),
    ?assertMatch({ok, _}, write_to_file(P1, User2,          f(<<"space1">>, File3), 3, crypto:rand_bytes(17))),

    %% Cleanup only
    ?assertMatch(ok, unlink(P1, User1,                      f(<<"space0">>, [Dir1], File1))),
    ?assertMatch(ok, unlink(P1, User1,                      f(<<"space0">>, Dir1))),


    %% ### Space2 ###
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space2">>, File1), 0, crypto:rand_bytes(26))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space2">>, File2), 0, crypto:rand_bytes(18))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space2">>, File3), 0, crypto:rand_bytes(7))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space2">>, File3), 0, crypto:rand_bytes(28))),
    ?assertMatch(ok, rename(P1, User1,                      f(<<"space2">>, File2), f(<<"space0">>, File2))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space2">>, File3), 0, crypto:rand_bytes(7))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space2">>, File3), 0, crypto:rand_bytes(28))),

    %% Cleanup only
    ?assertMatch(ok, unlink(P1, User1,                      f(<<"space0">>, File2))),
    ?assertMatch(ok, unlink(P1, User1,                      f(<<"space2">>, File1))),

    ?assertMatch({ok, _}, write_to_file(P1, User2,          f(<<"space2">>, [Dir1], File1), 0, crypto:rand_bytes(17))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space2">>, File3), 7, crypto:rand_bytes(27))),
    ?assertMatch(ok, rename(P1, User1,                      f(<<"space2">>, Dir1), f(<<"space0">>, Dir1))),
    ?assertMatch({ok, _}, write_to_file(P1, User2,          f(<<"space2">>, File3), 7, crypto:rand_bytes(27))),
    ?assertMatch({ok, _}, write_to_file(P1, User2,          f(<<"space2">>, File3), 7, crypto:rand_bytes(37))),

    ok.


rename_bigger_then_quota_should_fail(Config) ->
    #env{p1 = P1, p2 = _P2, user1 = User1, user2 = User2, file1 = File1, file2 = File2, file3 = File3} =
        gen_test_env(Config),

    {ok, _} = create_file(P1, User1,    f(<<"space1">>, File1)),
    {ok, _} = create_file(P1, User1,    f(<<"space1">>, File2)),
    {ok, _} = mkdir(P1, User1,          f(<<"space1">>, File3)),
    {ok, _} = mkdir(P1, User1,          f(<<"space1">>, [File3], File3)),
    {ok, _} = create_file(P1, User1,    f(<<"space1">>, [File3], File2)),
    {ok, _} = create_file(P1, User1,    f(<<"space1">>, [File3, File3], File2)),

    {ok, _} = create_file(P1, User1,    f(<<"space2">>, File1)),
    {ok, _} = create_file(P1, User1,    f(<<"space2">>, File2)),
    {ok, _} = mkdir(P1, User1,          f(<<"space2">>, File3)),
    {ok, _} = mkdir(P1, User1,          f(<<"space2">>, [File3], File3)),
    {ok, _} = create_file(P1, User1,    f(<<"space2">>, [File3], File2)),
    {ok, _} = create_file(P1, User1,    f(<<"space2">>, [File3, File3], File2)),


    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space1">>, File1), 0, crypto:rand_bytes(16))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space1">>, File2), 0, crypto:rand_bytes(12))),
    ?assertMatch(ok, rename(P1, User1,                      f(<<"space1">>, File2), f(<<"space0">>, File2))),
    ?assertMatch({error, ?ENOSPC}, rename(P1, User1,        f(<<"space1">>, File1), f(<<"space0">>, File1))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space1">>, [File3, File3], File2), 0, crypto:rand_bytes(8))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space1">>, [File3], File2), 0, crypto:rand_bytes(2))),
    ?assertMatch({error, ?ENOSPC}, rename(P1, User1,        f(<<"space1">>, File3), f(<<"space0">>, File3))),
    ?assertMatch(ok, rename(P1, User1,                      f(<<"space1">>, [File3], File3), f(<<"space0">>, File3))),

    %% Cleanup only
    ?assertMatch(ok, unlink(P1, User1,                      f(<<"space0">>, File2))),
    ?assertMatch(ok, unlink(P1, User1,                      f(<<"space0">>, [File3], File2))),

    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space2">>, File1), 0, crypto:rand_bytes(16))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space2">>, File2), 0, crypto:rand_bytes(12))),
    ?assertMatch(ok, rename(P1, User1,                      f(<<"space2">>, File2), f(<<"space0">>, File2))),
    ?assertMatch({error, ?ENOSPC}, rename(P1, User1,        f(<<"space2">>, File1), f(<<"space0">>, File1))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space2">>, [File3, File3], File2), 0, crypto:rand_bytes(8))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space2">>, [File3], File2), 0, crypto:rand_bytes(2))),
    ?assertMatch({error, ?ENOSPC}, rename(P1, User1,        f(<<"space2">>, File3), f(<<"space0">>, File3))),

    ok.


soft_quota_should_allow_bigger_writes(Config) ->
    #env{p1 = P1, p2 = _P2, user1 = User1, user2 = User2, file1 = File1, file2 = File2, file3 = File3} =
        gen_test_env(Config),

    {ok, _} = create_file(P1, User1,    f(<<"space1">>, File1)),
    {ok, _} = create_file(P1, User1,    f(<<"space1">>, File2)),
    {ok, _} = create_file(P1, User2,    f(<<"space1">>, File3)),

    {ok, _} = create_file(P1, User1,    f(<<"space2">>, File1)),
    {ok, _} = create_file(P1, User1,    f(<<"space2">>, File2)),
    {ok, _} = create_file(P1, User2,    f(<<"space2">>, File3)),

    SetSoftLimit =
        fun(Size) ->
            Workers = ?config(op_worker_nodes, Config),
            rpc:multicall(Workers, application, set_env, [?APP_NAME, soft_quota_limit_size, Size])
        end,

    SetSoftLimit(0),

    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space1">>, File1), 0, crypto:rand_bytes(16))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space1">>, File2), 0, crypto:rand_bytes(12))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space1">>, File3), 0, crypto:rand_bytes(8))),

    SetSoftLimit(6),
    ?assertMatch({ok, _}, write_to_file(P1, User2,          f(<<"space1">>, File3), 0, crypto:rand_bytes(8))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space1">>, File3), 0, crypto:rand_bytes(9))),

    SetSoftLimit(10),
    ?assertMatch({ok, _}, write_to_file(P1, User2,          f(<<"space1">>, File3), 0, crypto:rand_bytes(12))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space1">>, File3), 0, crypto:rand_bytes(13))),



    SetSoftLimit(0),

    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space2">>, File1), 0, crypto:rand_bytes(36))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space2">>, File2), 0, crypto:rand_bytes(12))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space2">>, File3), 0, crypto:rand_bytes(8))),

    SetSoftLimit(6),
    ?assertMatch({ok, _}, write_to_file(P1, User2,          f(<<"space2">>, File3), 0, crypto:rand_bytes(8))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space2">>, File3), 0, crypto:rand_bytes(9))),

    SetSoftLimit(10),
    ?assertMatch({ok, _}, write_to_file(P1, User2,          f(<<"space2">>, File3), 0, crypto:rand_bytes(12))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space2">>, File3), 0, crypto:rand_bytes(13))),

    ok.



%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ConfigWithNodes = ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json"), [initializer]),
    initializer:setup_storage(ConfigWithNodes).

end_per_suite(Config) ->
    initializer:teardown_storage(Config),
    test_node_starter:clean_environment(Config).

init_per_testcase(_, Config) ->
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    initializer:enable_grpca_based_communication(Config),

    Workers = ?config(op_worker_nodes, ConfigWithSessionInfo),
    rpc:multicall(Workers, application, set_env, [?APP_NAME, soft_quota_limit_size, 0]),

    lfm_proxy:init(ConfigWithSessionInfo).


end_per_testcase(_, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lists:foreach(
        fun({SpaceId, _}) ->
            rpc:multicall(Workers, space_quota, delete, [SpaceId])
        end, ?config(spaces, Config)),

    lfm_proxy:teardown(Config),
     %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:disable_grpca_based_communication(Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================


create_file(Worker, SessionId, Path) ->
    lfm_proxy:create(Worker, SessionId, Path, ?DEFAULT_FILE_MODE).

open_file(Worker, SessionId, Path, OpenMode) ->
    lfm_proxy:open(Worker, SessionId, {path, Path}, OpenMode).

write_to_file(Worker, SessionId, Path, Offset, Data) ->
    {ok, FileHandle} = open_file(Worker, SessionId, Path, write),
    Result = lfm_proxy:write(Worker, FileHandle, Offset, Data),
    lfm_proxy:fsync(Worker, FileHandle),
    timer:sleep(500), %% @todo: remove after fixing fsync
    lfm_proxy:close(Worker, FileHandle),
    Result.

get_file_content(Worker, SessionId, Path) ->
    {ok, FileHandle} = open_file(Worker, SessionId, Path, read),
    Result = case lfm_proxy:read(Worker, FileHandle, ?FILE_BEGINNING, ?INFINITY) of
                 {error, Error} -> {error, Error};
                 {ok, Content} -> Content
             end,
    lfm_proxy:close(Worker, FileHandle),
    Result.

mkdir(Worker, SessionId, Path) ->
    lfm_proxy:mkdir(Worker, SessionId, Path).

unlink(Worker, SessionId, Path) ->
    lfm_proxy:unlink(Worker, SessionId, {path, Path}).

truncate(Worker, SessionId, Path, Size) ->
    {ok, FileHandle} = open_file(Worker, SessionId, Path, write),
    Result = lfm_proxy:truncate(Worker, SessionId, {path, Path}, Size),
    lfm_proxy:fsync(Worker, FileHandle),
    timer:sleep(500), %% @todo: remove after fixing fsync
    lfm_proxy:close(Worker, FileHandle),
    Result.

rename(Worker, SessionId, Path, Target) ->
    lfm_proxy:mv(Worker, SessionId, {path, Path}, Target).


gen_test_env(Config) ->
    [P1, P2] = ?config(op_worker_nodes, Config),
    User1 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(P1)}}, Config),
    User2 = ?config({session_id, {<<"user2">>, ?GET_DOMAIN(P1)}}, Config),

    #env{
        p1 = P1,
        p2 = P2,
        user1 = User1,
        user2 = User2,
        file1 = generator:gen_name(),
        file2 = generator:gen_name(),
        file3 = generator:gen_name(),
        dir1 = generator:gen_name()
    }.

f(Space, FileName) ->
    fslogic_path:join([<<?DIRECTORY_SEPARATOR>>, <<"spaces">>, Space, FileName]).

f(Space, Dirs, FileName) ->
    fslogic_path:join([<<?DIRECTORY_SEPARATOR>>, <<"spaces">>, Space] ++ Dirs ++ [FileName]).