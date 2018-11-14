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
    % single provider tests
    write_bigger_then_quota_should_fail/1,
    write_smaller_then_quota_should_not_fail/1,
    truncate_bigger_then_quota_should_fail/1,
    truncate_smaller_then_quota_should_not_fail/1,
    incremental_write_bigger_then_quota_should_fail/1,
    incremental_write_smaller_then_quota_should_not_fail/1,
    unlink_should_unlock_space/1,
    rename_should_unlock_space/1,
    rename_bigger_then_quota_should_fail/1,
    
    % multiple providers tests
    multiprovider_test/1,
    remove_file_on_remote_provider_should_unlock_space/1,
    replicate_file_smaller_than_quota_should_not_fail/1,
    replicate_file_bigger_than_quota_should_fail/1,
    
    % gui upload tests
    quota_updated_on_gui_upload/1,
    failed_gui_upload_test/1
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
        multiprovider_test,
        remove_file_on_remote_provider_should_unlock_space,
        replicate_file_smaller_than_quota_should_not_fail,
%        replicate_file_bigger_than_quota_should_fail,
        quota_updated_on_gui_upload
        % TODO Uncomment after resolving VFS-5101
%        failed_gui_upload_test
    ]).

-define(ATTEMPTS, 60).
-define(DEFAULT_FILE_MODE, 8#664).

-record(env, {
    p1, p2, user1, user2, file1, file2, file3, dir1
}).

%% Spaces support:
%%  p1 -> space_id0: 20 bytes
%%  p1 -> space_id1: 30 bytes
%%  p2 -> space_id2: 50 bytes
%%  p1 -> space_id3: 20 bytes
%%  p2 -> space_id3: 20 bytes
%%  p1 -> space_id4: 1000000000 bytes = ~953 MB

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

    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space1">>, File1), 0, crypto:strong_rand_bytes(31))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space1">>, File1), 0, crypto:strong_rand_bytes(38))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space1">>, File1), 0, crypto:strong_rand_bytes(3131))),

    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space1">>, File2), 0, crypto:strong_rand_bytes(31))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space1">>, File2), 0, crypto:strong_rand_bytes(38))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space1">>, File2), 0, crypto:strong_rand_bytes(3131))),

    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space2">>, File1), 0, crypto:strong_rand_bytes(51))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space2">>, File1), 0, crypto:strong_rand_bytes(58))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space2">>, File1), 0, crypto:strong_rand_bytes(3131))),

    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space2">>, File2), 0, crypto:strong_rand_bytes(51))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space2">>, File2), 0, crypto:strong_rand_bytes(58))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space2">>, File2), 0, crypto:strong_rand_bytes(3131))),

    ok.

write_smaller_then_quota_should_not_fail(Config) ->
    #env{p1 = P1, p2 = _P2, user1 = User1, user2 = User2, file1 = File1, file2 = _File2} =
        gen_test_env(Config),

    {ok, _} = create_file(P1, User1, f(<<"space1">>, File1)),
    {ok, _} = create_file(P1, User1, f(<<"space2">>, File1)),

    ?assertMatch({ok, _}, write_to_file(P1, User1, f(<<"space1">>, File1), 0, crypto:strong_rand_bytes(29))),
    ?assertMatch({ok, _}, write_to_file(P1, User1, f(<<"space1">>, File1), 0, crypto:strong_rand_bytes(5))),
    ?assertMatch({ok, _}, write_to_file(P1, User1, f(<<"space1">>, File1), 0, crypto:strong_rand_bytes(30))),

    ?assertMatch({ok, _}, write_to_file(P1, User2, f(<<"space1">>, File1), 0, crypto:strong_rand_bytes(29))),
    ?assertMatch({ok, _}, write_to_file(P1, User2, f(<<"space1">>, File1), 0, crypto:strong_rand_bytes(5))),
    ?assertMatch({ok, _}, write_to_file(P1, User2, f(<<"space1">>, File1), 0, crypto:strong_rand_bytes(30))),

    ?assertMatch({ok, _}, write_to_file(P1, User1, f(<<"space2">>, File1), 0, crypto:strong_rand_bytes(49))),
    ?assertMatch({ok, _}, write_to_file(P1, User1, f(<<"space2">>, File1), 0, crypto:strong_rand_bytes(4))),
    ?assertMatch({ok, _}, write_to_file(P1, User1, f(<<"space2">>, File1), 0, crypto:strong_rand_bytes(50))),

    ?assertMatch({ok, _}, write_to_file(P1, User2, f(<<"space2">>, File1), 0, crypto:strong_rand_bytes(49))),
    ?assertMatch({ok, _}, write_to_file(P1, User2, f(<<"space2">>, File1), 0, crypto:strong_rand_bytes(4))),
    ?assertMatch({ok, _}, write_to_file(P1, User2, f(<<"space2">>, File1), 0, crypto:strong_rand_bytes(50))),

    ok.

truncate_bigger_then_quota_should_fail(Config) ->
    #env{p1 = P1, p2 = _P2, user1 = User1, user2 = User2, file1 = File1, file2 = File2} =
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
    #env{p1 = P1, p2 = _P2, user1 = User1, user2 = User2, file1 = File1, file2 = File2} =
        gen_test_env(Config),

    {ok, _} = create_file(P1, User1, f(<<"space1">>, File1)),
    {ok, _} = create_file(P1, User2, f(<<"space1">>, File2)),
    {ok, _} = create_file(P1, User1, f(<<"space2">>, File1)),
    {ok, _} = create_file(P1, User2, f(<<"space2">>, File2)),

    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space1">>, File1), 0,  crypto:strong_rand_bytes(5))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space1">>, File1), 2,  crypto:strong_rand_bytes(20))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space1">>, File1), 19, crypto:strong_rand_bytes(12))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space1">>, File1), 29, crypto:strong_rand_bytes(5))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space1">>, File2), 0,  crypto:strong_rand_bytes(9))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space1">>, File2), 0,  crypto:strong_rand_bytes(134))),

    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space1">>, File1), 0,  crypto:strong_rand_bytes(5))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space1">>, File1), 2,  crypto:strong_rand_bytes(20))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space1">>, File1), 19, crypto:strong_rand_bytes(12))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space1">>, File1), 29, crypto:strong_rand_bytes(5))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space1">>, File2), 0,  crypto:strong_rand_bytes(9))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space1">>, File2), 0,  crypto:strong_rand_bytes(134))),

    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space2">>, File1), 0,  crypto:strong_rand_bytes(17))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space2">>, File1), 12, crypto:strong_rand_bytes(31))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space2">>, File1), 19, crypto:strong_rand_bytes(32))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space2">>, File1), 49, crypto:strong_rand_bytes(5))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space2">>, File2), 0,  crypto:strong_rand_bytes(9))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space2">>, File2), 0,  crypto:strong_rand_bytes(134))
    ),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space2">>, File1), 0,  crypto:strong_rand_bytes(17))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space2">>, File1), 12, crypto:strong_rand_bytes(31))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space2">>, File1), 19, crypto:strong_rand_bytes(32))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space2">>, File1), 49, crypto:strong_rand_bytes(5))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space2">>, File2), 0,  crypto:strong_rand_bytes(9))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space2">>, File2), 0,  crypto:strong_rand_bytes(134))),

    ok.

incremental_write_smaller_then_quota_should_not_fail(Config) ->
    #env{p1 = P1, p2 = _P2, user1 = User1, user2 = User2, file1 = File1, file2 = File2} =
        gen_test_env(Config),

    {ok, _} = create_file(P1, User1, f(<<"space1">>, File1)),
    {ok, _} = create_file(P1, User2, f(<<"space1">>, File2)),
    {ok, _} = create_file(P1, User1, f(<<"space2">>, File1)),
    {ok, _} = create_file(P1, User2, f(<<"space2">>, File2)),

    ?assertMatch({ok, _}, write_to_file(P1, User1, f(<<"space1">>, File1), 0,  crypto:strong_rand_bytes(5))),
    ?assertMatch({ok, _}, write_to_file(P1, User1, f(<<"space1">>, File1), 2,  crypto:strong_rand_bytes(20))),
    ?assertMatch({ok, _}, write_to_file(P1, User1, f(<<"space1">>, File1), 19, crypto:strong_rand_bytes(1))),
    ?assertMatch({ok, _}, write_to_file(P1, User2, f(<<"space1">>, File2), 0,  crypto:strong_rand_bytes(7))),
    ?assertMatch({ok, _}, write_to_file(P1, User2, f(<<"space1">>, File2), 7,  crypto:strong_rand_bytes(1))),

    ?assertMatch({ok, _}, write_to_file(P1, User1, f(<<"space1">>, File1), 0,  crypto:strong_rand_bytes(5))),
    ?assertMatch({ok, _}, write_to_file(P1, User1, f(<<"space1">>, File1), 2,  crypto:strong_rand_bytes(20))),
    ?assertMatch({ok, _}, write_to_file(P1, User1, f(<<"space1">>, File1), 19, crypto:strong_rand_bytes(1))),
    ?assertMatch({ok, _}, write_to_file(P1, User2, f(<<"space1">>, File2), 0,  crypto:strong_rand_bytes(7))),
    ?assertMatch({ok, _}, write_to_file(P1, User2, f(<<"space1">>, File2), 7,  crypto:strong_rand_bytes(1))),

    ?assertMatch({ok, _}, write_to_file(P1, User1, f(<<"space2">>, File1), 0,  crypto:strong_rand_bytes(17))),
    ?assertMatch({ok, _}, write_to_file(P1, User1, f(<<"space2">>, File1), 12, crypto:strong_rand_bytes(21))),
    ?assertMatch({ok, _}, write_to_file(P1, User1, f(<<"space2">>, File1), 19, crypto:strong_rand_bytes(11))),
    ?assertMatch({ok, _}, write_to_file(P1, User2, f(<<"space2">>, File2), 0,  crypto:strong_rand_bytes(7))),
    ?assertMatch({ok, _}, write_to_file(P1, User2, f(<<"space2">>, File2), 7,  crypto:strong_rand_bytes(10))
    ),
    ?assertMatch({ok, _}, write_to_file(P1, User1, f(<<"space2">>, File1), 0,  crypto:strong_rand_bytes(17))),
    ?assertMatch({ok, _}, write_to_file(P1, User1, f(<<"space2">>, File1), 12, crypto:strong_rand_bytes(21))),
    ?assertMatch({ok, _}, write_to_file(P1, User1, f(<<"space2">>, File1), 19, crypto:strong_rand_bytes(11))),
    ?assertMatch({ok, _}, write_to_file(P1, User2, f(<<"space2">>, File2), 0,  crypto:strong_rand_bytes(7))),
    ?assertMatch({ok, _}, write_to_file(P1, User2, f(<<"space2">>, File2), 7,  crypto:strong_rand_bytes(10))),

    ok.

unlink_should_unlock_space(Config) ->
    #env{p1 = P1, p2 = _P2, user1 = User1, user2 = User2, file1 = File1, file2 = File2, file3 = File3} =
        gen_test_env(Config),

    {ok, _} = create_file(P1, User1, f(<<"space1">>, File1)),
    {ok, _} = create_file(P1, User1, f(<<"space1">>, File2)),
    {ok, _} = create_file(P1, User2, f(<<"space1">>, File3)),
    {ok, _} = create_file(P1, User1, f(<<"space2">>, File1)),
    {ok, _} = create_file(P1, User1, f(<<"space2">>, File2)),
    {ok, _} = create_file(P1, User2, f(<<"space2">>, File3)),

    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space1">>, File1), 0, crypto:strong_rand_bytes(16))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space1">>, File2), 0, crypto:strong_rand_bytes(12))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space1">>, File3), 0, crypto:strong_rand_bytes(3))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space1">>, File3), 0, crypto:strong_rand_bytes(18))),
    ?assertMatch(ok, unlink(P1, User1,                      f(<<"space1">>, File2))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space1">>, File3), 0, crypto:strong_rand_bytes(3))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space1">>, File3), 0, crypto:strong_rand_bytes(18))),
    ?assertMatch(ok, unlink(P1, User1,                      f(<<"space1">>, File1))),
    ?assertMatch({ok, _}, write_to_file(P1, User2,          f(<<"space1">>, File3), 0, crypto:strong_rand_bytes(18))),

    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space2">>, File1), 0, crypto:strong_rand_bytes(26))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space2">>, File2), 0, crypto:strong_rand_bytes(22))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space2">>, File3), 0, crypto:strong_rand_bytes(3))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space2">>, File3), 0, crypto:strong_rand_bytes(28))),
    ?assertMatch(ok, unlink(P1, User1,                      f(<<"space2">>, File2))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space2">>, File3), 0, crypto:strong_rand_bytes(3))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space2">>, File3), 0, crypto:strong_rand_bytes(28))),
    ?assertMatch(ok, unlink(P1, User1,                      f(<<"space2">>, File1))),
    ?assertMatch({ok, _}, write_to_file(P1, User2,          f(<<"space2">>, File3), 0, crypto:strong_rand_bytes(28))),

    ok.

rename_should_unlock_space(Config) ->
    #env{p1 = P1, p2 = _P2, user1 = User1, user2 = User2, file1 = File1, file2 = File2, file3 = File3, dir1 = Dir1} =
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
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space1">>, File1), 0, crypto:strong_rand_bytes(16))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space1">>, File2), 0, crypto:strong_rand_bytes(12))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space1">>, File3), 0, crypto:strong_rand_bytes(3))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space1">>, File3), 0, crypto:strong_rand_bytes(18))),
    ?assertMatch({ok, _}, rename(P1, User1,                 f(<<"space1">>, File2), f(<<"space0">>, File2))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space1">>, File3), 0, crypto:strong_rand_bytes(3))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space1">>, File3), 0, crypto:strong_rand_bytes(18))),
    ?assertMatch(ok, unlink(P1, User1,                      f(<<"space0">>, File2))),
    ?assertMatch({ok, _}, rename(P1, User1,                 f(<<"space1">>, File1), f(<<"space0">>, File1))),

    %% Cleanup only
    ?assertMatch(ok, unlink(P1, User1,                      f(<<"space0">>, File1))),

    ?assertMatch({ok, _}, write_to_file(P1, User2,          f(<<"space1">>, [Dir1], File1), 0, crypto:strong_rand_bytes(17))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space1">>, File3), 3, crypto:strong_rand_bytes(11))),
    ?assertMatch({ok, _}, rename(P1, User1,                 f(<<"space1">>, Dir1), f(<<"space0">>, Dir1))),
    ?assertMatch({ok, _}, write_to_file(P1, User2,          f(<<"space1">>, File3), 3, crypto:strong_rand_bytes(11))),
    ?assertMatch({ok, _}, write_to_file(P1, User2,          f(<<"space1">>, File3), 3, crypto:strong_rand_bytes(17))),

    %% Cleanup only
    ?assertMatch(ok, unlink(P1, User1,                      f(<<"space0">>, [Dir1], File1))),
    ?assertMatch(ok, unlink(P1, User1,                      f(<<"space0">>, Dir1))),


    %% ### Space2 ###
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space2">>, File1), 0, crypto:strong_rand_bytes(26))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space2">>, File2), 0, crypto:strong_rand_bytes(18))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space2">>, File3), 0, crypto:strong_rand_bytes(7))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space2">>, File3), 0, crypto:strong_rand_bytes(28))),
    ?assertMatch({ok, _}, rename(P1, User1,                 f(<<"space2">>, File2), f(<<"space0">>, File2))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space2">>, File3), 0, crypto:strong_rand_bytes(7))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space2">>, File3), 0, crypto:strong_rand_bytes(28))),

    %% Cleanup only
    ?assertMatch(ok, unlink(P1, User1,                      f(<<"space0">>, File2))),
    ?assertMatch(ok, unlink(P1, User1,                      f(<<"space2">>, File1))),

    ?assertMatch({ok, _}, write_to_file(P1, User2,          f(<<"space2">>, [Dir1], File1), 0, crypto:strong_rand_bytes(17))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space2">>, File3), 7, crypto:strong_rand_bytes(27))),
    ?assertMatch({ok, _}, rename(P1, User1,                 f(<<"space2">>, Dir1), f(<<"space0">>, Dir1))),
    ?assertMatch({ok, _}, write_to_file(P1, User2,          f(<<"space2">>, File3), 7, crypto:strong_rand_bytes(27))),
    ?assertMatch({ok, _}, write_to_file(P1, User2,          f(<<"space2">>, File3), 7, crypto:strong_rand_bytes(37))),

    ok.


rename_bigger_then_quota_should_fail(Config) ->
    #env{p1 = P1, p2 = _P2, user1 = User1, user2 = _User2, file1 = File1, file2 = File2, file3 = File3} =
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


    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space1">>, File1), 0, crypto:strong_rand_bytes(16))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space1">>, File2), 0, crypto:strong_rand_bytes(12))),
    ?assertMatch({ok, _}, rename(P1, User1,                 f(<<"space1">>, File2), f(<<"space0">>, File2))),
    ?assertMatch({error, ?ENOSPC}, rename(P1, User1,        f(<<"space1">>, File1), f(<<"space0">>, File1))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space1">>, [File3, File3], File2), 0, crypto:strong_rand_bytes(8))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space1">>, [File3], File2), 0, crypto:strong_rand_bytes(2))),
    ?assertMatch({error, ?ENOSPC}, rename(P1, User1,        f(<<"space1">>, File3), f(<<"space0">>, File3))),
    ?assertMatch(ok, rm_recursive(P1, User1,                f(<<"space0">>, File3))),
    ?assertMatch({ok, _}, rename(P1, User1,                 f(<<"space1">>, [File3], File3), f(<<"space0">>, File3))),

    %% Cleanup only
    ?assertMatch(ok, unlink(P1, User1,                      f(<<"space0">>, File2))),
    ?assertMatch(ok, unlink(P1, User1,                      f(<<"space0">>, [File3], File2))),

    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space2">>, File1), 0, crypto:strong_rand_bytes(16))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space2">>, File2), 0, crypto:strong_rand_bytes(12))),
    ?assertMatch({ok, _}, rename(P1, User1,                 f(<<"space2">>, File2), f(<<"space0">>, File2))),
    ?assertMatch({error, ?ENOSPC}, rename(P1, User1,        f(<<"space2">>, File1), f(<<"space0">>, File1))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space2">>, [File3, File3], File2), 0, crypto:strong_rand_bytes(8))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space2">>, [File3], File2), 0, crypto:strong_rand_bytes(2))),
    ?assertMatch({error, ?ENOSPC}, rename(P1, User1,        f(<<"space2">>, File3), f(<<"space0">>, File3))),

    ok.


multiprovider_test(Config) ->
    #env{p1 = P1, p2 = P2, user1 = User1, file1 = File1, file2 = File2} =
        gen_test_env(Config),
    SessId = fun(Worker) -> ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config) end,
    
    {ok, _} = create_file(P1, SessId(P1), f(<<"space3">>, File1)),
    {ok, _} = create_file(P2, SessId(P2), f(<<"space3">>, File2)),

    ?assertMatch({ok, _}, write_to_file(P1, User1, f(<<"space3">>, File1), 0, crypto:strong_rand_bytes(10))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P2, SessId(P2), f(<<"space3">>, File2), 0, crypto:strong_rand_bytes(40))),
    ?assertMatch({ok, _}, write_to_file(P1, User1, f(<<"space3">>, File1), 10, crypto:strong_rand_bytes(10))),

    ?assertMatch(0, current_size(P2, <<"space_id3">>)),
    ?assertMatch(20, current_size(P1, <<"space_id3">>)),

    ?assertMatch(20, available_size(P2, <<"space_id3">>)),
    ?assertMatch(0, available_size(P1, <<"space_id3">>)).


remove_file_on_remote_provider_should_unlock_space(Config) ->
    #env{p1 = P1, p2 = P2, file1 = File1, file2 = File2} =
        gen_test_env(Config),
    SessId = fun(Worker) -> ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config) end,
    
    {ok, _} = create_file(P1, SessId(P1), f(<<"space3">>, File1)),
    {ok, _} = create_file(P1, SessId(P1), f(<<"space3">>, File2)),
    
    ?assertMatch({ok, _}, write_to_file(P1, SessId(P1), f(<<"space3">>, File1), 0, crypto:strong_rand_bytes(20))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, SessId(P1), f(<<"space3">>, File2), 0, crypto:strong_rand_bytes(20))),

    ?assertMatch({ok, _}, lfm_proxy:stat(P2, SessId(P2), {path, f(<<"space3">>, File1)}), ?ATTEMPTS),
    ?assertMatch(ok, unlink(P2, SessId(P2), f(<<"space3">>, File1))),
    ?assertMatch({error, enoent}, lfm_proxy:stat(P1, SessId(P1), {path, f(<<"space3">>, File1)}), ?ATTEMPTS),
    ?assertMatch(0, current_size(P2, <<"space_id3">>)),
    ?assertMatch(0, current_size(P1, <<"space_id3">>)),
    ?assertMatch({ok, _}, write_to_file(P1, SessId(P1), f(<<"space3">>, File2), 0, crypto:strong_rand_bytes(20))).


replicate_file_smaller_than_quota_should_not_fail(Config) ->
    #env{p1 = P1, p2 = P2, file1 = File1} =
        gen_test_env(Config),
    SessId = fun(Worker) -> ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config) end,
    
    {ok, Guid} = create_file(P1, SessId(P1), f(<<"space3">>, File1)),
    ?assertMatch({ok, _}, write_to_file(P1, SessId(P1), f(<<"space3">>, File1), 0, crypto:strong_rand_bytes(20))),
    {ok, Tid} = lfm_proxy:schedule_file_replication(P1, SessId(P1), {guid, Guid}, ?GET_DOMAIN_BIN(P2)),
    
    % wait for replication to finish
    ?assertMatch({ok, []}, rpc:call(P1, transfer, list_waiting_transfers, [<<"space_id3">>]), ?ATTEMPTS),
    ?assertMatch({ok, []}, rpc:call(P1, transfer, list_ongoing_transfers, [<<"space_id3">>]), ?ATTEMPTS),
    ?assertEqual(true, lists:member(Tid, list_ended_transfers(P1, <<"space_id3">>)), ?ATTEMPTS),
    
    ?assertMatch({ok, [#{<<"totalBlocksSize">> := 20}, #{<<"totalBlocksSize">> := 20}]}, 
        lfm_proxy:get_file_distribution(P2, SessId(P2), {guid, Guid}), 30),
    
    ok = fsync(P2, SessId(P2), f(<<"space3">>, File1)),
    ?assertEqual(20, current_size(P1, <<"space_id3">>), ?ATTEMPTS),
    ?assertEqual(20, current_size(P2, <<"space_id3">>), ?ATTEMPTS),

    ?assertMatch({ok, _}, lfm_proxy:stat(P2, SessId(P2), {guid, Guid}), ?ATTEMPTS),
    ?assertMatch(ok, unlink(P2, SessId(P2), f(<<"space3">>, File1))),

    ?assertMatch({error, enoent}, lfm_proxy:stat(P1, SessId(P1), {guid, Guid}), ?ATTEMPTS),
    ?assertEqual(0, current_size(P2, <<"space_id3">>)),
    ?assertEqual(0, current_size(P1, <<"space_id3">>)).


replicate_file_bigger_than_quota_should_fail(Config) ->
    #env{p1 = P1, p2 = P2, file1 = File1, file2 = File2} =
        gen_test_env(Config),
    SessId = fun(Worker) -> ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config) end,
    
    % How many bytes can be written above quota
    Tolerance = 100*1024*1024, % 100 MB
    MaxSize = 1000000000,
    
    {ok, Guid} = create_file(P1, SessId(P1), f(<<"space5">>, File1)),
    {ok, _} = create_file(P2, SessId(P2), f(<<"space5">>, File2)),
    ?assertMatch({ok, _}, write_to_file(P1, SessId(P1), f(<<"space5">>, File1), 0, crypto:strong_rand_bytes(MaxSize))),
    % Make sure that there is not enough space left in destination space
    ?assertMatch({ok, _}, write_to_file(P2, SessId(P2), f(<<"space5">>, File2), 0, crypto:strong_rand_bytes(Tolerance))),
    
    {ok, Tid} = lfm_proxy:schedule_file_replication(P1, SessId(P1), {guid, Guid}, ?GET_DOMAIN_BIN(P2)),
    
    % wait for replication to finish
    ?assertMatch({ok, []}, rpc:call(P1, transfer, list_waiting_transfers, [<<"space_id5">>]), ?ATTEMPTS),
    ?assertMatch({ok, []}, rpc:call(P1, transfer, list_ongoing_transfers, [<<"space_id5">>]), ?ATTEMPTS),
    ?assertEqual(true, lists:member(Tid, list_ended_transfers(P1, <<"space_id5">>)), ?ATTEMPTS),
    
    ok = fsync(P2, SessId(P2), f(<<"space5">>, File1)),
    ?assertEqual(true, available_size(P2, <<"space_id5">>) > -Tolerance, ?ATTEMPTS),
    T = rpc:call(P1, transfer, get, [Tid]),
    ?assertMatch(#transfer{replication_status = failed}, T).
    

quota_updated_on_gui_upload(Config) ->
    #env{p1 = P1, p2 = P2, file1 = File1, file2 = File2} =
        gen_test_env(Config),
    SessId = fun(Worker) -> ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config) end,
    
    {ok, #file_attr{guid = Guid}} = lfm_proxy:stat(P1, SessId(P1), {path, <<"/space3">>}),
    
    do_multipart(P1, SessId(P1), 1, 20, 1, Guid, File1),
    
    {ok, _} = lfm_proxy:stat(P1, SessId(P1), {path, f(<<"space3">>, File1)}),
    {ok, FileHandle} = lfm_proxy:open(P1, SessId(P1), {path, f(<<"space3">>, File1)}, rdwr),
    ok = lfm_proxy:fsync(P1, FileHandle),

    ?assertEqual(20, current_size(P1, <<"space_id3">>)),
    ?assertEqual(0, current_size(P2, <<"space_id3">>)),

    {ok, _} = create_file(P1, SessId(P1), f(<<"space3">>, File2)),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, SessId(P1), f(<<"space3">>, File2), 0, crypto:strong_rand_bytes(10))).


failed_gui_upload_test(Config) ->
    #env{p1 = P1, user1 = User1, file1 = File1, file2 = File2} =
        gen_test_env(Config),

    {ok, #file_attr{guid = Guid}} = lfm_proxy:stat(P1, User1, {path, <<"/space4">>}),
    
    FileSize = 500*1024*1024, % 500 MB

    ProviderId = initializer:domain_to_provider_id(?GET_DOMAIN(P1)),
    % Upload File1 500MB to space4
    do_multipart(P1, User1, 100, 1048576, 5, Guid, File1),
    
    {ok, FileHandle} = lfm_proxy:open(P1, User1, {path, f(<<"space4">>, File1)}, rdwr),
    ok = lfm_proxy:fsync(P1, FileHandle),
    ?assertMatch(FileSize, current_size(P1, <<"space_id4">>)),
    
    % Upload File2 800MB to space4
    do_multipart(P1, User1, 160, 1048576, 5, Guid, File2),
    ok = lfm_proxy:fsync(P1, User1, {path, f(<<"space4">>, File2)}, ProviderId),

    StorageFilePath1 = storage_file_path(P1, <<"space_id4">>, File2),
    ?assertMatch(FileSize, current_size(P1, <<"space_id4">>)),
    ?assertMatch({error, enoent}, open_file(P1, StorageFilePath1), ?ATTEMPTS),
    ?assertMatch({error, enoent}, lfm_proxy:stat(P1, User1, {path, f(<<"space4">>, File2)}), ?ATTEMPTS).
    

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) -> initializer:setup_storage(NewConfig) end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].

end_per_suite(Config) ->
    initializer:teardown_storage(Config).

init_per_testcase(Case, Config) when 
    Case =:= quota_updated_on_gui_upload;
    Case =:= failed_gui_upload_test ->
    Workers = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_new(Workers, cow_multipart),
    ok = test_utils:mock_new(Workers, cowboy_req),
    ok = test_utils:mock_expect(Workers, cow_multipart, form_data, 
        fun(_) -> {file, ok, ok, ok} end),
    ok = test_utils:mock_expect(Workers, cowboy_req, read_part_body, 
        fun F(#{left := 1}=Req, Opts) -> 
                F(maps:remove(left, Req), Opts);
            F(#{left := Left, size := Size} = Req, _Opts) -> 
                {more, crypto:strong_rand_bytes(Size), Req#{left => Left-1}};
            F(#{size := Size}=Req, _Opts) -> 
                {ok, crypto:strong_rand_bytes(Size), Req#{done => true}}
        end),
    ok = test_utils:mock_expect(Workers, cowboy_req, read_part, 
        fun (#{done := true} = Req) -> {done, Req}; (Req) -> {ok, [], Req} end),
    init_per_testcase(default, Config);

init_per_testcase(_Case, Config) ->
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    lfm_proxy:init(ConfigWithSessionInfo).

end_per_testcase(Case, Config) when
    Case =:= quota_updated_on_gui_upload;
    Case =:= failed_upload_test ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, cowboy_req),
    test_utils:mock_unload(Workers, cow_multipart),
    end_per_testcase(all, Config);
    

end_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),

    lfm_proxy:teardown(Config),
     %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    initializer:clean_test_users_and_spaces_no_validate(Config),

    lists:foreach(
        fun({SpaceId, _}) ->
            rpc:multicall(Workers, space_quota, delete, [SpaceId])
        end, ?config(spaces, Config)).

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

mkdir(Worker, SessionId, Path) ->
    lfm_proxy:mkdir(Worker, SessionId, Path).

rm_recursive(Worker, SessionId, Path) ->
    lfm_proxy:rm_recursive(Worker, SessionId, {path, Path}).

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
    Result = lfm_proxy:mv(Worker, SessionId, {path, Path}, Target),
    fsync(Worker, SessionId, Target),
    Result.

fsync(Worker, SessionId, Path) ->
    case open_file(Worker, SessionId, Path, write) of
        {ok, FileHandle} ->
            Result = lfm_proxy:fsync(Worker, FileHandle),
            lfm_proxy:close(Worker, FileHandle),
            Result;
        _ ->
            ok
    end.

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
    fslogic_path:join([<<?DIRECTORY_SEPARATOR>>, Space, FileName]).

f(Space, Dirs, FileName) ->
    fslogic_path:join([<<?DIRECTORY_SEPARATOR>>, Space] ++ Dirs ++ [FileName]).

current_size(Worker, SpaceId) ->
    rpc:call(Worker, space_quota, current_size, [SpaceId]).

available_size(Worker, SpaceId) ->
    rpc:call(Worker, space_quota, available_size, [SpaceId]).

do_multipart(Worker, SessionId, PartsNumber, PartSize, ChunksNumber, ParentGuid, FileName) ->
    ChunkSize = PartsNumber*PartSize,
    ParamsProp = [{<<"resumableIdentifier">>, <<"id">>}, {<<"parentId">>, ParentGuid},
        {<<"resumableFilename">>, FileName}, {<<"resumableChunkSize">>, integer_to_binary(ChunkSize)}],
    
    utils:pforeach(fun(Chunk) ->
        rpc:call(Worker, page_file_upload, multipart, [#{size => PartSize, left => PartsNumber}, SessionId, 
            [{<<"resumableChunkNumber">>, integer_to_binary(Chunk)} | ParamsProp]])
    end, lists:seq(1,ChunksNumber)).

open_file(Worker, FilePath) ->
    rpc:call(Worker, file, open, [FilePath, read]).

storage_file_path(Worker, SpaceId, FilePath) ->
    SpaceMnt = get_space_mount_point(Worker, SpaceId),
    filename:join([SpaceMnt, SpaceId, FilePath]).

get_space_mount_point(Worker, SpaceId) ->
    StorageId = get_supporting_storage_id(Worker, SpaceId),
    storage_mount_point(Worker, StorageId).

get_supporting_storage_id(Worker, SpaceId) ->
    [StorageId] = rpc:call(Worker, space_storage, get_storage_ids, [SpaceId]),
    StorageId.

storage_mount_point(Worker, StorageId) ->
    [Helper | _] = rpc:call(Worker, storage, get_helpers, [StorageId]),
    HelperArgs = helper:get_args(Helper),
    maps:get(<<"mountPoint">>, HelperArgs).

list_ended_transfers(Worker, SpaceId) ->
    {ok, List} = rpc:call(Worker, transfer, list_ended_transfers, [SpaceId]),
    List.
