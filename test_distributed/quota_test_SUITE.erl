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

-include("fuse_test_utils.hrl").
-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/onedata.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("clproto/include/messages.hrl").
-include_lib("proto/common/credentials.hrl").

%% API
-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    % single provider tests
    write_with_no_quota_left_should_fail/1,
    truncate_bigger_then_quota_should_not_fail/1,
    truncate_smaller_then_quota_should_not_fail/1,
    incremental_write_with_no_quota_left_should_fail/1,
    unlink_should_unlock_space/1,
    rename_should_unlock_space/1,
    rename_with_no_quota_left_should_fail/1,

    % multiple providers tests
    multiprovider_test/1,
    remove_file_on_remote_provider_should_unlock_space/1,
    replicate_file_smaller_than_quota_should_not_fail/1,
    replicate_file_bigger_than_quota_should_fail/1,

    % gui upload tests
    quota_updated_on_gui_upload/1,
    failed_gui_upload_test/1,

    % events tests
    events_sent_to_client_directio/1,
    events_sent_to_client_proxyio/1
]).

all() ->
    ?ALL([
        % single provider tests
        write_with_no_quota_left_should_fail,
        truncate_bigger_then_quota_should_not_fail,
        truncate_smaller_then_quota_should_not_fail,
        incremental_write_with_no_quota_left_should_fail,
        unlink_should_unlock_space,
        rename_should_unlock_space,
        rename_with_no_quota_left_should_fail,

        % multiple providers tests
        multiprovider_test,
        remove_file_on_remote_provider_should_unlock_space,
        replicate_file_smaller_than_quota_should_not_fail,
        % TODO uncomment after fixing rtransfer not respecting quota
%        replicate_file_bigger_than_quota_should_fail,

        % gui upload tests
        quota_updated_on_gui_upload,
        % TODO uncomment after resolving VFS-5101
%        failed_gui_upload_test

        % events tests
        % TODO uncomment after resolving VFS-5248
        % events_sent_to_client_proxyio,
        events_sent_to_client_directio
    ]).

-define(ATTEMPTS, 60).

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


write_with_no_quota_left_should_fail(Config) ->
    #env{p1 = P1, p2 = _P2, user1 = User1, user2 = User2, file1 = File1, file2 = File2} =
        gen_test_env(Config),

    {ok, _} = create_file(P1, User1, f(<<"space1">>, File1)),
    {ok, _} = create_file(P1, User2, f(<<"space1">>, File2)),
    {ok, _} = create_file(P1, User1, f(<<"space2">>, File1)),
    {ok, _} = create_file(P1, User2, f(<<"space2">>, File2)),

    % Writes be allowed until there is no quota left
    ?assertMatch({ok, 29}, write_to_file(P1, User1, f(<<"space1">>, File1), 0, crypto:strong_rand_bytes(29))),
    ?assertMatch({ok, 131}, write_to_file(P1, User1, f(<<"space1">>, File1), 0, crypto:strong_rand_bytes(131))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space1">>, File1), 0, crypto:strong_rand_bytes(38))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space1">>, File1), 0, crypto:strong_rand_bytes(3131))),

    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space1">>, File2), 0, crypto:strong_rand_bytes(31))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space1">>, File2), 0, crypto:strong_rand_bytes(38))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space1">>, File2), 0, crypto:strong_rand_bytes(3131))),

    % Until some space is freed not event writes to already allocated blocks will be permitted
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space1">>, File2), 0, crypto:strong_rand_bytes(5))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space1">>, File2), 0, crypto:strong_rand_bytes(1))),

    ?assertMatch({ok, 29}, write_to_file(P1, User1, f(<<"space2">>, File1), 0, crypto:strong_rand_bytes(29))),
    ?assertMatch({ok, 1131}, write_to_file(P1, User1, f(<<"space2">>, File1), 0, crypto:strong_rand_bytes(1131))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space2">>, File1), 0, crypto:strong_rand_bytes(58))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space2">>, File1), 0, crypto:strong_rand_bytes(3131))),

    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space2">>, File2), 0, crypto:strong_rand_bytes(51))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space2">>, File2), 0, crypto:strong_rand_bytes(58))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space2">>, File2), 0, crypto:strong_rand_bytes(3131))),

    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space2">>, File2), 0, crypto:strong_rand_bytes(5))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space2">>, File2), 0, crypto:strong_rand_bytes(1))),

    ok.

truncate_bigger_then_quota_should_not_fail(Config) ->
    #env{p1 = P1, p2 = _P2, user1 = User1, user2 = User2, file1 = File1, file2 = File2} =
        gen_test_env(Config),

    {ok, _} = create_file(P1, User1, f(<<"space1">>, File1)),
    {ok, _} = create_file(P1, User2, f(<<"space1">>, File2)),
    {ok, _} = create_file(P1, User1, f(<<"space2">>, File1)),
    {ok, _} = create_file(P1, User2, f(<<"space2">>, File2)),

    ?assertMatch(ok, truncate(P1, User1, f(<<"space1">>, File1), 31)),
    ?assertMatch(ok, truncate(P1, User1, f(<<"space1">>, File1), 38)),
    ?assertMatch(ok, truncate(P1, User1, f(<<"space1">>, File1), 3131)),
    ?assertMatch(ok, truncate(P1, User2, f(<<"space1">>, File2), 31)),
    ?assertMatch(ok, truncate(P1, User2, f(<<"space1">>, File2), 38)),
    ?assertMatch(ok, truncate(P1, User2, f(<<"space1">>, File2), 3131)),
    ?assertMatch(ok, truncate(P1, User1, f(<<"space2">>, File1), 51)),
    ?assertMatch(ok, truncate(P1, User1, f(<<"space2">>, File1), 58)),
    ?assertMatch(ok, truncate(P1, User1, f(<<"space2">>, File1), 3131)),
    ?assertMatch(ok, truncate(P1, User2, f(<<"space2">>, File2), 51)),
    ?assertMatch(ok, truncate(P1, User2, f(<<"space2">>, File2), 58)),
    ?assertMatch(ok, truncate(P1, User2, f(<<"space2">>, File2), 3131)),

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

incremental_write_with_no_quota_left_should_fail(Config) ->
    #env{p1 = P1, p2 = _P2, user1 = User1, user2 = User2, file1 = File1, file2 = File2} =
        gen_test_env(Config),

    {ok, _} = create_file(P1, User1, f(<<"space1">>, File1)),
    {ok, _} = create_file(P1, User2, f(<<"space1">>, File2)),
    {ok, _} = create_file(P1, User1, f(<<"space2">>, File1)),
    {ok, _} = create_file(P1, User2, f(<<"space2">>, File2)),

    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space1">>, File1), 0,  crypto:strong_rand_bytes(5))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space1">>, File1), 2,  crypto:strong_rand_bytes(20))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space1">>, File1), 19, crypto:strong_rand_bytes(12))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space1">>, File1), 29, crypto:strong_rand_bytes(5))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space1">>, File2), 0,  crypto:strong_rand_bytes(9))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space1">>, File2), 0,  crypto:strong_rand_bytes(134))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space1">>, File1), 0,  crypto:strong_rand_bytes(5))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space1">>, File1), 2,  crypto:strong_rand_bytes(20))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space1">>, File1), 19, crypto:strong_rand_bytes(12))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space1">>, File1), 29, crypto:strong_rand_bytes(5))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space1">>, File2), 0,  crypto:strong_rand_bytes(9))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space1">>, File2), 0,  crypto:strong_rand_bytes(134))),

    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space2">>, File1), 0,  crypto:strong_rand_bytes(17))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space2">>, File1), 12, crypto:strong_rand_bytes(31))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space2">>, File1), 19, crypto:strong_rand_bytes(32))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space2">>, File1), 49, crypto:strong_rand_bytes(5))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space2">>, File2), 0,  crypto:strong_rand_bytes(9))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space2">>, File2), 0,  crypto:strong_rand_bytes(134))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space2">>, File1), 0,  crypto:strong_rand_bytes(17))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space2">>, File1), 12, crypto:strong_rand_bytes(31))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space2">>, File1), 19, crypto:strong_rand_bytes(32))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User1, f(<<"space2">>, File1), 49, crypto:strong_rand_bytes(5))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space2">>, File2), 0,  crypto:strong_rand_bytes(9))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space2">>, File2), 0,  crypto:strong_rand_bytes(134))),

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
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space1">>, File3), 0, crypto:strong_rand_bytes(3))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space1">>, File3), 0, crypto:strong_rand_bytes(18))),
    ?assertMatch(ok, unlink(P1, User1,                      f(<<"space1">>, File2))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space1">>, File3), 0, crypto:strong_rand_bytes(21))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space1">>, File3), 0, crypto:strong_rand_bytes(3))),
    ?assertMatch(ok, unlink(P1, User1,                      f(<<"space1">>, File1))),
    ?assertMatch({ok, _}, write_to_file(P1, User2,          f(<<"space1">>, File3), 0, crypto:strong_rand_bytes(18))),

    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space2">>, File1), 0, crypto:strong_rand_bytes(26))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space2">>, File2), 0, crypto:strong_rand_bytes(22))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space2">>, File3), 0, crypto:strong_rand_bytes(3))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space2">>, File3), 0, crypto:strong_rand_bytes(28))),
    ?assertMatch(ok, unlink(P1, User1,                      f(<<"space2">>, File2))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space2">>, File3), 0, crypto:strong_rand_bytes(31))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space2">>, File3), 0, crypto:strong_rand_bytes(3))),
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
    ?assertMatch({ok, 3}, write_to_file(P1, User1,          f(<<"space1">>, File3), 0, crypto:strong_rand_bytes(3))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space1">>, File3), 0, crypto:strong_rand_bytes(18))),
    ?assertMatch({ok, _}, rename(P1, User1,                 f(<<"space1">>, File2), f(<<"space0">>, File2))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space1">>, File3), 0, crypto:strong_rand_bytes(3))),
    ?assertMatch({ok, _}, write_to_file(P1, User2,          f(<<"space1">>, File3), 0, crypto:strong_rand_bytes(18))),
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
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space2">>, File3), 0, crypto:strong_rand_bytes(7))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space2">>, File3), 0, crypto:strong_rand_bytes(28))),
    ?assertMatch({ok, _}, rename(P1, User1,                 f(<<"space2">>, File2), f(<<"space0">>, File2))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space2">>, File3), 0, crypto:strong_rand_bytes(7))),
    ?assertMatch({ok, _}, write_to_file(P1, User2, f(<<"space2">>, File3), 0, crypto:strong_rand_bytes(28))),

    %% Cleanup only
    ?assertMatch(ok, unlink(P1, User1,                      f(<<"space0">>, File2))),
    ?assertMatch(ok, unlink(P1, User1,                      f(<<"space2">>, File1))),

    ?assertMatch({ok, _}, write_to_file(P1, User2,          f(<<"space2">>, [Dir1], File1), 0, crypto:strong_rand_bytes(27))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, User2, f(<<"space2">>, File3), 7, crypto:strong_rand_bytes(3))),
    ?assertMatch({ok, _}, rename(P1, User1,                 f(<<"space2">>, Dir1), f(<<"space0">>, Dir1))),
    ?assertMatch({ok, _}, write_to_file(P1, User2,          f(<<"space2">>, File3), 7, crypto:strong_rand_bytes(27))),
    ?assertMatch({ok, _}, write_to_file(P1, User2,          f(<<"space2">>, File3), 7, crypto:strong_rand_bytes(37))),

    ok.


rename_with_no_quota_left_should_fail(Config) ->
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
    ?assertMatch({ok, _}, rename(P1, User1,                 f(<<"space1">>, File1), f(<<"space0">>, File1))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space1">>, [File3, File3], File2), 0, crypto:strong_rand_bytes(8))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space1">>, [File3], File2), 0, crypto:strong_rand_bytes(2))),
    ?assertMatch({error, ?ENOSPC}, rename(P1, User1,        f(<<"space1">>, File3), f(<<"space0">>, File3))),
    ?assertMatch(ok, rm_recursive(P1, User1,                f(<<"space0">>, File3))),
    ?assertMatch({error, ?ENOSPC}, rename(P1, User1,        f(<<"space1">>, [File3], File3), f(<<"space0">>, File3))),

    %% Cleanup only
    ?assertMatch(ok, unlink(P1, User1,                      f(<<"space0">>, File2))),
    ?assertMatch(ok, unlink(P1, User1,                      f(<<"space0">>, [File3], File2))),

    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space2">>, File1), 0, crypto:strong_rand_bytes(16))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space2">>, File2), 0, crypto:strong_rand_bytes(12))),
    ?assertMatch({ok, _}, rename(P1, User1,                 f(<<"space2">>, File2), f(<<"space0">>, File2))),
    ?assertMatch({ok, _}, rename(P1, User1,                 f(<<"space2">>, File1), f(<<"space0">>, File1))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space2">>, [File3, File3], File2), 0, crypto:strong_rand_bytes(8))),
    ?assertMatch({ok, _}, write_to_file(P1, User1,          f(<<"space2">>, [File3], File2), 0, crypto:strong_rand_bytes(2))),
    ?assertMatch({error, ?ENOSPC}, rename(P1, User1,        f(<<"space2">>, File3), f(<<"space0">>, File3))),

    ok.


multiprovider_test(Config) ->
    #env{p1 = P1, p2 = P2, file1 = File1, file2 = File2} =
        gen_test_env(Config),
    SessId = fun(Worker) ->
        ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config) end,

    {ok, _} = create_file(P1, SessId(P1), f(<<"space3">>, File1)),
    {ok, _} = create_file(P2, SessId(P2), f(<<"space3">>, File2)),

    ?assertMatch({ok, _}, write_to_file(P1, SessId(P1), f(<<"space3">>, File1), 0, crypto:strong_rand_bytes(10))),
    ?assertMatch({ok, _}, write_to_file(P2, SessId(P2), f(<<"space3">>, File2), 0, crypto:strong_rand_bytes(30))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P2, SessId(P2), f(<<"space3">>, File2), 10, crypto:strong_rand_bytes(10))),
    ?assertMatch({ok, _}, write_to_file(P1, SessId(P1), f(<<"space3">>, File1), 10, crypto:strong_rand_bytes(10))),

    ?assertMatch(30, current_size(P2, <<"space_id3">>)),
    ?assertMatch(20, current_size(P1, <<"space_id3">>)),

    ?assertMatch(-10, available_size(P2, <<"space_id3">>)),
    ?assertMatch(0, available_size(P1, <<"space_id3">>)).


remove_file_on_remote_provider_should_unlock_space(Config) ->
    #env{p1 = P1, p2 = P2, file1 = File1, file2 = File2} =
        gen_test_env(Config),
    SessId = fun(Worker) ->
        ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config) end,

    {ok, _} = create_file(P1, SessId(P1), f(<<"space3">>, File1)),
    {ok, _} = create_file(P1, SessId(P1), f(<<"space3">>, File2)),

    ?assertMatch({ok, _}, write_to_file(P1, SessId(P1), f(<<"space3">>, File1), 0, crypto:strong_rand_bytes(20))),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, SessId(P1), f(<<"space3">>, File2), 0, crypto:strong_rand_bytes(20))),

    ?assertMatch({ok, _}, lfm_proxy:stat(P2, SessId(P2), f(<<"space3">>, File1)), ?ATTEMPTS),
    ?assertMatch(ok, unlink(P2, SessId(P2), f(<<"space3">>, File1))),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(P1, SessId(P1), f(<<"space3">>, File1)), ?ATTEMPTS),
    ?assertMatch(0, current_size(P2, <<"space_id3">>), ?ATTEMPTS),
    ?assertMatch(0, current_size(P1, <<"space_id3">>), ?ATTEMPTS),
    ?assertMatch({ok, _}, write_to_file(P1, SessId(P1), f(<<"space3">>, File2), 0, crypto:strong_rand_bytes(20))).


replicate_file_smaller_than_quota_should_not_fail(Config) ->
    #env{p1 = P1, p2 = P2, file1 = File1} =
        gen_test_env(Config),
    SessId = fun(Worker) ->
        ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config) end,

    {ok, Guid} = create_file(P1, SessId(P1), f(<<"space3">>, File1)),
    ?assertMatch({ok, _}, write_to_file(P1, SessId(P1), f(<<"space3">>, File1), 0, crypto:strong_rand_bytes(20))),
    ?assertMatch({ok, [#{<<"totalBlocksSize">> := 20}]},
        lfm_proxy:get_file_distribution(P2, SessId(P2), {guid, Guid}), ?ATTEMPTS),

    {ok, Tid} = lfm_proxy:schedule_file_replication(P1, SessId(P1), {guid, Guid}, ?GET_DOMAIN_BIN(P2)),

    % wait for replication to finish
    ?assertMatch({ok, []}, rpc:call(P1, transfer, list_waiting_transfers, [<<"space_id3">>]), ?ATTEMPTS),
    ?assertMatch({ok, []}, rpc:call(P1, transfer, list_ongoing_transfers, [<<"space_id3">>]), ?ATTEMPTS),
    ?assertEqual(true, lists:member(Tid, list_ended_transfers(P1, <<"space_id3">>)), ?ATTEMPTS),

    ?assertMatch({ok, [#{<<"totalBlocksSize">> := 20}, #{<<"totalBlocksSize">> := 20}]},
        lfm_proxy:get_file_distribution(P2, SessId(P2), {guid, Guid}), ?ATTEMPTS),

    ok = fsync(P2, SessId(P2), f(<<"space3">>, File1)),
    ?assertEqual(20, current_size(P1, <<"space_id3">>), ?ATTEMPTS),
    ?assertEqual(20, current_size(P2, <<"space_id3">>), ?ATTEMPTS),

    ?assertMatch({ok, _}, lfm_proxy:stat(P2, SessId(P2), {guid, Guid}), ?ATTEMPTS),
    ?assertMatch(ok, unlink(P2, SessId(P2), f(<<"space3">>, File1))),

    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(P1, SessId(P1), {guid, Guid}), ?ATTEMPTS),
    ?assertEqual(0, current_size(P2, <<"space_id3">>)),
    ?assertEqual(0, current_size(P1, <<"space_id3">>)).


replicate_file_bigger_than_quota_should_fail(Config) ->
    #env{p1 = P1, p2 = P2, file1 = File1, file2 = File2} =
        gen_test_env(Config),
    SessId = fun(Worker) ->
        ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config) end,

    % How many bytes can be written above quota
    Tolerance = 100 * 1024 * 1024, % 100 MB
    MaxSize = 1000000000,

    {ok, Guid} = create_file(P1, SessId(P1), f(<<"space5">>, File1)),
    {ok, _} = create_file(P2, SessId(P2), f(<<"space5">>, File2)),
    ?assertMatch({ok, _}, write_to_file(P1, SessId(P1), f(<<"space5">>, File1), 0, crypto:strong_rand_bytes(MaxSize))),
    % Make sure that there is not enough space left in destination space
    ?assertMatch({ok, _}, write_to_file(P2, SessId(P2), f(<<"space5">>, File2), 0, crypto:strong_rand_bytes(Tolerance))),
    ?assertMatch({ok, [#{<<"totalBlocksSize">> := MaxSize}]},
        lfm_proxy:get_file_distribution(P2, SessId(P2), {guid, Guid}), ?ATTEMPTS),

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
    UserId = <<"user1">>,
    SessId = fun(Worker) -> ?config({session_id, {UserId, ?GET_DOMAIN(Worker)}}, Config) end,

    {ok, #file_attr{guid = DirGuid}} = lfm_proxy:stat(P1, SessId(P1), {path, <<"/space3">>}),
    {ok, FileGuid} = lfm_proxy:create(P1, SessId(P1), DirGuid, File1, 8#777),

    upload_file(P1, ?USER(UserId, SessId(P1)), 1, 20, 1, FileGuid),

    {ok, _} = lfm_proxy:stat(P1, SessId(P1), f(<<"space3">>, File1)),
    {ok, FileHandle} = lfm_proxy:open(P1, SessId(P1), f(<<"space3">>, File1), rdwr),
    ok = lfm_proxy:fsync(P1, FileHandle),

    ?assertEqual(20, current_size(P1, <<"space_id3">>)),
    ?assertEqual(0, current_size(P2, <<"space_id3">>)),

    {ok, _} = create_file(P1, SessId(P1), f(<<"space3">>, File2)),
    ?assertMatch({error, ?ENOSPC}, write_to_file(P1, SessId(P1), f(<<"space3">>, File2), 0, crypto:strong_rand_bytes(10))).


failed_gui_upload_test(Config) ->
    #env{p1 = P1, user1 = User1, file1 = File1, file2 = File2} =
        gen_test_env(Config),
    SessId = fun(Worker) -> ?config({session_id, {User1, ?GET_DOMAIN(Worker)}}, Config) end,

    {ok, #file_attr{guid = DirGuid}} = lfm_proxy:stat(P1, User1, {path, <<"/space4">>}),

    FileSize = 500*1024*1024, % 500 MB

    ProviderId = initializer:domain_to_provider_id(?GET_DOMAIN(P1)),
    % Upload File1 500MB to space4
    {ok, File1Guid} = lfm_proxy:create(P1, SessId(P1), DirGuid, File1, 8#777),
    upload_file(P1, ?USER(User1, SessId(P1)), 100, 1048576, 5, File1Guid),

    {ok, FileHandle} = lfm_proxy:open(P1, User1, f(<<"space4">>, File1), rdwr),
    ok = lfm_proxy:fsync(P1, FileHandle),
    ?assertMatch(FileSize, current_size(P1, <<"space_id4">>)),

    % Upload File2 800MB to space4
    {ok, File2Guid} = lfm_proxy:create(P1, SessId(P1), DirGuid, File2, 8#777),
    upload_file(P1, ?USER(User1, SessId(P1)), 160, 1048576, 5, File2Guid),
    ok = lfm_proxy:fsync(P1, User1, f(<<"space4">>, File2), ProviderId),

    StorageFilePath1 = storage_file_path(P1, <<"space_id4">>, File2),
    ?assertMatch(FileSize, current_size(P1, <<"space_id4">>)),
    ?assertMatch({error, ?ENOENT}, open_storage_file(P1, StorageFilePath1), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(P1, User1, f(<<"space4">>, File2)), ?ATTEMPTS).


events_sent_to_client_directio(Config) ->
    #env{p1 = P1} = gen_test_env(Config),
    events_sent_test_base(Config, <<"space_id1">>, P1).

events_sent_to_client_proxyio(Config) ->
    #env{p2 = P2} = gen_test_env(Config),
    events_sent_test_base(Config, <<"space_id2">>, P2).

events_sent_test_base(Config, SpaceId, SupportingProvider) ->
    #env{p1 = P1, file1 = Filename} = gen_test_env(Config),
    User = <<"user1">>,
    SessId = ?config({session_id, {User, ?GET_DOMAIN(P1)}}, Config),

    SpaceSize = available_size(SupportingProvider, SpaceId),
    RootGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),

    {ok, {Conn, SessId}} = fuse_test_utils:connect_as_user(Config, P1, User, [{active, true}]),
    SubId = rpc:call(P1, subscription,  generate_id, [<<"quota_exceeded">>]),
    rpc:call(P1, event, subscribe, [#subscription{id = SubId, type = #quota_exceeded_subscription{}}, SessId]),

    {FileGuid, _FileHandleId} = fuse_test_utils:create_file(Conn, RootGuid, Filename),

    ?assertMatch({ok, _}, lfm_proxy:stat(P1, SessId, {guid, FileGuid}), ?ATTEMPTS),
    ?assertMatch({ok, _}, write_to_file(P1, SessId, {guid, FileGuid}, 0, crypto:strong_rand_bytes(SpaceSize))),
    ?assertMatch(0, available_size(SupportingProvider, SpaceId), ?ATTEMPTS),

    ExpectedMessage = #'ServerMessage'{
        message_body = {events, #'Events'{
            events = [#'Event'{
                type = {quota_exceeded, #'QuotaExceededEvent'{
                    spaces = [SpaceId]
                }}
            }]
        }}
    },

    ?assert(verify_message_received(ExpectedMessage)).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) -> initializer:setup_storage(NewConfig) end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].

end_per_suite(Config) ->
    initializer:teardown_storage(Config).

init_per_testcase(Case, Config) when
    Case =:= events_sent_to_client_directio;
    Case =:= events_sent_to_client_proxyio
->
    ct:timetrap(timer:minutes(10)),
    initializer:remove_pending_messages(),
    ssl:start(),

    initializer:mock_auth_manager(Config),
    init_per_testcase(default, Config);

init_per_testcase(Case, Config) when
    Case =:= quota_updated_on_gui_upload;
    Case =:= failed_gui_upload_test
->
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
    Case =:= failed_upload_test
->
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

create_file(Worker, SessionId, {path, Path}) ->
    create_file(Worker, SessionId, Path);
create_file(Worker, SessionId, Path) ->
    lfm_proxy:create(Worker, SessionId, Path, ?DEFAULT_FILE_MODE).

open_file(Worker, SessionId, FileKey, OpenMode)->
    lfm_proxy:open(Worker, SessionId, FileKey, OpenMode).

write_to_file(Worker, SessionId, FileKey, Offset, Data) ->
    {ok, FileHandle} = open_file(Worker, SessionId, FileKey, write),
    Result = lfm_proxy:write(Worker, FileHandle, Offset, Data),
    lfm_proxy:fsync(Worker, FileHandle),
    timer:sleep(500), %% @todo: remove after fixing fsync
    lfm_proxy:close(Worker, FileHandle),
    Result.

mkdir(Worker, SessionId, {path, Path}) ->
    mkdir(Worker, SessionId, Path);
mkdir(Worker, SessionId, Path) ->
    lfm_proxy:mkdir(Worker, SessionId, Path).

rm_recursive(Worker, SessionId, FileKey) ->
    lfm_proxy:rm_recursive(Worker, SessionId, FileKey).

unlink(Worker, SessionId, FileKey) ->
    lfm_proxy:unlink(Worker, SessionId, FileKey).

truncate(Worker, SessionId, FileKey, Size) ->
    {ok, FileHandle} = open_file(Worker, SessionId, FileKey, write),
    Result = lfm_proxy:truncate(Worker, SessionId, FileKey, Size),
    lfm_proxy:fsync(Worker, FileHandle),
    timer:sleep(500), %% @todo: remove after fixing fsync
    lfm_proxy:close(Worker, FileHandle),
    Result.

rename(Worker, SessionId, FileKey, {path, Target}) ->
    rename(Worker, SessionId, FileKey, Target);
rename(Worker, SessionId, FileKey, Target) ->
    Result = lfm_proxy:mv(Worker, SessionId, FileKey, Target),
    fsync(Worker, SessionId, Target),
    Result.

fsync(Worker, SessionId, FileKey) ->
    case open_file(Worker, SessionId, FileKey, write) of
        {ok, FileHandle} ->
            Result = lfm_proxy:fsync(Worker, FileHandle),
            lfm_proxy:close(Worker, FileHandle),
            Result;
        _ ->
            ok
    end.

gen_test_env(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    Workers1 = lists:filter(fun(W) ->
        case re:run(atom_to_list(W), "p1") of
            nomatch -> false;
            _ -> true
        end
    end, Workers),
    Workers2 = Workers -- Workers1,
    P1 = lists:last(Workers1),
    P2 = lists:last(Workers2),
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
    P = filepath_utils:join([<<?DIRECTORY_SEPARATOR>>, Space, FileName]),
    {path, P}.

f(Space, Dirs, FileName) ->
    P = filepath_utils:join([<<?DIRECTORY_SEPARATOR>>, Space] ++ Dirs ++ [FileName]),
    {path, P}.

current_size(Worker, SpaceId) ->
    rpc:call(Worker, space_quota, current_size, [SpaceId]).

available_size(Worker, SpaceId) ->
    rpc:call(Worker, space_quota, available_size, [SpaceId]).

upload_file(Worker, ?USER(UserId) = Auth, PartsNumber, PartSize, ChunksNumber, FileGuid) ->
    ?assertMatch(
        {ok, _},
        rpc:call(Worker, gs_rpc, handle, [
            Auth, <<"initializeFileUpload">>, #{<<"guid">> => FileGuid}
        ])
    ),
    ?assertMatch(
        true,
        rpc:call(Worker, file_upload_manager, is_upload_registered, [UserId, FileGuid])
    ),

    do_multipart(Worker, Auth, PartsNumber, PartSize, ChunksNumber, FileGuid),

    ?assertMatch(
        {ok, _},
        rpc:call(Worker, gs_rpc, handle, [
            Auth, <<"finalizeFileUpload">>, #{<<"guid">> => FileGuid}
        ])
    ),
    ?assertMatch(
        false,
        rpc:call(Worker, file_upload_manager, is_upload_registered, [UserId, FileGuid]), ?ATTEMPTS
    ).

do_multipart(Worker, ?USER(UserId) = Auth, PartsNumber, PartSize, ChunksNumber, FileGuid) ->
    Params = #{
        <<"guid">> => FileGuid,
        <<"resumableChunkSize">> => integer_to_binary(PartsNumber*PartSize)
    },
    lists_utils:pforeach(fun(Chunk) ->
        rpc:call(Worker, page_file_upload, handle_multipart_req, [
            #{size => PartSize, left => PartsNumber},
            Auth,
            Params#{<<"resumableChunkNumber">> => integer_to_binary(Chunk)}
        ])
    end, lists:seq(1,ChunksNumber)).

open_storage_file(Worker, FilePath) ->
    rpc:call(Worker, file, open, [FilePath, read]).

storage_file_path(Worker, SpaceId, FilePath) ->
    SpaceMnt = get_space_mount_point(Worker, SpaceId),
    filename:join([SpaceMnt, SpaceId, FilePath]).

get_space_mount_point(Worker, SpaceId) ->
    StorageId = initializer:get_supporting_storage_id(Worker, SpaceId),
    storage_mount_point(Worker, StorageId).

storage_mount_point(Worker, StorageId) ->
    Helper = rpc:call(Worker, storage, get_helper, [StorageId]),
    HelperArgs = helper:get_args(Helper),
    maps:get(<<"mountPoint">>, HelperArgs).

list_ended_transfers(Worker, SpaceId) ->
    {ok, List} = rpc:call(Worker, transfer, list_ended_transfers, [SpaceId]),
    List.

verify_message_received(Message) ->
    case fuse_test_utils:receive_server_message([], timer:seconds(10)) of
        Message -> true;
        {error, timeout} -> false;
        _ -> verify_message_received(Message)
    end.