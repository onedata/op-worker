%%%--------------------------------------------------------------------
%%% @author Michal Wrona
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module tests file deletion.
%%% @end
%%%--------------------------------------------------------------------
-module(storage_sync_test_SUITE).
-author("Michal Wrona").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    simple_file_import_test/1
]).

-define(TEST_CASES, [
    simple_file_import_test
]).

all() -> ?ALL(?TEST_CASES).

-define(FILE_UUID, <<"file_uuid">>).
-define(SESSION_ID_1, <<"session_id_1">>).
-define(SESSION_ID_2, <<"session_id_2">>).

-define(req(W, P), rpc:call(W, worker_proxy, call, [file_deletion_worker, P])).


%%%===================================================================
%%% Test functions
%%%===================================================================

simple_file_import_test(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),

    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W1)}}, Config),

    tracer:trace_calls(luma_provider, get_or_create_user),

    {ok, _} = lfm_proxy:create(W1, SessId, <<"/space_name1/test1">>, 8#777),
    {ok, _} = lfm_proxy:create(W1, SessId, <<"/space_name1/test2">>, 8#777),
    {ok, _} = lfm_proxy:create(W1, SessId, <<"/space_name1/test3">>, 8#777),
    {ok, _} = lfm_proxy:create(W1, SessId, <<"/space_name1/test4">>, 8#777),

    {ok, _} = lfm_proxy:mkdir(W1, SessId, <<"/space_name1/dir1">>),
    {ok, _} = lfm_proxy:mkdir(W1, SessId, <<"/space_name1/dir2">>),
    {ok, _} = lfm_proxy:mkdir(W1, SessId, <<"/space_name1/dir1/dir1">>),

    {ok, _} = lfm_proxy:create(W1, SessId, <<"/space_name1/dir1/test1">>, 8#777),
    {ok, _} = lfm_proxy:create(W1, SessId, <<"/space_name1/dir1/test2">>, 8#777),
    {ok, _} = lfm_proxy:create(W1, SessId, <<"/space_name1/dir1/test3">>, 8#777),
    {ok, _} = lfm_proxy:create(W1, SessId, <<"/space_name1/dir1/test4">>, 8#777),

    {ok, _} = lfm_proxy:create(W1, SessId, <<"/space_name1/dir2/test1">>, 8#777),
    {ok, _} = lfm_proxy:create(W1, SessId, <<"/space_name1/dir2/test2">>, 8#777),
    {ok, _} = lfm_proxy:create(W1, SessId, <<"/space_name1/dir2/test3">>, 8#777),
    {ok, _} = lfm_proxy:create(W1, SessId, <<"/space_name1/dir2/test4">>, 8#777),

    {ok, _} = lfm_proxy:create(W1, SessId, <<"/space_name1/dir1/dir1/test1">>, 8#777),
    {ok, _} = lfm_proxy:create(W1, SessId, <<"/space_name1/dir1/dir1/test2">>, 8#777),
    {ok, _} = lfm_proxy:create(W1, SessId, <<"/space_name1/dir1/dir1/test3">>, 8#777),
    {ok, _} = lfm_proxy:create(W1, SessId, <<"/space_name1/dir1/dir1/test4">>, 8#777),


    ct:print("SessId ~p", [SessId]),
    timer:sleep(timer:minutes(1)),

    ok.


%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json"), [initializer]).

end_per_suite(Config) ->
    ?TEST_STOP(Config).

init_per_testcase(Case, Config) ->
    ?CASE_START(Case),
    ct:timetrap({minutes, 60}),
    application:start(etls),
    hackney:start(),
%%    initializer:enable_grpca_based_communication(Config),
    initializer:disable_quota_limit(Config),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    initializer:enable_grpca_based_communication(Config),
    lfm_proxy:init(ConfigWithSessionInfo).

end_per_testcase(Case, Config) ->
    ?CASE_STOP(Case),
    lfm_proxy:teardown(Config),
    %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:unload_quota_mocks(Config),
%%    initializer:disable_grpca_based_communication(Config),
    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================

