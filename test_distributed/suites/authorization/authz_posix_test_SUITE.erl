%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020-2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This test suite verifies correct behaviour of authorization mechanism
%%% with corresponding lfm (logical_file_manager) functions and posix storage.
%%% @end
%%%-------------------------------------------------------------------
-module(authz_posix_test_SUITE).
-author("Bartosz Walkowicz").

-include("modules/logical_file_manager/lfm.hrl").
-include("permissions_test.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    mkdir_test/1
]).

all() -> [
    mkdir_test
].


%%%===================================================================
%%% Test functions
%%%===================================================================


mkdir_test(_Config) ->
    authz_test_runner:run_suite(#authz_test_suite_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        files = [#dir{
            name = <<"dir1">>,
            perms = [?traverse_container, ?add_subcontainer]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        available_in_open_handle_mode = false,
        operation = fun(Node, SessionId, TestCaseRootDirPath, ExtraData) ->
            ParentDirPath = <<TestCaseRootDirPath/binary, "/dir1">>,
            ?FILE_REF(ParentDirGuid) = maps:get(ParentDirPath, ExtraData),
            case lfm_proxy:mkdir(Node, SessionId, ParentDirGuid, <<"dir2">>, 8#777) of
                {ok, DirGuid} ->
                    permissions_test_utils:ensure_dir_created_on_storage(Node, DirGuid);
                {error, _} = Error ->
                    Error
            end
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_change_ownership, <<TestCaseRootDirPath/binary, "/dir1/dir2">>}
        end
    }).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    ModulesToLoad = [?MODULE, authz_test_runner],
    oct_background:init_per_suite([{?LOAD_MODULES, ModulesToLoad} | Config], #onenv_test_config{
        onenv_scenario = "1op",
        envs = [{op_worker, op_worker, [
            {fuse_session_grace_period_seconds, 24 * 60 * 60}
        ]}],
        posthook = fun(NewConfig) ->
            % clean space
            lists:foreach(fun(SpaceSelector) ->
                {ok, FileEntries} = onenv_file_test_utils:ls(user1, SpaceSelector, 0, 10000),

                lists_utils:pforeach(fun({Guid, _}) ->
                    onenv_file_test_utils:rm_and_sync_file(user1, Guid)
                end, FileEntries)
            end, [space_krk]),

            NewConfig
        end
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_testcase(_Case, Config) ->
    lfm_proxy:init(Config).


end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).
