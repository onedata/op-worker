%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2018-2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of the lifecycle of a file - its creation, the handles held to it and
%%% its deletion - seen from the inside of the provider, down to what each step
%%% leaves on the storage. This module holds only the wiring; every test body
%%% lives in the file_*_tests module for the aspect its group covers.
%%%
%%% Every case runs in a space of its own, backed by a freshly created storage,
%%% which lets the storage contents be asserted in absolute terms. The space is
%%% named after the test case and deliberately left behind for post-mortem
%%% inspection - the next run of the suite cleans it up.
%%% @end
%%%-------------------------------------------------------------------
-module(file_lifecycle_test_SUITE).
-author("Michal Wrzeszcz").

-include("env/space_setup_utils.hrl").
-include("file/file_lifecycle_test.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

%% export for ct
-export([
    groups/0, all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    concurrent_opens_create_one_storage_file_test/1,
    open_during_create_test/1,
    open_before_file_doc_is_saved_test/1,
    open_during_create_and_open_test/1,
    open_before_creation_times_are_reported_test/1,
    cancelled_create_leaves_no_storage_file_test/1,
    create_file_existing_on_disk_test/1,

    delete_during_open_with_deletion_marker_test/1,
    delete_during_open_with_storage_rename_test/1,
    delete_of_opened_file_moves_it_on_storage_test/1,
    name_of_deleted_opened_file_can_be_reused_test/1,
    release_before_deleted_file_is_moved_on_storage_test/1,
    release_after_deleted_file_is_moved_on_storage_test/1,
    rename_to_opened_file_test/1
]).

groups() -> [
    {creation_tests, [], [
        concurrent_opens_create_one_storage_file_test,
        open_during_create_test,
        open_before_file_doc_is_saved_test,
        open_during_create_and_open_test,
        open_before_creation_times_are_reported_test,
        cancelled_create_leaves_no_storage_file_test
        %%    create_file_existing_on_disk_test % TODO VFS-5271
    ]},

    {deletion_tests, [], [
        delete_during_open_with_deletion_marker_test,
        delete_during_open_with_storage_rename_test,
        delete_of_opened_file_moves_it_on_storage_test,
        name_of_deleted_opened_file_can_be_reused_test,
        release_before_deleted_file_is_moved_on_storage_test,
        release_after_deleted_file_is_moved_on_storage_test
        %%    rename_to_opened_file_test % TODO VFS-5290
    ]}
].

all() -> [
    {group, creation_tests},
    {group, deletion_tests}
].

% modules mocked by the test cases; unloaded after every one of them regardless
% of which ones it actually used
-define(MOCKED_MODULES, [
    file_meta, file_req, fslogic_delete, fslogic_event_emitter, sd_utils,
    storage_driver, times_api
]).

% Test cases whose subject is how the Oneprovider copes with a storage that
% cannot rename a file; every other case gets a POSIX storage, which is what the
% assertions on the storage contents by file name require.
-define(OBJECT_STORAGE_CASES, [
    delete_during_open_with_deletion_marker_test
]).


%%%====================================================================
%%% Creation tests
%%%====================================================================


concurrent_opens_create_one_storage_file_test(Config) ->
    ?RUN_CREATION_TEST(Config).


open_during_create_test(Config) ->
    ?RUN_CREATION_TEST(Config).


open_before_file_doc_is_saved_test(Config) ->
    ?RUN_CREATION_TEST(Config).


open_during_create_and_open_test(Config) ->
    ?RUN_CREATION_TEST(Config).


open_before_creation_times_are_reported_test(Config) ->
    ?RUN_CREATION_TEST(Config).


cancelled_create_leaves_no_storage_file_test(Config) ->
    ?RUN_CREATION_TEST(Config).


create_file_existing_on_disk_test(Config) ->
    ?RUN_CREATION_TEST(Config).


%%%====================================================================
%%% Deletion tests
%%%====================================================================


delete_during_open_with_deletion_marker_test(Config) ->
    ?RUN_DELETION_TEST(Config).


delete_during_open_with_storage_rename_test(Config) ->
    ?RUN_DELETION_TEST(Config).


delete_of_opened_file_moves_it_on_storage_test(Config) ->
    ?RUN_DELETION_TEST(Config).


name_of_deleted_opened_file_can_be_reused_test(Config) ->
    ?RUN_DELETION_TEST(Config).


release_before_deleted_file_is_moved_on_storage_test(Config) ->
    ?RUN_DELETION_TEST(Config).


release_after_deleted_file_is_moved_on_storage_test(Config) ->
    ?RUN_DELETION_TEST(Config).


rename_to_opened_file_test(Config) ->
    ?RUN_DELETION_TEST(Config).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    % NOTE: every module whose code runs on the provider node must be listed - the
    % test modules because their mocks ship functions there, and
    % storage_file_tree_test_utils because it dispatches to itself over rpc
    LoadModules = [file_creation_tests, file_deletion_tests, storage_file_tree_test_utils],

    opt:init_per_suite([{?LOAD_MODULES, LoadModules} | Config], #onenv_test_config{
        % NOTE: the scenario is needed for the s3 volume it deploys, not for the
        % space it sets up - every case builds a storage and a space of its own
        onenv_scenario = "1op_s3",
        envs = [{op_worker, op_worker, [
            {fuse_session_grace_period_seconds, 24 * 60 * 60}
        ]}],
        posthook = fun(NewConfig) ->
            space_setup_utils:clean_up_after_previous_run(
                all_test_cases(), [?PROVIDER_SELECTOR]
            ),
            NewConfig
        end
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_testcase(Case, Config) ->
    ct:timetrap({minutes, 5}),
    SpaceId = space_setup_utils:set_up_space(#space_spec{
        name = Case,
        owner = ?USER_SELECTOR,
        supports = [#support_spec{
            provider = ?PROVIDER_SELECTOR,
            storage_spec = create_storage_for(Case),
            size = 1073741824
        }]
    }),
    lfm_proxy:init([{space_id, SpaceId} | Config]).


end_per_testcase(_Case, Config) ->
    Node = file_lifecycle_test_utils:get_node(),
    % release the handles while the mocks are still in place - the release path
    % goes through some of the mocked modules
    ?assertEqual(ok, lfm_proxy:close_all(Node)),
    ok = test_utils:mock_unload(Node, ?MOCKED_MODULES),
    lfm_proxy:teardown(Config).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec create_storage_for(atom()) -> storage:id().
create_storage_for(Case) ->
    case lists:member(Case, ?OBJECT_STORAGE_CASES) of
        true -> file_lifecycle_test_utils:create_s3_storage();
        false -> file_lifecycle_test_utils:create_posix_storage()
    end.


%% @private
%% @doc
%% Names of all the test cases wired into the suite. NOTE: this must stay a flat
%% list of case names - space_setup_utils:clean_up_after_previous_run/2 matches
%% the spaces left behind by their names, so feeding it all/0, which yields group
%% references, would silently stop cleaning anything up.
%% @end
-spec all_test_cases() -> [atom()].
all_test_cases() ->
    lists:flatmap(fun({_GroupName, _Opts, Cases}) -> Cases end, groups()).
