%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2018-2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of what the creation and the deletion of a file leave on the storage
%%% when something else happens at the same time - a second open, a delete of a
%%% file still held open, a handle released mid-deletion, a node restarted with
%%% files open. This module holds only the wiring; every test body lives in the
%%% file_lifecycle_*_tests module for the aspect its group covers.
%%%
%%% Not every case here is a race. The deletion_procedure_tests group drives the
%%% steps of the deletion procedure directly, with nothing running against them,
%%% to pin down what each step answers for on its own.
%%%
%%% The lfm API surface itself - what each operation returns, across the breadth
%%% of the API and on either storage backend - belongs to
%%% file_lfm_posix_test_SUITE and file_lfm_s3_test_SUITE, not here.
%%%
%%% Every case runs in a space of its own, backed by a freshly created storage,
%%% which lets the storage contents be asserted in absolute terms. The space is
%%% named after the test case and deliberately left behind for post-mortem
%%% inspection - the next run of the suite cleans it up.
%%% @end
%%%-------------------------------------------------------------------
-module(file_lifecycle_races_test_SUITE).
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

    counting_file_open_and_release_test/1,
    session_deletion_releases_its_open_files_test/1,

    delete_during_open_with_deletion_marker_test/1,
    delete_during_open_with_storage_rename_test/1,
    delete_of_opened_file_moves_it_on_storage_test/1,
    name_of_deleted_opened_file_can_be_reused_test/1,
    release_before_deleted_file_is_moved_on_storage_test/1,
    release_after_deleted_file_is_moved_on_storage_test/1,
    content_of_deleted_opened_file_survives_name_takeover_test/1,
    content_of_deleted_opened_file_survives_name_takeover_on_object_storage_test/1,
    delete_of_newer_generation_first_leaves_older_on_storage_test/1,
    delete_of_older_generation_first_leaves_newer_on_storage_test/1,
    delete_via_fuse_removes_object_from_storage_test/1,
    node_restart_deletes_open_files_marked_for_removal_test/1,
    node_restart_deletes_open_files_with_no_storage_file_test/1,
    release_of_deleted_file_removes_it_from_storage_test/1,
    release_of_deleted_file_with_no_storage_file_test/1,
    delete_of_not_opened_file_removes_it_from_storage_test/1,
    delete_of_not_opened_file_with_no_storage_file_test/1,
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

    % NOTE: named after the file_handles model rather than after the lfm handle -
    % these cases work on the provider's registry of open files, not on what an
    % lfm open hands back (that is file_lfm_{posix,s3}_test_SUITE's handles_tests)
    {open_file_registry_tests, [], [
        counting_file_open_and_release_test,
        session_deletion_releases_its_open_files_test
    ]},

    {deletion_of_open_file_tests, [], [
        delete_during_open_with_deletion_marker_test,
        delete_during_open_with_storage_rename_test,
        delete_of_opened_file_moves_it_on_storage_test,
        name_of_deleted_opened_file_can_be_reused_test,
        release_before_deleted_file_is_moved_on_storage_test,
        release_after_deleted_file_is_moved_on_storage_test,
        content_of_deleted_opened_file_survives_name_takeover_test,
        content_of_deleted_opened_file_survives_name_takeover_on_object_storage_test,
        delete_of_newer_generation_first_leaves_older_on_storage_test,
        delete_of_older_generation_first_leaves_newer_on_storage_test
        %%    rename_to_opened_file_test % TODO VFS-5290
    ]},

    % NOTE: unlike the group above, nothing races the deletion here - these cases
    % ask for it over the fuse protocol or drive the steps of the deletion
    % procedure directly, and check what each of them leaves on the storage
    {deletion_procedure_tests, [], [
        delete_via_fuse_removes_object_from_storage_test,
        node_restart_deletes_open_files_marked_for_removal_test,
        node_restart_deletes_open_files_with_no_storage_file_test,
        release_of_deleted_file_removes_it_from_storage_test,
        release_of_deleted_file_with_no_storage_file_test,
        delete_of_not_opened_file_removes_it_from_storage_test,
        delete_of_not_opened_file_with_no_storage_file_test
    ]}
].

all() -> [
    {group, creation_tests},
    {group, open_file_registry_tests},
    {group, deletion_of_open_file_tests},
    {group, deletion_procedure_tests}
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
    delete_during_open_with_deletion_marker_test,
    content_of_deleted_opened_file_survives_name_takeover_on_object_storage_test,
    delete_via_fuse_removes_object_from_storage_test
]).

% Test cases working on the file_handles model directly, on a file that exists
% nowhere else (see file_lifecycle_handles_tests) - they need neither a space nor
% a storage, only fuse sessions of their own, which they clean up after instead.
-define(CASES_WITHOUT_A_SPACE, [
    counting_file_open_and_release_test,
    session_deletion_releases_its_open_files_test
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
%%% Handles tests
%%%====================================================================


counting_file_open_and_release_test(Config) ->
    ?RUN_HANDLES_TEST(Config).


session_deletion_releases_its_open_files_test(Config) ->
    ?RUN_HANDLES_TEST(Config).


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


content_of_deleted_opened_file_survives_name_takeover_test(Config) ->
    ?RUN_DELETION_TEST(Config).


content_of_deleted_opened_file_survives_name_takeover_on_object_storage_test(Config) ->
    ?RUN_DELETION_TEST(Config).


delete_of_newer_generation_first_leaves_older_on_storage_test(Config) ->
    ?RUN_DELETION_TEST(Config).


delete_of_older_generation_first_leaves_newer_on_storage_test(Config) ->
    ?RUN_DELETION_TEST(Config).


delete_via_fuse_removes_object_from_storage_test(Config) ->
    ?RUN_DELETION_TEST(Config).


node_restart_deletes_open_files_marked_for_removal_test(Config) ->
    ?RUN_DELETION_TEST(Config).


node_restart_deletes_open_files_with_no_storage_file_test(Config) ->
    ?RUN_DELETION_TEST(Config).


release_of_deleted_file_removes_it_from_storage_test(Config) ->
    ?RUN_DELETION_TEST(Config).


release_of_deleted_file_with_no_storage_file_test(Config) ->
    ?RUN_DELETION_TEST(Config).


delete_of_not_opened_file_removes_it_from_storage_test(Config) ->
    ?RUN_DELETION_TEST(Config).


delete_of_not_opened_file_with_no_storage_file_test(Config) ->
    ?RUN_DELETION_TEST(Config).


rename_to_opened_file_test(Config) ->
    ?RUN_DELETION_TEST(Config).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    % NOTE: every module whose code runs on the provider node must be listed - the
    % test modules because their mocks ship functions there, and
    % storage_file_tree_test_utils and fuse_test_utils because they dispatch to
    % themselves over rpc
    LoadModules = [
        file_lifecycle_creation_tests, file_lifecycle_handles_tests, file_lifecycle_deletion_tests,
        storage_file_tree_test_utils, fuse_test_utils
    ],

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

    case lists:member(Case, ?CASES_WITHOUT_A_SPACE) of
        true ->
            Config;
        false ->
            SpaceId = space_setup_utils:set_up_space(#space_spec{
                name = Case,
                owner = ?USER_SELECTOR,
                supports = [#support_spec{
                    provider = ?PROVIDER_SELECTOR,
                    storage_spec = create_storage_for(Case),
                    size = 1073741824
                }]
            }),
            lfm_proxy:init([{space_id, SpaceId} | Config])
    end.


end_per_testcase(Case, Config) ->
    Node = file_lifecycle_test_utils:get_node(),

    % whatever is undone here must be undone while the mocks are still in place -
    % both the release of a handle and the deletion of a session go through them
    case lists:member(Case, ?CASES_WITHOUT_A_SPACE) of
        true -> file_lifecycle_handles_tests:clean_up(Node);
        false -> ?assertEqual(ok, lfm_proxy:close_all(Node))
    end,
    ok = test_utils:mock_unload(Node, ?MOCKED_MODULES),

    % NOTE: safe to call even for the cases that never started the proxy
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
