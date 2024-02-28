%%%--------------------------------------------------------------------
%%% @author Katarzyna Such
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module tests storage import.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_import_oct_test_SUITE).
-author("Katarzyna Such").

-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").
-include_lib("space_setup_utils.hrl").
-include_lib("storage_import_oct_test.hrl").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    empty_import_test/1,
    create_directory_import_test/1
]).

-define(TEST_CASES, [
    empty_import_test,
    create_directory_import_test
]).

-define(RANDOM_PROVIDER(), ?RAND_ELEMENT([krakow, paris])).

all() -> ?ALL(?TEST_CASES).

-define(RUN_TEST(Config),
    storage_import_oct_test_base:?FUNCTION_NAME(?config(storage_import_test_config, Config))
).

%%%==================================================================
%%% Test functions
%%%===================================================================

empty_import_test(Config) ->
    ?RUN_TEST(Config).


create_directory_import_test(Config) ->
    ?RUN_TEST(Config).

%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    ModulesToLoad = [?MODULE, sd_test_utils, storage_import_oct_test_base],
    oct_background:init_per_suite([{?LOAD_MODULES, ModulesToLoad} | Config], #onenv_test_config{
        onenv_scenario = "2op",
        envs = [{op_worker, op_worker, [
            {fuse_session_grace_period_seconds, 24 * 60 * 60},
            {dbsync_changes_broadcast_interval, timer:seconds(1)},
            {datastore_links_tree_order, 100},
            {cache_to_disk_delay_ms, timer:seconds(1)},
            {cache_to_disk_force_delay_ms, timer:seconds(2)}
        ]}],
        posthook = fun(NewConfig) ->
            delete_spaces_from_previous_run(),
            NewConfig
        end
    }).


%% @private
-spec delete_spaces_from_previous_run() -> ok.
delete_spaces_from_previous_run() ->
    AllTestCases = all(),
    RemovedSpaces = lists:filter(fun(SpaceId) ->
        SpaceDetails = ozw_test_rpc:get_space_protected_data(?ROOT, SpaceId),
        SpaceName = maps:get(<<"name">>, SpaceDetails),
        Exists = lists:member(binary_to_atom(SpaceName), AllTestCases),
        Exists andalso ozw_test_rpc:delete_space(SpaceId),

        Exists
    end, ozw_test_rpc:list_spaces()),

    ?assertEqual([], lists_utils:intersect(opw_test_rpc:get_spaces(krakow), RemovedSpaces), ?ATTEMPTS),

    ok.


end_per_suite(_Config) ->
    oct_background:end_per_suite().

init_per_testcase(Case, Config) ->
    ImportedStorageId = space_setup_utils:create_storage(
        krakow,
        #posix_storage_params{
            mount_point = <<"/mnt/synced_storage", (generator:gen_name())/binary>>,
            imported_storage = true
        }
    ),
    NotImportedStorageId = space_setup_utils:create_storage(
        paris,
        #posix_storage_params{mount_point = <<"/mnt/st2", (generator:gen_name())/binary>>}
    ),
    SpaceId = space_setup_utils:set_up_space(
        #space_spec{name = Case, owner = user1, users = [user2], supports = [
            #support_spec{
                provider = krakow,
                storage_spec = ImportedStorageId,
                size = 10000000000
            },
            #support_spec{
                provider = paris,
                storage_spec = NotImportedStorageId,
                size = 10000000000
            }
    ]}),
    [{storage_import_test_config, #storage_import_test_config{
        space_id = SpaceId,
        imported_storage_id = ImportedStorageId,
        not_imported_storage_id = NotImportedStorageId
    }} | Config].


end_per_testcase(_Case, _Config) ->
    ok.
