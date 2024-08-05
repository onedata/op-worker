%%%--------------------------------------------------------------------
%%% @author Katarzyna Such
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module tests storage import on S3 storage.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_import_s3_oct_test_SUITE).
-author("Katarzyna Such").

-include("storage_import_oct_test.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    import_empty_storage_test/1,
    import_empty_file_test/1,
    import_file_with_content_test/1
]).

all() -> [
    import_empty_storage_test,
    import_empty_file_test,
    import_file_with_content_test
].

-define(IMPORTING_PROVIDER_SELECTOR, krakow).

-define(MOCK_SPACE_DIR_STATBUF, #statbuf{
    st_uid = ?ROOT_UID,
    st_gid = ?ROOT_GID,
    st_mode = ?DEFAULT_DIR_PERMS bor 8#40000,
    % Return min times to ensure space root dir will not be considered
    % as modified by storage import
    st_mtime = 1,
    st_atime = 1,
    st_ctime = 1,
    st_size = 0
}).

-define(SUITE_CTX, #storage_import_test_suite_ctx{
    storage_type = s3,
    importing_provider_selector = ?IMPORTING_PROVIDER_SELECTOR,
    non_importing_provider_selector = paris,
    space_owner_selector = space_owner
}).
-define(run_test(), storage_import_oct_test_base:?FUNCTION_NAME(?SUITE_CTX)).


%%%==================================================================
%%% Test functions
%%%===================================================================


import_empty_storage_test(_Config) ->
    ?run_test().


import_empty_file_test(_Config) ->
    ?run_test().


import_file_with_content_test(_Config) ->
    ?run_test().


%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    ModulesToLoad = [?MODULE, sd_test_utils, storage_import_oct_test_base],
    oct_background:init_per_suite([{?LOAD_MODULES, ModulesToLoad} | Config], #onenv_test_config{
        onenv_scenario = "2op_s3",
        envs = [{op_worker, op_worker, [
            {fuse_session_grace_period_seconds, 24 * 60 * 60},
            {datastore_links_tree_order, 100},
            {cache_to_disk_delay_ms, timer:seconds(1)},
            {cache_to_disk_force_delay_ms, timer:seconds(2)}
        ]}],
        posthook = fun(NewConfig) ->
            storage_import_oct_test_base:clean_up_after_previous_run(all(), ?SUITE_CTX),

            % Space root dir is emulated for flat storages (object storages that
            % have no concept of directories - see flat_storage_iterator.erl).
            % It's time stats are always set to current time. This may cause
            % storage import tests to fail as sometimes space root dir will be
            % reported as modified (if import started later than times doc was created)
            % or unmodified (if imported started in the same second as time doc
            % was created). To run tests deterministically, space root dir time
            % stats are mocked to be in the past.
            Nodes = oct_background:get_provider_nodes(?IMPORTING_PROVIDER_SELECTOR),
            ok = test_utils:mock_new(Nodes, storage_file_ctx),
            ok = test_utils:mock_expect(Nodes, storage_file_ctx, new_with_stat,
                fun
                    (StorageFileId = <<"/">>, SpaceId, StorageId, _Stat) ->
                        storage_file_ctx:new_with_stat(
                            StorageFileId, <<>>, SpaceId, StorageId, ?MOCK_SPACE_DIR_STATBUF
                        );
                    (StorageFileId, SpaceId, StorageId, Stat) ->
                        meck:passthrough([StorageFileId, SpaceId, StorageId, Stat])
                end
            ),

            NewConfig
        end
    }).


end_per_suite(_Config) ->
    Nodes = oct_background:get_provider_nodes(?IMPORTING_PROVIDER_SELECTOR),
    ok = test_utils:mock_unload(Nodes, storage_file_ctx),

    oct_background:end_per_suite().


init_per_testcase(_Case, Config) ->
    storage_import_oct_test_base:init_per_testcase(Config).


end_per_testcase(Case, Config) ->
    storage_import_oct_test_base:end_per_testcase(Case, ?SUITE_CTX, Config).
