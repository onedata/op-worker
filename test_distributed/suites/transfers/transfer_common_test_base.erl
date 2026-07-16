%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains the base test functions common to all transfer
%%% types (replication, replica eviction and replica migration).
%%% @end
%%%-------------------------------------------------------------------
-module(transfer_common_test_base).
-author("Bartosz Walkowicz").

-include("transfer_test.hrl").
-include("onenv_test_utils.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/test/assertions.hrl").

% API
-export([
    init_per_testcase/3,
    end_per_testcase/3
]).

%% tests
-export([
    empty_dir_test/1,
    tree_of_empty_dirs_test/1,
    regular_file_test/1,
    file_in_directory_test/1,
    big_file_test/1,

    hundred_files_in_one_transfer_test/1,
    hundred_files_in_separate_transfers_test/1,

    transfer_despite_protection_flags_test/1
]).

-define(BIG_FILE_CHUNK_SIZE, 33554432).  % 32 MiB
-define(BIG_FILE_CHUNKS_COUNT, 32).
-define(BIG_FILE_SIZE, ?BIG_FILE_CHUNKS_COUNT * ?BIG_FILE_CHUNK_SIZE).  % 1 GiB


%%%===================================================================
%%% API
%%%===================================================================


init_per_testcase(Case, TestSuiteCtx, Config) ->
    NewConfig = lfm_proxy:init(Config),
    transfer_test_utils:remove_leftover_file_trees(TestSuiteCtx, Case),
    transfer_test_utils:remove_all_transfers(TestSuiteCtx),
    NewConfig.


end_per_testcase(_Case, _TestSuiteCtx, Config) ->
    lfm_proxy:teardown(Config).


%%%===================================================================
%%% Tests
%%%===================================================================


empty_dir_test(TestSuiteCtx) ->
    RootDir = transfer_test_utils:create_file_tree(TestSuiteCtx, ?FUNCTION_NAME, #dir_spec{}),
    transfer_test_utils:ensure_initial_replicas(TestSuiteCtx, RootDir),

    TransferId = transfer_test_utils:schedule_transfer(TestSuiteCtx, RootDir),
    transfer_test_utils:await_transfer_ended(TestSuiteCtx, TransferId, RootDir, #{}),
    transfer_test_utils:assert_distribution(TestSuiteCtx, RootDir).


tree_of_empty_dirs_test(TestSuiteCtx) ->
    % 3 levels of nested directories, 10 dirs on each level (1110 dirs overall)
    RootDir = transfer_test_utils:create_file_tree(TestSuiteCtx, ?FUNCTION_NAME, #dir_spec{
        children = transfer_test_utils:gen_nested_tree_spec([10, 10, 10, 0], <<>>)
    }),
    transfer_test_utils:ensure_initial_replicas(TestSuiteCtx, RootDir),

    TransferId = transfer_test_utils:schedule_transfer(TestSuiteCtx, RootDir),
    transfer_test_utils:await_transfer_ended(
        TestSuiteCtx, TransferId, RootDir, #{}, ?LARGE_TRANSFER_ATTEMPTS
    ),
    transfer_test_utils:assert_distribution(TestSuiteCtx, RootDir).


regular_file_test(TestSuiteCtx) ->
    RootDir = #object{children = [FileObject]} = transfer_test_utils:create_file_tree(
        TestSuiteCtx, ?FUNCTION_NAME,
        #dir_spec{children = [#file_spec{content = ?RAND_CONTENT()}]}
    ),
    transfer_test_utils:ensure_initial_replicas(TestSuiteCtx, RootDir),

    TransferId = transfer_test_utils:schedule_transfer(TestSuiteCtx, FileObject),
    transfer_test_utils:await_transfer_ended(TestSuiteCtx, TransferId, FileObject, #{}),
    transfer_test_utils:assert_distribution(TestSuiteCtx, FileObject).


file_in_directory_test(TestSuiteCtx) ->
    RootDir = transfer_test_utils:create_file_tree(
        TestSuiteCtx, ?FUNCTION_NAME,
        #dir_spec{children = [#file_spec{content = ?RAND_CONTENT()}]}
    ),
    transfer_test_utils:ensure_initial_replicas(TestSuiteCtx, RootDir),

    TransferId = transfer_test_utils:schedule_transfer(TestSuiteCtx, RootDir),
    transfer_test_utils:await_transfer_ended(TestSuiteCtx, TransferId, RootDir, #{}),
    transfer_test_utils:assert_distribution(TestSuiteCtx, RootDir).


big_file_test(TestSuiteCtx = #transfer_test_suite_ctx{
    user_selector = UserSelector,
    creation_provider_selector = CreationProviderSelector,
    other_provider_selector = OtherProviderSelector
}) ->
    #object{children = [FileObject = #object{guid = FileGuid}]} = transfer_test_utils:create_file_tree(
        TestSuiteCtx, ?FUNCTION_NAME, #dir_spec{children = [#file_spec{}]}
    ),

    % write the content in chunks - declaring (or writing) it as a single
    % binary of this size would be prohibitively memory-heavy
    CreationNode = oct_background:get_random_provider_node(CreationProviderSelector),
    SessionId = oct_background:get_user_session_id(UserSelector, CreationProviderSelector),
    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(
        CreationNode, SessionId, ?FILE_REF(FileGuid), write
    )),
    lists:foreach(fun(ChunkNum) ->
        ?assertMatch({ok, _}, lfm_proxy:write(
            CreationNode, Handle, ChunkNum * ?BIG_FILE_CHUNK_SIZE, ?RAND_CONTENT(?BIG_FILE_CHUNK_SIZE)
        ))
    end, lists:seq(0, ?BIG_FILE_CHUNKS_COUNT - 1)),
    ok = lfm_proxy:close(CreationNode, Handle),

    OtherNode = oct_background:get_random_provider_node(OtherProviderSelector),
    file_test_utils:await_size(OtherNode, FileGuid, ?BIG_FILE_SIZE),

    transfer_test_utils:ensure_initial_replicas(TestSuiteCtx, FileObject),

    TransferId = transfer_test_utils:schedule_transfer(TestSuiteCtx, FileObject),
    transfer_test_utils:await_transfer_ended(TestSuiteCtx, TransferId, FileObject, #{
        % the content is written after the tree creation, so it is invisible
        % in the declared file tree - adjust the derivation input accordingly
        tree_bytes => ?BIG_FILE_SIZE,
        % the transfer may outlast the minute histogram window, rolling the
        % early bytes out of it
        min_hist => skip
    }, ?LARGE_TRANSFER_ATTEMPTS),

    transfer_test_utils:assert_distribution(
        TestSuiteCtx, FileObject, #{FileGuid => ?BIG_FILE_SIZE}
    ).


hundred_files_in_one_transfer_test(TestSuiteCtx) ->
    % 10 directories with 10 files each
    RootDir = transfer_test_utils:create_file_tree(TestSuiteCtx, ?FUNCTION_NAME, #dir_spec{
        children = transfer_test_utils:gen_nested_tree_spec([10, 10], ?RAND_CONTENT())
    }),
    transfer_test_utils:ensure_initial_replicas(TestSuiteCtx, RootDir),

    TransferId = transfer_test_utils:schedule_transfer(TestSuiteCtx, RootDir),
    transfer_test_utils:await_transfer_ended(TestSuiteCtx, TransferId, RootDir, #{
        % a transfer of this many files may outlast the minute histogram window
        min_hist => skip
    }, ?SCALE_TRANSFER_ATTEMPTS),
    transfer_test_utils:assert_distribution(TestSuiteCtx, RootDir).


hundred_files_in_separate_transfers_test(TestSuiteCtx) ->
    RootDir = #object{children = FileObjects} = transfer_test_utils:create_file_tree(
        TestSuiteCtx, ?FUNCTION_NAME, #dir_spec{
            children = transfer_test_utils:gen_nested_tree_spec([100], ?RAND_CONTENT())
        }
    ),
    transfer_test_utils:ensure_initial_replicas(TestSuiteCtx, RootDir),

    TransferIdsAndFiles = lists:map(fun(FileObject) ->
        {transfer_test_utils:schedule_transfer(TestSuiteCtx, FileObject), FileObject}
    end, FileObjects),

    %% TODO list_utils:pmap/pforeach ?
    lists:foreach(fun({TransferId, FileObject}) ->
        transfer_test_utils:await_transfer_ended(
            TestSuiteCtx, TransferId, FileObject, #{}, ?SCALE_TRANSFER_ATTEMPTS
        )
    end, TransferIdsAndFiles),
    transfer_test_utils:assert_distribution(TestSuiteCtx, RootDir).


transfer_despite_protection_flags_test(TestSuiteCtx) ->
    RootDir = transfer_test_utils:create_file_tree(TestSuiteCtx, ?FUNCTION_NAME, #dir_spec{
        children = [
            #dir_spec{
                dataset = #dataset_spec{
                    protection_flags = [?DATA_PROTECTION_BIN, ?METADATA_PROTECTION_BIN]
                },
                children = transfer_test_utils:gen_nested_tree_spec([10], ?RAND_CONTENT())
            }
            | transfer_test_utils:gen_nested_tree_spec([10], ?RAND_CONTENT())
        ]
    }),
    transfer_test_utils:ensure_initial_replicas(TestSuiteCtx, RootDir),

    TransferId = transfer_test_utils:schedule_transfer(TestSuiteCtx, RootDir),
    transfer_test_utils:await_transfer_ended(TestSuiteCtx, TransferId, RootDir, #{
        % a transfer of this many files may outlast the minute histogram window
        min_hist => skip
    }, ?SCALE_TRANSFER_ATTEMPTS),
    transfer_test_utils:assert_distribution(TestSuiteCtx, RootDir).
