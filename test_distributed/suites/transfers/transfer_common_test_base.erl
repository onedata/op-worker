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
-include("modules/datastore/transfer.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

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

    transfer_despite_protection_flags_test/1,

    regular_file_by_view_test/1,
    files_matched_by_view_with_reduce_test/1,
    transfer_by_not_existing_view_test/1,
    transfer_by_view_emitting_invalid_file_id_test/1,
    transfer_by_view_emitting_not_existing_file_id_test/1,
    transfer_by_empty_view_test/1,
    transfer_by_view_with_not_matching_key_test/1,
    hundred_files_by_view_test/1,
    hundred_files_by_view_with_batch_10_test/1
]).

-define(BIG_FILE_CHUNK_SIZE, 33554432).  % 32 MiB
-define(BIG_FILE_CHUNKS_COUNT, 32).
-define(BIG_FILE_SIZE, ?BIG_FILE_CHUNKS_COUNT * ?BIG_FILE_CHUNK_SIZE).  % 1 GiB


%%%===================================================================
%%% API
%%%===================================================================


init_per_testcase(Case = hundred_files_by_view_with_batch_10_test, TestSuiteCtx, Config) ->
    Nodes = get_all_provider_nodes(TestSuiteCtx),
    {ok, DefaultBatchSize} = test_utils:get_env(
        hd(Nodes), op_worker, transfer_traverse_list_batch_size
    ),
    test_utils:set_env(Nodes, op_worker, transfer_traverse_list_batch_size, 10),

    NewConfig = init_per_testcase(?DEFAULT_CASE(Case), TestSuiteCtx, Config),
    [{transfer_traverse_list_batch_size, DefaultBatchSize} | NewConfig];

init_per_testcase(Case, TestSuiteCtx, Config) ->
    NewConfig = lfm_proxy:init(Config),
    transfer_test_utils:remove_leftover_file_trees(TestSuiteCtx, strip_default_case_suffix(Case)),
    transfer_test_utils:remove_all_transfers(TestSuiteCtx),
    transfer_test_utils:remove_all_views(TestSuiteCtx),
    NewConfig.


end_per_testcase(Case = hundred_files_by_view_with_batch_10_test, TestSuiteCtx, Config) ->
    Nodes = get_all_provider_nodes(TestSuiteCtx),
    DefaultBatchSize = ?config(transfer_traverse_list_batch_size, Config),
    test_utils:set_env(Nodes, op_worker, transfer_traverse_list_batch_size, DefaultBatchSize),
    end_per_testcase(?DEFAULT_CASE(Case), TestSuiteCtx, Config);

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
    % 3 levels of nested directories, 5 dirs on each level (155 dirs overall) -
    % enough to exercise a multi-level, multi-batch traverse; anything bigger
    % (the original shape was 10 dirs per level - 1110 overall) floods dbsync
    % for minutes, starving the cross-provider syncs the tests await
    RootDir = transfer_test_utils:create_file_tree(TestSuiteCtx, ?FUNCTION_NAME, #dir_spec{
        children = transfer_test_utils:gen_nested_tree_spec([5, 5, 5, 0], <<>>)
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

    TransferIdsAndFiles = lists_utils:pmap(fun(FileObject) ->
        {transfer_test_utils:schedule_transfer(TestSuiteCtx, FileObject), FileObject}
    end, FileObjects),

    lists_utils:pforeach(fun({TransferId, FileObject}) ->
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


regular_file_by_view_test(TestSuiteCtx) ->
    FileObject = #object{guid = FileGuid} = setup_single_file_for_view_test(
        TestSuiteCtx, ?FUNCTION_NAME
    ),
    XattrName = transfer_test_utils:rand_xattr_name(?FUNCTION_NAME),
    XattrValue = 1,
    set_xattr(TestSuiteCtx, FileGuid, XattrName, XattrValue),

    ViewName = transfer_test_utils:rand_view_name(?FUNCTION_NAME),
    transfer_test_utils:create_view(
        TestSuiteCtx, ViewName, transfer_test_utils:gen_view_map_function(XattrName), undefined, []
    ),
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),
    transfer_test_utils:await_view_query_result(
        TestSuiteCtx, ViewName, [{key, XattrValue}], [FileObjectId]
    ),

    TransferId = transfer_test_utils:schedule_view_transfer(
        TestSuiteCtx, ViewName, [{key, XattrValue}]
    ),
    transfer_test_utils:await_transfer_ended(TestSuiteCtx, TransferId, [FileObject], #{}),
    transfer_test_utils:assert_distribution(TestSuiteCtx, [FileObject]).


files_matched_by_view_with_reduce_test(TestSuiteCtx) ->
    RootDir = #object{children = FileObjects} = transfer_test_utils:create_file_tree(
        TestSuiteCtx, ?FUNCTION_NAME, #dir_spec{
            children = transfer_test_utils:gen_nested_tree_spec([6], ?RAND_CONTENT())
        }
    ),
    transfer_test_utils:ensure_initial_replicas(TestSuiteCtx, RootDir),
    [File1, File2, File3, File4, _File5, File6] = FileObjects,

    % the map function emits the files having Xattr1 (keyed by its value) and
    % the reduce function filters the emissions down to the ones with Xattr2 = 1
    XattrName1 = transfer_test_utils:rand_xattr_name(?FUNCTION_NAME),
    XattrName2 = transfer_test_utils:rand_xattr_name(?FUNCTION_NAME),

    % File1: xattr1 = 1, xattr2 = 1 - transferred
    set_xattr(TestSuiteCtx, File1#object.guid, XattrName1, 1),
    set_xattr(TestSuiteCtx, File1#object.guid, XattrName2, 1),
    % File2: xattr1 = 1, xattr2 = 2 - filtered out by the reduce function
    set_xattr(TestSuiteCtx, File2#object.guid, XattrName1, 1),
    set_xattr(TestSuiteCtx, File2#object.guid, XattrName2, 2),
    % File3: xattr1 = 1, xattr2 unset - filtered out by the reduce function
    set_xattr(TestSuiteCtx, File3#object.guid, XattrName1, 1),
    % File4: xattr1 = 2 - emitted under a key other than the queried one
    set_xattr(TestSuiteCtx, File4#object.guid, XattrName1, 2),
    % File5: no xattrs - not emitted at all
    % File6: xattr1 = 1, xattr2 = 1 - transferred
    set_xattr(TestSuiteCtx, File6#object.guid, XattrName1, 1),
    set_xattr(TestSuiteCtx, File6#object.guid, XattrName2, 1),

    ViewName = transfer_test_utils:rand_view_name(?FUNCTION_NAME),
    transfer_test_utils:create_view(
        TestSuiteCtx, ViewName,
        transfer_test_utils:gen_view_map_function(XattrName1, XattrName2),
        transfer_test_utils:gen_view_reduce_function(1),
        [{group, 1}, {key, 1}]
    ),
    {ok, FileObjectId1} = file_id:guid_to_objectid(File1#object.guid),
    {ok, FileObjectId6} = file_id:guid_to_objectid(File6#object.guid),
    transfer_test_utils:await_view_query_result(
        TestSuiteCtx, ViewName, [{key, 1}], [FileObjectId1, FileObjectId6]
    ),

    TransferId = transfer_test_utils:schedule_view_transfer(
        TestSuiteCtx, ViewName, [{group, true}, {key, 1}]
    ),
    transfer_test_utils:await_transfer_ended(TestSuiteCtx, TransferId, [File1, File6], #{}),
    transfer_test_utils:assert_distribution(TestSuiteCtx, [File1, File6]).


transfer_by_not_existing_view_test(TestSuiteCtx) ->
    FileObject = setup_single_file_for_view_test(TestSuiteCtx, ?FUNCTION_NAME),
    NotExistingViewName = transfer_test_utils:rand_view_name(?FUNCTION_NAME),

    % the view existence is validated only by the REST/GS middleware layer
    % (covered by transfer_create_api tests) - a transfer of an unknown view
    % scheduled directly is accepted; its processing treats the view as not
    % yet synchronized and completes as a no-op
    TransferId = transfer_test_utils:schedule_view_transfer(
        TestSuiteCtx, NotExistingViewName, [{key, 1}]
    ),
    transfer_test_utils:await_transfer_ended(TestSuiteCtx, TransferId, [], #{}),
    transfer_test_utils:assert_initial_distribution(TestSuiteCtx, [FileObject]).


transfer_by_view_emitting_invalid_file_id_test(TestSuiteCtx = #transfer_test_suite_ctx{
    transfer_type = TransferType
}) ->
    FileObject = #object{guid = FileGuid} = setup_single_file_for_view_test(
        TestSuiteCtx, ?FUNCTION_NAME
    ),
    XattrName = transfer_test_utils:rand_xattr_name(?FUNCTION_NAME),
    XattrValue = 1,
    set_xattr(TestSuiteCtx, FileGuid, XattrName, XattrValue),

    % the map function emits a value that is not a valid file id - processing
    % it counts as one failed file
    InvalidFileId = <<"invalid_file_id">>,
    MapFunction = <<
        "function (id, type, meta, ctx) {
            if (type == 'custom_metadata' && meta['", XattrName/binary, "']) {
                return [meta['", XattrName/binary, "'], '", InvalidFileId/binary, "'];
            }
            return null;
        }"
    >>,
    ViewName = transfer_test_utils:rand_view_name(?FUNCTION_NAME),
    transfer_test_utils:create_view(TestSuiteCtx, ViewName, MapFunction, undefined, []),
    transfer_test_utils:await_view_query_result(
        TestSuiteCtx, ViewName, [{key, XattrValue}], [InvalidFileId]
    ),

    TransferId = transfer_test_utils:schedule_view_transfer(
        TestSuiteCtx, ViewName, [{key, XattrValue}]
    ),
    StatusOverrides = case TransferType of
        replication ->
            #{replication_status => ?FAILED_STATUS};
        eviction ->
            #{eviction_status => ?FAILED_STATUS};
        migration ->
            % the failed replication subtask precludes the eviction one - the
            % invalid entry is counted (and fails) only once
            #{
                replication_status => ?FAILED_STATUS,
                eviction_status => ?FAILED_STATUS,
                files_to_process => 1,
                files_processed => 1
            }
    end,
    transfer_test_utils:await_transfer_ended(TestSuiteCtx, TransferId, [FileObject], StatusOverrides#{
        failed_files => 1,
        files_replicated => 0,
        files_evicted => 0,
        tree_bytes => 0
    }),
    transfer_test_utils:assert_initial_distribution(TestSuiteCtx, [FileObject]).


transfer_by_view_emitting_not_existing_file_id_test(TestSuiteCtx = #transfer_test_suite_ctx{
    space_selector = SpaceSelector
}) ->
    FileObject = #object{guid = FileGuid} = setup_single_file_for_view_test(
        TestSuiteCtx, ?FUNCTION_NAME
    ),
    XattrName = transfer_test_utils:rand_xattr_name(?FUNCTION_NAME),
    XattrValue = 1,
    set_xattr(TestSuiteCtx, FileGuid, XattrName, XattrValue),

    SpaceId = oct_background:get_space_id(SpaceSelector),
    NotExistingFileGuid = file_id:pack_guid(<<"not_existing_uuid">>, SpaceId),
    {ok, NotExistingFileObjectId} = file_id:guid_to_objectid(NotExistingFileGuid),

    MapFunction = <<
        "function (id, type, meta, ctx) {
            if (type == 'custom_metadata' && meta['", XattrName/binary, "']) {
                return [meta['", XattrName/binary, "'], '", NotExistingFileObjectId/binary, "'];
            }
            return null;
        }"
    >>,
    ViewName = transfer_test_utils:rand_view_name(?FUNCTION_NAME),
    transfer_test_utils:create_view(TestSuiteCtx, ViewName, MapFunction, undefined, []),
    transfer_test_utils:await_view_query_result(
        TestSuiteCtx, ViewName, [{key, XattrValue}], [NotExistingFileObjectId]
    ),

    % the transfer skips the not existing file without an error
    TransferId = transfer_test_utils:schedule_view_transfer(
        TestSuiteCtx, ViewName, [{key, XattrValue}]
    ),
    transfer_test_utils:await_transfer_ended(TestSuiteCtx, TransferId, [], #{}),
    transfer_test_utils:assert_initial_distribution(TestSuiteCtx, [FileObject]).


transfer_by_empty_view_test(TestSuiteCtx) ->
    XattrName = transfer_test_utils:rand_xattr_name(?FUNCTION_NAME),
    ViewName = transfer_test_utils:rand_view_name(?FUNCTION_NAME),
    transfer_test_utils:create_view(
        TestSuiteCtx, ViewName, transfer_test_utils:gen_view_map_function(XattrName), undefined, []
    ),
    transfer_test_utils:await_view_query_result(TestSuiteCtx, ViewName, [], []),

    TransferId = transfer_test_utils:schedule_view_transfer(TestSuiteCtx, ViewName, []),
    transfer_test_utils:await_transfer_ended(TestSuiteCtx, TransferId, [], #{}).


transfer_by_view_with_not_matching_key_test(TestSuiteCtx) ->
    FileObject = #object{guid = FileGuid} = setup_single_file_for_view_test(
        TestSuiteCtx, ?FUNCTION_NAME
    ),
    XattrName = transfer_test_utils:rand_xattr_name(?FUNCTION_NAME),
    set_xattr(TestSuiteCtx, FileGuid, XattrName, 1),

    ViewName = transfer_test_utils:rand_view_name(?FUNCTION_NAME),
    transfer_test_utils:create_view(
        TestSuiteCtx, ViewName, transfer_test_utils:gen_view_map_function(XattrName), undefined, []
    ),
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),
    transfer_test_utils:await_view_query_result(TestSuiteCtx, ViewName, [{key, 1}], [FileObjectId]),

    TransferId = transfer_test_utils:schedule_view_transfer(TestSuiteCtx, ViewName, [{key, 2}]),
    transfer_test_utils:await_transfer_ended(TestSuiteCtx, TransferId, [], #{}),
    transfer_test_utils:assert_initial_distribution(TestSuiteCtx, [FileObject]).


hundred_files_by_view_test(TestSuiteCtx) ->
    hundred_files_by_view_test_base(TestSuiteCtx, ?FUNCTION_NAME).


hundred_files_by_view_with_batch_10_test(TestSuiteCtx) ->
    % transfer_traverse_list_batch_size is lowered to 10 in init_per_testcase,
    % exercising the multi-batch view traverse (the default of 1000 lists the
    % whole hundred-file view in one batch)
    hundred_files_by_view_test_base(TestSuiteCtx, ?FUNCTION_NAME).


%% @private
hundred_files_by_view_test_base(TestSuiteCtx, CaseName) ->
    RootDir = #object{children = FileObjects} = transfer_test_utils:create_file_tree(
        TestSuiteCtx, CaseName, #dir_spec{
            children = transfer_test_utils:gen_nested_tree_spec([100], ?RAND_CONTENT())
        }
    ),
    transfer_test_utils:ensure_initial_replicas(TestSuiteCtx, RootDir),

    XattrName = transfer_test_utils:rand_xattr_name(CaseName),
    XattrValue = 1,
    FileObjectIds = lists_utils:pmap(fun(#object{guid = FileGuid}) ->
        set_xattr(TestSuiteCtx, FileGuid, XattrName, XattrValue),
        {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),
        FileObjectId
    end, FileObjects),

    ViewName = transfer_test_utils:rand_view_name(CaseName),
    transfer_test_utils:create_view(
        TestSuiteCtx, ViewName, transfer_test_utils:gen_view_map_function(XattrName), undefined, []
    ),
    transfer_test_utils:await_view_query_result(
        TestSuiteCtx, ViewName, [{key, XattrValue}], FileObjectIds
    ),

    TransferId = transfer_test_utils:schedule_view_transfer(
        TestSuiteCtx, ViewName, [{key, XattrValue}]
    ),
    transfer_test_utils:await_transfer_ended(TestSuiteCtx, TransferId, FileObjects, #{
        % a transfer of this many files may outlast the minute histogram window
        min_hist => skip
    }, ?SCALE_TRANSFER_ATTEMPTS),
    transfer_test_utils:assert_distribution(TestSuiteCtx, FileObjects).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
%% Creates the single-file tree most view test cases operate on, ensures
%% the initial replicas and returns the file object.
setup_single_file_for_view_test(TestSuiteCtx, CaseName) ->
    RootDir = #object{children = [FileObject]} = transfer_test_utils:create_file_tree(
        TestSuiteCtx, CaseName,
        #dir_spec{children = [#file_spec{content = ?RAND_CONTENT()}]}
    ),
    transfer_test_utils:ensure_initial_replicas(TestSuiteCtx, RootDir),
    FileObject.


%% @private
%% Sets the xattr on the other provider - the one the views are evaluated
%% on - so that view emissions do not wait for metadata dbsync.
set_xattr(#transfer_test_suite_ctx{other_provider_selector = OtherProviderSelector}, FileGuid, XattrName, XattrValue) ->
    OtherNode = oct_background:get_random_provider_node(OtherProviderSelector),
    file_test_utils:set_xattr(OtherNode, FileGuid, XattrName, XattrValue).


%% @private
%% All nodes of both providers of the suite.
get_all_provider_nodes(#transfer_test_suite_ctx{
    creation_provider_selector = CreationProviderSelector,
    other_provider_selector = OtherProviderSelector
}) ->
    oct_background:get_provider_nodes(CreationProviderSelector)
        ++ oct_background:get_provider_nodes(OtherProviderSelector).


%% @private
%% Special init/end_per_testcase clauses chain to the default ones with
%% ?DEFAULT_CASE(Case), which suffixes the case name with "_default" -
%% strip it to recover the name the case's file trees are prefixed with.
strip_default_case_suffix(Case) ->
    CaseStr = atom_to_list(Case),
    case lists:suffix("_default", CaseStr) of
        true -> list_to_atom(lists:sublist(CaseStr, length(CaseStr) - length("_default")));
        false -> Case
    end.
