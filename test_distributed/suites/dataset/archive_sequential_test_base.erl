%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Sequential tests of archives mechanism.
%%% @end
%%%-------------------------------------------------------------------
-module(archive_sequential_test_base).
-author("Jakub Kudzia").


-include("onenv_test_utils.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("modules/dataset/archive.hrl").
-include("modules/dataset/archivisation_tree.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").


%% tests
-export([
    archive_big_tree_test/1,
    archive_directory_with_number_of_files_exceeding_batch_size_test/1,
    archive_simple_dataset_test/3,
    
    verification_modify_file_base/1,
    verification_modify_file_metadata_base/1,
    verification_modify_dir_metadata_base/1,
    verification_remove_file_base/1,
    verification_create_file_base/1,
    verification_recreate_file_base/1,
    dip_verification_test_base/1,
    nested_verification_test_base/1
]).

-define(RAND_NAME(), str_utils:rand_hex(20)).
-define(RAND_SIZE(), rand:uniform(50)).
-define(RAND_CONTENT(), crypto:strong_rand_bytes(?RAND_SIZE())).
-define(RAND_JSON_METADATA(),
    lists:foldl(fun(_, AccIn) ->
        AccIn#{?RAND_NAME() => ?RAND_NAME()}
    end, #{}, lists:seq(1, rand:uniform(10)))
).

-define(SPACE, space_krk_par_p).
-define(USER1, user1).

-define(LARGE_ATTEMPTS, 1200).
-define(SMALL_ATTEMPTS, 60).

%===================================================================
% Test bases
%===================================================================


archive_big_tree_test(Layout) ->
    archive_dataset_tree_test_base([{10, 10}, {10, 10}, {10, 10}], Layout).


archive_directory_with_number_of_files_exceeding_batch_size_test(Layout) ->
    % default batch size is 1000
    archive_dataset_tree_test_base([{0, 2048}], Layout).


archive_dataset_tree_test_base(FileStructure, ArchiveLayout) ->
    Provider = lists_utils:random_element(oct_background:get_space_supporting_providers(?SPACE)),
    Node = oct_background:get_random_provider_node(Provider),
    SessId = oct_background:get_user_session_id(?USER1, Provider),
    #object{
        guid = RootGuid,
        dataset = #dataset_object{id = DatasetId}
    } = onenv_file_test_utils:create_and_sync_file_tree(?USER1, ?SPACE, #dir_spec{dataset = #dataset_spec{}}),

    {_, FileGuids} = lfm_test_utils:create_files_tree(Node, SessId, FileStructure, RootGuid),

    {ok, ArchiveId} =
        opt_archives:archive_dataset(Node, SessId, DatasetId, #archive_config{layout = ArchiveLayout}, <<>>),

    % created files are empty therefore expected size is 0
    archive_tests_utils:assert_archive_is_preserved(Node, SessId, ArchiveId, DatasetId, RootGuid, length(FileGuids), 0, ?LARGE_ATTEMPTS).


archive_simple_dataset_test(Guid, DatasetId, ArchiveId) ->
    SpaceId = oct_background:get_space_id(?SPACE),
    lists:foreach(fun(Provider) ->
        Node = oct_background:get_random_provider_node(Provider),
        SessionId = oct_background:get_user_session_id(?USER1, Provider),
        UserId = oct_background:get_user_id(?USER1),
        archive_tests_utils:assert_archive_dir_structure_is_correct(Node, SessionId, SpaceId, DatasetId, ArchiveId, UserId, ?LARGE_ATTEMPTS),
        {ok, #file_attr{type = Type, size = Size}} = lfm_proxy:stat(Node, SessionId, ?FILE_REF(Guid)),
        {FileCount, ExpSize} = case Type of
            ?DIRECTORY_TYPE -> {0, 0};
            ?SYMLINK_TYPE -> {1, 0};
            _ -> {1, Size}
        end,
        archive_tests_utils:assert_archive_is_preserved(Node, SessionId, ArchiveId, DatasetId, Guid, FileCount, ExpSize, ?LARGE_ATTEMPTS)
    end, oct_background:get_space_supporting_providers(?SPACE)).


verification_modify_file_base(Layout) ->
    ModificationFun = fun(Node, _DirGuid, FileGuid, _FileName, _Content, _Metadata) ->
        ok = lfm_test_utils:write_file(Node, ?ROOT_SESS_ID, FileGuid, ?RAND_CONTENT())
    end,
    simple_verification_test_base(Layout, ModificationFun).


verification_modify_file_metadata_base(Layout) ->
    ModificationFun = fun(Node, _DirGuid, FileGuid, _FileName, _Content, _Metadata) ->
        ok = opt_file_metadata:set_custom_metadata(Node, ?ROOT_SESS_ID, #file_ref{guid = FileGuid}, json, ?RAND_JSON_METADATA(), [])
    end,
    simple_verification_test_base(Layout, ModificationFun).


verification_modify_dir_metadata_base(Layout) ->
    ModificationFun = fun(Node, DirGuid, _FileGuid, _FileName, _Content, _Metadata) ->
        ok = opt_file_metadata:set_custom_metadata(Node, ?ROOT_SESS_ID, #file_ref{guid = DirGuid}, json, ?RAND_JSON_METADATA(), [])
    end,
    simple_verification_test_base(Layout, ModificationFun).


verification_remove_file_base(Layout) ->
    ModificationFun = fun(Node, _DirGuid, FileGuid, _FileName, _Content, _Metadata) ->
        ok = lfm_proxy:unlink(Node, ?ROOT_SESS_ID, #file_ref{guid = FileGuid})
    end,
    simple_verification_test_base(Layout, ModificationFun).


verification_create_file_base(Layout) ->
    ModificationFun = fun(Node, DirGuid, _FileGuid, _FileName, _Content, _Metadata) ->
        {ok, _} = lfm_proxy:create(Node, ?ROOT_SESS_ID, DirGuid, generator:gen_name(), ?DEFAULT_FILE_MODE)
    end,
    simple_verification_test_base(Layout, ModificationFun).


verification_recreate_file_base(Layout) ->
    ModificationFun = fun(Node, DirGuid, FileGuid, FileName, Content, Metadata) ->
        ok = lfm_proxy:unlink(Node, ?ROOT_SESS_ID, #file_ref{guid = FileGuid}),
        {ok, NewFileGuid} = lfm_test_utils:create_and_write_file(Node, ?ROOT_SESS_ID, DirGuid, FileName, 0, Content),
        ok = opt_file_metadata:set_custom_metadata(Node, ?ROOT_SESS_ID, #file_ref{guid = NewFileGuid}, json, Metadata, [])
    end,
    simple_verification_test_base(Layout, ModificationFun).


simple_verification_test_base(Layout, ModificationFun) ->
    OriginalContent = ?RAND_CONTENT(),
    OriginalMetadata = ?RAND_JSON_METADATA(),
    archive_tests_utils:mock_archive_verification(),
    #object{
        dataset = #dataset_object{
            archives = [#archive_object{id = ArchiveId}]
        }
    } = onenv_file_test_utils:create_and_sync_file_tree(?USER1, ?SPACE,
        #dir_spec{
            dataset = #dataset_spec{archives = [#archive_spec{config = #archive_config{layout = Layout}}]},
            children = [#file_spec{content = OriginalContent, metadata = #metadata_spec{json = OriginalMetadata}}],
            metadata = #metadata_spec{json = OriginalMetadata}
        }),
    {ok, Pid} = archive_tests_utils:wait_for_archive_verification_traverse(ArchiveId, ?SMALL_ATTEMPTS),
    
    [Provider | _] = oct_background:get_space_supporting_providers(?SPACE),
    Node = oct_background:get_random_provider_node(Provider),
    SessionId = oct_background:get_user_session_id(?USER1, Provider),
    ?assertMatch({ok, #document{value = #archive{state = ?ARCHIVE_VERIFYING}}}, ?rpc(Provider, archive:get(ArchiveId)), ?SMALL_ATTEMPTS),
    
    {ok, ArchiveDataDirGuid} = ?rpc(Provider, archive:get_data_dir_guid(ArchiveId)),
    {ok, [{ArchivedDirGuid, _}]} = lfm_proxy:get_children(Node, SessionId, #file_ref{guid = ArchiveDataDirGuid}, 0, 1),
    {ok, [{ArchivedFileGuid, ArchivedFileName}]} = lfm_proxy:get_children(Node, SessionId, #file_ref{guid = ArchivedDirGuid}, 0, 1),
    
    ModificationFun(Node, ArchivedDirGuid, ArchivedFileGuid, ArchivedFileName, OriginalContent, OriginalMetadata),
    
    archive_tests_utils:start_verification_traverse(Pid, ArchiveId),
    
    ?assertMatch({ok, #document{value = #archive{state = ?ARCHIVE_VERIFICATION_FAILED}}}, ?rpc(Provider, archive:get(ArchiveId)), ?SMALL_ATTEMPTS).


dip_verification_test_base(Layout) ->
    archive_tests_utils:mock_archive_verification(),
    #object{
        dataset = #dataset_object{
            archives = [#archive_object{id = AipArchiveId}]
        }
    } = onenv_file_test_utils:create_and_sync_file_tree(?USER1, ?SPACE,
        #dir_spec{
            dataset = #dataset_spec{archives = [#archive_spec{config = #archive_config{layout = Layout, include_dip = true}}]},
            children = [#file_spec{content = ?RAND_CONTENT(), metadata = #metadata_spec{json = ?RAND_JSON_METADATA()}}]
        }),
    
    [Provider | _] = oct_background:get_space_supporting_providers(?SPACE),
    
    {ok, #document{value = #archive{related_dip = DipArchiveId}}} = ?rpc(Provider, archive:get(AipArchiveId)),
    
    lists:foreach(fun(ArchiveId) ->
        {ok, Pid} = archive_tests_utils:wait_for_archive_verification_traverse(ArchiveId, ?SMALL_ATTEMPTS),
        archive_tests_utils:start_verification_traverse(Pid, ArchiveId),
        ?assertMatch({ok, #document{value = #archive{state = ?ARCHIVE_PRESERVED}}}, ?rpc(Provider, archive:get(ArchiveId)), ?SMALL_ATTEMPTS)
    end, [AipArchiveId, DipArchiveId]).


nested_verification_test_base(Layout) ->
    archive_tests_utils:mock_archive_verification(),
    #object{
        dataset = #dataset_object{
            archives = [#archive_object{id = ArchiveId}]
        },
        children = [
            #object{dataset = #dataset_object{id = NestedDatasetId1}},
            #object{dataset = #dataset_object{id = NestedDatasetId2}}
        ]
    } = onenv_file_test_utils:create_and_sync_file_tree(?USER1, ?SPACE,
        #dir_spec{
            dataset = #dataset_spec{archives = [#archive_spec{config = #archive_config{layout = Layout, create_nested_archives = true}}]},
            children = [
                #file_spec{dataset = #dataset_spec{}},
                #dir_spec{dataset = #dataset_spec{}}
            ]
        }),
    
    [Provider | _] = oct_background:get_space_supporting_providers(?SPACE),
    
    {ok, {[{_, NestedArchiveId1} | _], _}} = opt_archives:list(Provider, ?ROOT_SESS_ID, NestedDatasetId1, #{offset => 0, limit => 1}),
    {ok, {[{_, NestedArchiveId2} | _], _}} = opt_archives:list(Provider, ?ROOT_SESS_ID, NestedDatasetId2, #{offset => 0, limit => 1}),
    
    lists:foreach(fun(Id) ->
        {ok, Pid} = archive_tests_utils:wait_for_archive_verification_traverse(Id, ?SMALL_ATTEMPTS),
        archive_tests_utils:start_verification_traverse(Pid, Id),
        ?assertMatch({ok, #document{value = #archive{state = ?ARCHIVE_PRESERVED}}}, ?rpc(Provider, archive:get(Id)), ?SMALL_ATTEMPTS)
    end, [NestedArchiveId1, NestedArchiveId2, ArchiveId]).
