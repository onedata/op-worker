%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is used to create an archive which is compliant
%%% with Bagit format (RFC 8493).
%%% @end
%%%-------------------------------------------------------------------
-module(bagit).
-author("Jakub Kudzia").


-include("modules/dataset/bagit.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([prepare_bag/2, calculate_checksums_and_write_to_manifests/4]).

-define(DATA_DIR_NAME, <<"data">>).

-define(BAG_DECLARATION_FILE_NAME, <<"bagit.txt">>).
-define(VERSION, "1.0").
-define(ENCODING, "UTF-8").

-define(CHECKSUM_MANIFEST_FILE_ENTRY_FORMAT, "~s    ~s~n"). % <CHECKSUM_VALUE>    <FILEPATH>\n

-type checksum_algorithm() :: bagit_checksums:algorithm().
-type checksum_algorithms() :: bagit_checksums:algorithms().


-define(BUFFER_SIZE, 52428800). % 50 M

-define(CRITICAL_SECTION(FileGuid, Fun), critical_section:run({?MODULE, FileGuid}, Fun)).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec prepare_bag(file_ctx:ctx(), user_ctx:ctx()) -> {ok, file_ctx:ctx()}.
prepare_bag(ParentCtx, UserCtx) ->
    DataDirCtx = create_data_dir(ParentCtx, UserCtx),
    create_bag_declaration(ParentCtx, UserCtx),
    % TODO VFS-7819 allow to pass this algorithms in archivisation request as param
    ChecksumAlgorithms = ?SUPPORTED_CHECKSUM_ALGORITHMS,
    create_checksum_algorithms_manifests(ParentCtx, UserCtx, ChecksumAlgorithms),
    {ok, DataDirCtx}.


-spec calculate_checksums_and_write_to_manifests(archive:doc(), user_ctx:ctx(), file_ctx:ctx(), file_ctx:ctx()) -> ok.
calculate_checksums_and_write_to_manifests(CurrentArchiveDoc, UserCtx, ArchivedFileCtx, SourceFileCtx) ->
    % TODO VFS-7819 allow to pass this algorithms in archivisation request as param
    ChecksumAlgorithms = ?SUPPORTED_CHECKSUM_ALGORITHMS,
    {SourceFilePath, _SourceFileCtx2} = file_ctx:get_canonical_path(SourceFileCtx),

    Checksums = calculate_file_checksums(ArchivedFileCtx, UserCtx, ChecksumAlgorithms),
    {ok, AncestorArchives} = archive:get_all_ancestors(CurrentArchiveDoc),
    lists:foreach(fun(ArchiveDoc) ->
        {ok, DatasetRootFileGuid} = archive:get_dataset_root_file_guid(ArchiveDoc),
        {DatasetRootPath, _} = file_ctx:get_canonical_path(file_ctx:new_by_guid(DatasetRootFileGuid)),
        {_, DatasetRootParentPath} = filepath_utils:basename_and_parent_dir(DatasetRootPath),
        DatasetRootParentPathTokens = filename:split(DatasetRootParentPath),
        SourceFilePathTokens = filename:split(SourceFilePath),
        RelativeFilePath = filename:join(SourceFilePathTokens -- DatasetRootParentPathTokens),

        {ok, ArchiveDirCtx} = archive:get_root_dir_ctx(ArchiveDoc),
        lists:foreach(fun(Algorithm) ->
            Checksum = bagit_checksums:get(Algorithm, Checksums),
            add_checksum_entry_to_manifest(ArchiveDirCtx, UserCtx, RelativeFilePath, Checksum, Algorithm)
        end, ChecksumAlgorithms)
    end, [CurrentArchiveDoc | AncestorArchives]).


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec create_data_dir(file_ctx:ctx(), user_ctx:ctx()) -> file_ctx:ctx().
create_data_dir(ParentCtx, UserCtx) ->
    SessionId = user_ctx:get_session_id(UserCtx),
    ParentGuid = file_ctx:get_logical_guid_const(ParentCtx),
    {ok, DataDirGuid} = lfm:mkdir(SessionId, ParentGuid, ?DATA_DIR_NAME, ?DEFAULT_DIR_MODE),
    file_ctx:new_by_guid(DataDirGuid).


-spec create_bag_declaration(file_ctx:ctx(), user_ctx:ctx()) -> ok.
create_bag_declaration(ParentCtx, UserCtx) ->
    SessionId = user_ctx:get_session_id(UserCtx),
    ParentGuid = file_ctx:get_logical_guid_const(ParentCtx),
    {ok, {_Guid, Handle}} = lfm:create_and_open(SessionId, ParentGuid, ?BAG_DECLARATION_FILE_NAME, ?DEFAULT_FILE_MODE, write),

    Content = str_utils:format_bin(
        "BagIt-Version: ~s~n"
        "Tag-File-Character-Encoding: ~s", [?VERSION, ?ENCODING]
    ),
    {ok, _, _} = lfm:write(Handle, 0, Content),
    ok = lfm:fsync(Handle),
    ok = lfm:release(Handle).


-spec create_checksum_algorithms_manifests(file_ctx:ctx(), user_ctx:ctx(), [checksum_algorithm()]) -> ok.
create_checksum_algorithms_manifests(ParentCtx, UserCtx, ChecksumAlgorithms) ->
    lists:foreach(fun(ChecksumAlgorithm) ->
        create_checksum_algorithm_manifest(ParentCtx, UserCtx, ChecksumAlgorithm)
    end, ChecksumAlgorithms).


-spec create_checksum_algorithm_manifest(file_ctx:ctx(), user_ctx:ctx(), checksum_algorithm()) -> ok.
create_checksum_algorithm_manifest(ParentCtx, UserCtx, ChecksumAlgorithm) ->
    SessionId = user_ctx:get_session_id(UserCtx),
    ParentGuid = file_ctx:get_logical_guid_const(ParentCtx),
    {ok, _} = lfm:create(SessionId, ParentGuid, ?CHECKSUM_MANIFEST_FILE_NAME(ChecksumAlgorithm), ?DEFAULT_FILE_MODE),
    ok.


-spec calculate_file_checksums(file_ctx:ctx(), user_ctx:ctx(), checksum_algorithms()) -> bagit_checksums:checksums().
calculate_file_checksums(FileCtx, UserCtx, ChecksumAlgorithms) ->
    Buffer = bagit_checksums:init(ChecksumAlgorithms),
    SessionId = user_ctx:get_session_id(UserCtx),
    Guid = file_ctx:get_logical_guid_const(FileCtx),
    {ok, Handle} = lfm:open(SessionId, ?FILE_REF(Guid), read),
    Checksums = calculate_file_checksums_helper(Handle, 0, Buffer),
    lfm:release(Handle),
    Checksums.


-spec calculate_file_checksums_helper(lfm:handle(), non_neg_integer(), bagit_checksums:buffers()) ->
    bagit_checksums:checksums().
calculate_file_checksums_helper(Handle, Offset, Buffers) ->
    {ok, NewHandle, Content} = lfm:read(Handle, Offset, ?BUFFER_SIZE),
    UpdatedBuffers = bagit_checksums:update(Buffers, Content),
    ContentSize = byte_size(Content),
    case ContentSize =:= 0 of
        true -> bagit_checksums:finalize(UpdatedBuffers);
        false -> calculate_file_checksums_helper(NewHandle, Offset + byte_size(Content), UpdatedBuffers)
    end.


-spec add_checksum_entry_to_manifest(file_ctx:ctx(), user_ctx:ctx(), file_meta:path(), binary(), checksum_algorithm()) -> ok.
add_checksum_entry_to_manifest(ArchiveRootDirCtx, UserCtx, FilePath, Checksum, ChecksumAlgorithm) ->
    SessionId = user_ctx:get_session_id(UserCtx),
    ManifestFileCtx = get_manifest_file_ctx(ArchiveRootDirCtx, UserCtx, ChecksumAlgorithm),
    ManifestFileGuid = file_ctx:get_logical_guid_const(ManifestFileCtx),

    ?CRITICAL_SECTION(ManifestFileGuid, fun() ->
        {FileSize, _} = file_ctx:get_file_size(ManifestFileCtx),
        {ok, Handle} = lfm:open(SessionId, ?FILE_REF(ManifestFileGuid), write),
        Entry = str_utils:format_bin(?CHECKSUM_MANIFEST_FILE_ENTRY_FORMAT, [Checksum, filename:join(?DATA_DIR_NAME, FilePath)]),
        {ok, _, _} = lfm:write(Handle, FileSize, Entry),
        ok = lfm:fsync(Handle),
        ok = lfm:release(Handle)
    end).


-spec get_manifest_file_ctx(file_ctx:ctx(), user_ctx:ctx(), checksum_algorithm()) -> file_ctx:ctx().
get_manifest_file_ctx(ArchiveRootDirCtx, UserCtx, ChecksumAlgorithm) ->
    {ManifestFileCtx, _} = files_tree:get_child(ArchiveRootDirCtx, ?CHECKSUM_MANIFEST_FILE_NAME(ChecksumAlgorithm), UserCtx),
    ManifestFileCtx.