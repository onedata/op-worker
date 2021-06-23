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
%%%
%%% e. g.
%%% Following file structure
%%%
%%% Dir1(DS1)
%%%    f.txt (DS2)
%%%    f2.txt (DS3)
%%%    f3.txt
%%%    Dir1.1(DS4)
%%%        hello.txt
%%%
%%% will have the following archive structure:
%%%
%%% .__onedata_archives
%%% |--- dataset_DS1
%%% |    |--- archive_123  <- (A) archive_123
%%% |         |--- bagit.txt
%%% |         |---  manifest-md5.txt
%%% |         |---  data
%%% |               |--- Dir1
%%% |                    |--- f.txt  (SL -> dataset_DS2/archive_1234/data/f.txt)
%%% |                    |--- f2.txt (SL -> dataset_DS3/archive_1235/data/f2.txt)
%%% |                    |--- f3.txt
%%% |                    |--- Dir1.1 (SL -> dataset_DS4/archive_1236/data/Dir1.1)
%%% |
%%% |--- dataset_DS2
%%% |    |--- archive_1234
%%% |         |--- bagit.txt
%%% |         |--- manifest-md5.txt
%%% |         |--- data
%%% |              |--- f.txt
%%% |
%%% |--- dataset_DS3
%%% |    |--- archive_1235
%%% |         |--- bagit.txt
%%% |         |--- manifest-md5.txt
%%% |         |--- data
%%% |              |--- f2.txt
%%% |
%%% |--- dataset_DS4
%%% |    |--- archive_1236
%%% |         |--- bagit.txt
%%% |         |--- manifest-md5.txt
%%% |         |--- data
%%% |              |--- Dir1.1
%%% |                   |--- hello.txt
%%% @end
%%%-------------------------------------------------------------------
-module(bagit_archive).
-author("Jakub Kudzia").


-include("modules/dataset/bagit.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([prepare/2, finalize/2, archive_file/4]).


%%%===================================================================
%%% API functions
%%%===================================================================

-spec prepare(file_ctx:ctx(), user_ctx:ctx()) -> {ok, file_ctx:ctx()}.
prepare(ArchiveDirCtx, UserCtx) ->
    DataDirCtx = create_data_dir(ArchiveDirCtx, UserCtx),
    create_bag_declaration(ArchiveDirCtx, UserCtx),
    % TODO VFS-7819 allow to pass this algorithms in archivisation request as param
    ChecksumAlgorithms = ?SUPPORTED_CHECKSUM_ALGORITHMS,
    bagit_checksums:create_manifests(ArchiveDirCtx, UserCtx, ChecksumAlgorithms),
    bagit_metadata:init(ArchiveDirCtx, UserCtx),
    {ok, DataDirCtx}.


-spec finalize(file_ctx:ctx(), user_ctx:ctx()) -> ok.
finalize(ArchiveDirCtx, UserCtx) ->
    create_tag_manifests(ArchiveDirCtx, UserCtx).


-spec archive_file(archive:doc(), file_ctx:ctx(), file_ctx:ctx(), user_ctx:ctx()) ->
    {ok, file_ctx:ctx()} | {error, term()}.
archive_file(ArchiveDoc, FileCtx, TargetParentCtx, UserCtx) ->
    case plain_archive:archive_file(FileCtx, TargetParentCtx, UserCtx) of
        {ok, ArchivedFileCtx} ->
            {FileDoc, ArchivedFileCtx2} = file_ctx:get_file_doc(ArchivedFileCtx),
            case file_meta:get_effective_type(FileDoc) =:= ?REGULAR_FILE_TYPE of
                true ->
                    save_checksums_and_archive_custom_metadata(ArchiveDoc, UserCtx, ArchivedFileCtx2, FileCtx);
                false ->
                    ok
            end,
            {ok, ArchivedFileCtx};
        {error, _} = Error ->
            Error
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec create_data_dir(file_ctx:ctx(), user_ctx:ctx()) -> file_ctx:ctx().
create_data_dir(ArchiveDirCtx, UserCtx) ->
    SessionId = user_ctx:get_session_id(UserCtx),
    ArchiveDirGuid = file_ctx:get_logical_guid_const(ArchiveDirCtx),
    {ok, DataDirGuid} = lfm:mkdir(SessionId, ArchiveDirGuid, ?DATA_DIR_NAME, ?DEFAULT_DIR_MODE),
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


-spec save_checksums_and_archive_custom_metadata(archive:doc(), user_ctx:ctx(), file_ctx:ctx(), file_ctx:ctx()) -> ok.
save_checksums_and_archive_custom_metadata(CurrentArchiveDoc, UserCtx, ArchivedFileCtx, SourceFileCtx) ->
    % TODO VFS-7819 allow to pass this algorithms in archivisation request as param
    ChecksumAlgorithms = ?SUPPORTED_CHECKSUM_ALGORITHMS,

    CalculatedChecksums = bagit_checksums_calculator:calculate(ArchivedFileCtx, UserCtx, ChecksumAlgorithms),

    SessionId = user_ctx:get_session_id(UserCtx),
    ArchiveFileGuid = file_ctx:get_logical_guid_const(ArchivedFileCtx),
    JsonMetadata2 = case lfm:get_metadata(SessionId, ?FILE_REF(ArchiveFileGuid), json, [], false) of
        {ok, JsonMetadata} ->
            JsonMetadata;
        {error, ?ENODATA} ->
            undefined
    end,

    {ok, AncestorArchives} = archive:get_all_ancestors(CurrentArchiveDoc),
    lists:foreach(fun(ArchiveDoc) ->
        RelativeFilePath = calculate_relative_path(ArchiveDoc, SourceFileCtx, UserCtx),
        {ok, ArchiveDirCtx} = archive:get_root_dir_ctx(ArchiveDoc),
        bagit_checksums:add_entries_to_manifests(ArchiveDirCtx, UserCtx, RelativeFilePath, CalculatedChecksums,
            ChecksumAlgorithms),
        JsonMetadata2 /= undefined
            andalso bagit_metadata:add_entry(ArchiveDirCtx, UserCtx, RelativeFilePath, JsonMetadata2)
    end, [CurrentArchiveDoc | AncestorArchives]).


-spec calculate_relative_path(archive:doc(), file_ctx:ctx(), user_ctx:ctx()) -> file_meta:path().
calculate_relative_path(ArchiveDoc, SourceFileCtx, UserCtx) ->
    {SourceFilePath, _SourceFileCtx2} = file_ctx:get_logical_path(SourceFileCtx, UserCtx),
    {ok, DatasetRootFileGuid} = archive:get_dataset_root_file_guid(ArchiveDoc),
    {DatasetRootPath, _} = file_ctx:get_logical_path(file_ctx:new_by_guid(DatasetRootFileGuid), UserCtx),
    {_, DatasetRootParentPath} = filepath_utils:basename_and_parent_dir(DatasetRootPath),
    DatasetRootParentPathTokens = filename:split(DatasetRootParentPath),
    SourceFilePathTokens = filename:split(SourceFilePath),

    filename:join([?DATA_DIR_NAME | SourceFilePathTokens -- DatasetRootParentPathTokens]).


-spec create_tag_manifests(file_ctx:ctx(), user_ctx:ctx()) -> ok.
create_tag_manifests(ArchiveDirCtx, UserCtx) ->
    % TODO VFS-7819 allow to pass this algorithms in archivisation request as param
    ChecksumAlgorithms = ?SUPPORTED_CHECKSUM_ALGORITHMS,
    DataChecksumManifests = [?CHECKSUM_MANIFEST_FILE_NAME(A) || A <- ChecksumAlgorithms],
    AllTagFileNames = [?BAG_DECLARATION_FILE_NAME, ?METADATA_FILE_NAME | DataChecksumManifests],
    ArchiveDirGuid = file_ctx:get_logical_guid_const(ArchiveDirCtx),
    SessionId = user_ctx:get_session_id(UserCtx),

    AllTagFilesNamesAndChecksums = lists:map(fun(TagFileName) ->
        {TagFileCtx, _} = files_tree:get_child(ArchiveDirCtx, TagFileName, UserCtx),
        {TagFileName, bagit_checksums_calculator:calculate(TagFileCtx, UserCtx, ChecksumAlgorithms)}
    end, AllTagFileNames),

    lists:foreach(fun(ChecksumAlgorithm) ->
        {ok, {_, Handle}} = lfm:create_and_open(SessionId, ArchiveDirGuid,
            ?TAG_MANIFEST_FILE_NAME(ChecksumAlgorithm), ?DEFAULT_FILE_MODE, write),

        {_, FinalHandle} = lists:foldl(fun({TagFileName, TagFileChecksums}, {OffsetAcc, HandleAcc}) ->
            Checksum = bagit_checksums_calculator:get(ChecksumAlgorithm, TagFileChecksums),
            Entry = ?MANIFEST_FILE_ENTRY(Checksum, TagFileName),
            {ok, NewHandle, WrittenBytes} = lfm:write(HandleAcc, OffsetAcc, Entry),
            {OffsetAcc + WrittenBytes, NewHandle}
        end, {0, Handle}, AllTagFilesNamesAndChecksums),

        ok = lfm:fsync(FinalHandle),
        ok = lfm:release(FinalHandle)
    end, ChecksumAlgorithms).