%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module for tests that validate bagit archives.
%%% @end
%%%-------------------------------------------------------------------
-module(bagit_test_utils).
-author("Jakub Kudzia").

-include("modules/dataset/bagit.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/test/assertions.hrl").


%% API
-export([validate_all_files_checksums/3, validate_all_files_json_metadata/3]).

-define(BUFFER_SIZE, 1073741824). % 1GB.
-define(BATCH_SIZE, 10000).

-define(ATTEMPTS, 60).

%%%===================================================================
%%% API functions
%%%===================================================================

validate_all_files_checksums(Node, SessionId, ArchiveDirGuid) ->
    ChecksumsPerAlgorithm = collect_all_files_checksums_per_algorithms(Node, SessionId, ArchiveDirGuid),
    ValidationFun = fun(FileGuid, FilePath) ->
        validate_file_checksums(Node, SessionId, FileGuid, FilePath, ChecksumsPerAlgorithm)
    end,
    validate_all_files(Node, SessionId, ArchiveDirGuid, ValidationFun).

validate_all_files_json_metadata(Node, SessionId, ArchiveDirGuid) ->
    AllFilesMetadata = collect_all_files_json_metadata(Node, SessionId, ArchiveDirGuid),
    ValidationFun = fun(FileGuid, FilePath) ->
        validate_file_json_metadata(Node, SessionId, FileGuid, FilePath, AllFilesMetadata)
    end,
    validate_all_files(Node, SessionId, ArchiveDirGuid, ValidationFun).


validate_all_files(Node, SessionId, ArchiveDirGuid, ValidationFun) ->
    Path = <<"data">>,
    DataDirGuid = get_child_guid(Node, SessionId, ArchiveDirGuid, Path),
    traverse_and_validate(Node, SessionId, DataDirGuid, ValidationFun, Path).

%%%===================================================================
%%% Internal functions
%%%===================================================================

collect_all_files_checksums_per_algorithms(Node, SessionId, ArchiveDirGuid) ->
    lists:foldl(fun(Algorithm, AccIn) ->
        ManifestFileGuid = get_checksum_manifest_file_guid(Node, SessionId, ArchiveDirGuid, Algorithm),
        ChecksumsMap = collect_all_files_checksums(Node, SessionId, ManifestFileGuid),
        AccIn#{Algorithm => ChecksumsMap}
    end, #{}, ?SUPPORTED_CHECKSUM_ALGORITHMS).


collect_all_files_checksums(Node, SessionId, ManifestFileGuid) ->
    Content = read_whole_file(Node, SessionId, ManifestFileGuid),
    Lines = binary:split(Content, <<"\n">>, [global, trim_all]),
    lists:foldl(fun(Line, ChecksumsAcc) ->
        [Checksum, FilePath] = binary:split(Line, <<" ">>, [global, trim_all]),
        ChecksumsAcc#{FilePath => Checksum}
    end, #{}, Lines).


collect_all_files_json_metadata(Node, SessionId, ArchiveDirGuid) ->
    MetadataFileGuid = get_metadata_file_guid(Node, SessionId, ArchiveDirGuid),
    Content = read_whole_file(Node, SessionId, MetadataFileGuid),
    json_utils:decode(Content).


read_whole_file(Node, SessionId, FileGuid) ->
    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(Node, SessionId, ?FILE_REF(FileGuid), read), ?ATTEMPTS),
    % Load whole file into buffer
    % Buffer's size is 1GB which is enough for test cases.
    {ok, Content} = lfm_proxy:check_size_and_read(Node, Handle, 0, ?BUFFER_SIZE),
    ok = lfm_proxy:close(Node, Handle),
    Content.

traverse_and_validate(Node, SessionId, ParentGuid, ValidationFun, Path) ->
    traverse_and_validate(Node, SessionId, ParentGuid, ValidationFun, Path, 0).

traverse_and_validate(Node, SessionId, ParentGuid, ValidationFun, Path, Offset) ->
    {ok, Children} = ?assertMatch({ok, _},
        lfm_proxy:get_children(Node, SessionId, ?FILE_REF(ParentGuid), Offset, ?BATCH_SIZE), ?ATTEMPTS),

    lists:foreach(fun({Guid, Name}) ->
        NewPath = filename:join(Path, Name),
        {ok, #file_attr{type = Type}} =
            ?assertMatch({ok, _}, lfm_proxy:stat(Node, SessionId, ?FILE_REF(Guid)), ?ATTEMPTS),
        case Type of
            ?DIRECTORY_TYPE ->
                traverse_and_validate(Node, SessionId, Guid, ValidationFun, NewPath);
            ?REGULAR_FILE_TYPE ->
                ValidationFun(Guid, NewPath);
            ?SYMLINK_TYPE ->
                ok
        end

    end, Children),

    ChildrenCount = length(Children),
    case ChildrenCount < ?BATCH_SIZE of
        true ->
            ok;
        false ->
            traverse_and_validate(Node, SessionId, ParentGuid, ValidationFun, Offset + ChildrenCount, Path)
    end.


validate_file_checksums(Node, SessionId, Guid, FileRelativePath, ChecksumsPerAlgorithm) ->
    Content = read_whole_file(Node, SessionId, Guid),
    lists:foreach(fun(Algorithm) ->
        ChecksumsPerFile = maps:get(Algorithm, ChecksumsPerAlgorithm),
        Checksum = calculate_checksum(Content, Algorithm),
        ?assertEqual(Checksum, maps:get(FileRelativePath, ChecksumsPerFile))
    end, ?SUPPORTED_CHECKSUM_ALGORITHMS).


validate_file_json_metadata(Node, SessionId, Guid, FileRelativePath, AllFilesJsonMetadata) ->
    case lfm_proxy:get_metadata(Node, SessionId, ?FILE_REF(Guid), json, [], false) of
        {ok, JsonMetadata} ->
            ?assertEqual(JsonMetadata, maps:get(FileRelativePath, AllFilesJsonMetadata));
        _ ->
            ok
    end.

get_metadata_file_guid(Node, SessionId, ArchiveDirGuid) ->
    get_child_guid(Node, SessionId, ArchiveDirGuid, ?METADATA_FILE_NAME).

get_checksum_manifest_file_guid(Node, SessionId, ArchiveDirGuid, ChecksumAlgorithm) ->
    get_child_guid(Node, SessionId, ArchiveDirGuid, ?CHECKSUM_MANIFEST_FILE_NAME(ChecksumAlgorithm)).

get_child_guid(Node, SessionId, ParentGuid, ChildName) ->
    {ok, #file_attr{guid = ChildGuid}} = ?assertMatch({ok, _}, lfm_proxy:get_child_attr(Node, SessionId, ParentGuid, ChildName), ?ATTEMPTS),
    ChildGuid.

calculate_checksum(Data, HashAlgorithm) ->
    hex_utils:hex(crypto:hash(ensure_crypto_compatible_name(HashAlgorithm), Data)).

ensure_crypto_compatible_name(sha1) -> sha;
ensure_crypto_compatible_name(Alg) -> Alg.
