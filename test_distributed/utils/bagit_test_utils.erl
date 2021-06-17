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
-export([validate_all_files_checksums/3]).

-define(BUFFER_SIZE, 1073741824). % 1GB.
-define(BATCH_SIZE, 10000).

-define(ATTEMPTS, 60).

%%%===================================================================
%%% API functions
%%%===================================================================

validate_all_files_checksums(Node, SessionId, ArchiveDirGuid) ->
    ChecksumsPerAlgorithm = collect_all_files_checksums_per_algorithms(Node, SessionId, ArchiveDirGuid),
    Path = <<"data">>,
    DataDirGuid = get_child_guid(Node, SessionId, ArchiveDirGuid, Path),
    traverse_and_validate_checksums(Node, SessionId, DataDirGuid, ChecksumsPerAlgorithm, Path).

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


read_whole_file(Node, SessionId, FileGuid) ->
    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(Node, SessionId, ?FILE_REF(FileGuid), read), ?ATTEMPTS),
    % Load whole file into buffer
    % Buffer's size is 1GB which is enough for test cases.
    {ok, Content} = lfm_proxy:check_size_and_read(Node, Handle, 0, ?BUFFER_SIZE),
    ok = lfm_proxy:close(Node, Handle),
    Content.

traverse_and_validate_checksums(Node, SessionId, ParentGuid, ChecksumsPerAlgorithm, Path) ->
    traverse_and_validate_checksums(Node, SessionId, ParentGuid, ChecksumsPerAlgorithm, Path, 0).

traverse_and_validate_checksums(Node, SessionId, ParentGuid, ChecksumsPerAlgorithm, Path, Offset) ->
    {ok, Children} = ?assertMatch({ok, _},
        lfm_proxy:get_children(Node, SessionId, ?FILE_REF(ParentGuid), Offset, ?BATCH_SIZE), ?ATTEMPTS),

    lists:foreach(fun({Guid, Name}) ->
        NewPath = filename:join(Path, Name),
        {ok, #file_attr{type = Type}} =
            ?assertMatch({ok, _}, lfm_proxy:stat(Node, SessionId, ?FILE_REF(Guid, true)), ?ATTEMPTS),
        case Type of
            ?DIRECTORY_TYPE ->
                traverse_and_validate_checksums(Node, SessionId, Guid, ChecksumsPerAlgorithm, NewPath);
            ?REGULAR_FILE_TYPE ->
                validate_file_checksums(Node, SessionId, Guid, ChecksumsPerAlgorithm, NewPath)

        end

    end, Children),

    ChildrenCount = length(Children),
    case ChildrenCount < ?BATCH_SIZE of
        true ->
            ok;
        false ->
            traverse_and_validate_checksums(Node, SessionId, ParentGuid, ChecksumsPerAlgorithm, Offset + ChildrenCount, Path)
    end.

validate_file_checksums(Node, SessionId, Guid, ChecksumsPerAlgorithm, FileRelativePath) ->
    Content = read_whole_file(Node, SessionId, Guid),
    lists:foreach(fun(Algorithm) ->
        ChecksumsPerFile = maps:get(Algorithm, ChecksumsPerAlgorithm),
        Checksum = calculate_checksum(Content, Algorithm),
        ?assertEqual(Checksum, maps:get(FileRelativePath, ChecksumsPerFile))
    end, ?SUPPORTED_CHECKSUM_ALGORITHMS).


get_checksum_manifest_file_guid(Node, SessionId, ParentGuid, ChecksumAlgorithm) ->
    get_child_guid(Node, SessionId, ParentGuid, ?CHECKSUM_MANIFEST_FILE_NAME(ChecksumAlgorithm)).

get_child_guid(Node, SessionId, ParentGuid, ChildName) ->
    {ok, ParentPath} = ?assertMatch({ok, _}, lfm_proxy:get_file_path(Node, SessionId, ParentGuid), ?ATTEMPTS),
    ChildPath = filename:join([ParentPath, ChildName]),
    {ok, #file_attr{guid = ChildGuid}} = ?assertMatch({ok, _}, lfm_proxy:stat(Node, SessionId, {path, ChildPath}), ?ATTEMPTS),
    ChildGuid.

calculate_checksum(Data, HashAlgorithm) ->
    hex_utils:hex(crypto:hash(ensure_crypto_compatible_name(HashAlgorithm), Data)).

ensure_crypto_compatible_name(sha1) -> sha;
ensure_crypto_compatible_name(Alg) -> Alg.
