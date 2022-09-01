%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This is a helper module for bagit_archive module.
%%% It contains utility functions for creating checksum manifests files.
%%% @end
%%%-------------------------------------------------------------------
-module(bagit_checksums).
-author("Jakub Kudzia").

-include("modules/dataset/bagit.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").

%% API
-export([create_manifests/3, add_entries_to_manifests/5]).

-define(CRITICAL_SECTION(FileGuid, Fun), critical_section:run({?MODULE, FileGuid}, Fun)).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec create_manifests(file_ctx:ctx(), user_ctx:ctx(), file_checksum:algorithms()) -> ok.
create_manifests(ArchiveDirCtx, UserCtx, Algorithms) ->
    lists:foreach(fun(Algorithm) ->
        create_manifest(ArchiveDirCtx, UserCtx, Algorithm)
    end, Algorithms).


-spec add_entries_to_manifests(file_ctx:ctx(), user_ctx:ctx(), file_meta:path(), file_checksum:checksums(),
    file_checksum:algorithms()) -> ok.
add_entries_to_manifests(ArchiveDirCtx, UserCtx, RelativeFilePath, CalculatedChecksums, Algorithms) ->
    lists:foreach(fun(Algorithm) ->
        Checksum = file_checksum:get(Algorithm, CalculatedChecksums),
        ManifestFileCtx = get_manifest_file_ctx(ArchiveDirCtx, UserCtx, Algorithm),
        add_entry_to_manifest(ManifestFileCtx, UserCtx, RelativeFilePath, Checksum)
    end, Algorithms).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec create_manifest(file_ctx:ctx(), user_ctx:ctx(), file_checksum:algorithm()) -> ok.
create_manifest(ArchiveDirCtx, UserCtx, Algorithm) ->
    SessionId = user_ctx:get_session_id(UserCtx),
    ParentGuid = file_ctx:get_logical_guid_const(ArchiveDirCtx),
    {ok, _} = lfm:create(SessionId, ParentGuid, ?CHECKSUM_MANIFEST_FILE_NAME(Algorithm), ?DEFAULT_FILE_MODE),
    ok.


-spec add_entry_to_manifest(file_ctx:ctx(), user_ctx:ctx(), file_meta:path(), binary()) -> ok.
add_entry_to_manifest(ManifestFileCtx, UserCtx, FilePath, Checksum) ->
    SessionId = user_ctx:get_session_id(UserCtx),
    ManifestFileGuid = file_ctx:get_logical_guid_const(ManifestFileCtx),

    ?CRITICAL_SECTION(ManifestFileGuid, fun() ->
        {FileSize, _} = file_ctx:get_file_size(ManifestFileCtx),
        {ok, Handle} = lfm:open(SessionId, ?FILE_REF(ManifestFileGuid), write),
        Entry = ?MANIFEST_FILE_ENTRY(Checksum, FilePath),
        {ok, _, _} = lfm:write(Handle, FileSize, Entry),
        ok = lfm:fsync(Handle),
        ok = lfm:release(Handle)
    end).


-spec get_manifest_file_ctx(file_ctx:ctx(), user_ctx:ctx(), file_checksum:algorithm()) -> file_ctx:ctx().
get_manifest_file_ctx(ArchiveRootDirCtx, UserCtx, Algorithm) ->
    {ManifestFileCtx, _} = file_tree:get_child(ArchiveRootDirCtx, ?CHECKSUM_MANIFEST_FILE_NAME(Algorithm), UserCtx),
    ManifestFileCtx.
