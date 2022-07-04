%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% In this module, functions for generating files' ids on storage are
%%% implemented.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_file_id).
-author("Jakub Kudzia").

-include("modules/fslogic/fslogic_common.hrl").

%%%===================================================================
%%% Exports
%%%===================================================================

%% API
-export([space_dir_id/2, flat/4, canonical/3]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% Returns storage file id of space directory.
%% If storage is marked as "imported", space data is located directly in
%% storage root directory. Otherwise data of each space is stored
%% in a dedicated subdirectory.
%% @end
%%-------------------------------------------------------------------
-spec space_dir_id(od_space:id(), storage:id()) -> helpers:file_id().
space_dir_id(SpaceId, StorageId) ->
    case storage:is_imported(StorageId) of
        true ->
            <<?DIRECTORY_SEPARATOR>>;
        false ->
            <<?DIRECTORY_SEPARATOR, SpaceId/binary>>
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns StorageFileId on flat storage.
%% If storage is flat but it's also an archiveStorage and the
%% FslogicCanonicalPath points inside .__onedata__archive directory
%% the canonical path is returned.
%% @end
%%--------------------------------------------------------------------
-spec flat(file_meta:path(), file_meta:uuid(), od_space:id(), storage:id() | storage:data()) -> helpers:file_id().
flat(FslogicCanonicalPath, FileUuid, SpaceId, StorageDataOrId) ->
    % if storage has `archiveStorage` param equal to true
    % and the canonical path points inside the hidden .__onedata__archive directory
    % the path is canonical, despite the fact that storage is flat
    case storage:is_archive(StorageDataOrId) of
        true ->
            case archivisation_tree:is_in_archive(FslogicCanonicalPath) of
                true ->
                    canonical(FslogicCanonicalPath, SpaceId, storage:get_id(StorageDataOrId));
                false ->
                    raw_flat(FileUuid, SpaceId)
            end;
        false ->
            raw_flat(FileUuid, SpaceId)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns path to file on canonical storages. Depending whether storage
%% is mounted in root path can have 2 forms:
%%   * /SpaceId/PATH/TO/FILE/... - in case of storages not mounted in root
%%   * /PATH/TO/FILE/... - in case of storages mounted in root
%% WARNING !!!
%% DO NOT CONFUSE CANONICAL PATH (PATH TO FILE IN ONEDATA SPACE) WITH
%% FILE'S ID ON CANONICAL STORAGE.
%% @end
%%--------------------------------------------------------------------
-spec canonical(file_meta:path(), od_space:id(), storage:id() | storage:data()) -> helpers:file_id().
canonical(FslogicCanonicalPath, SpaceId, StorageDataOrId) ->
    case storage:is_imported(StorageDataOrId) of
        true ->
            filter_space_id(FslogicCanonicalPath, SpaceId);
        false ->
            ensure_starts_with_space_id(FslogicCanonicalPath, SpaceId)
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns Uuid based flat path. Uuid based path is for storages, which
%% do not have POSIX style file paths (e.g. object stores) and
%% do not provide rename on files on the storage without necessity
%% to copy and delete.
%% The paths have a flat 3-level tree namespace based on the first characters,
%% e.g. "/SpaceId/A/B/C/ABCyasd7321r5ssasdd7asdsafdfvsd"
%% @end
%%--------------------------------------------------------------------
-spec raw_flat(file_meta:uuid(), od_space:id()) -> helpers:file_id().
raw_flat(FileUuid, SpaceId) ->
    case fslogic_file_id:is_root_dir_uuid(FileUuid) of
        true ->
            <<?DIRECTORY_SEPARATOR>>;
        false ->
            PathTokens = [<<?DIRECTORY_SEPARATOR>>, SpaceId],
            case fslogic_file_id:is_space_dir_uuid(FileUuid) of
                true ->
                    filepath_utils:join(PathTokens);
                false ->
                    case size(FileUuid) > 3 of
                        true ->
                            filepath_utils:join(PathTokens ++ [
                                binary_part(FileUuid, 0, 1),
                                binary_part(FileUuid, 1, 1),
                                binary_part(FileUuid, 2, 1),
                                FileUuid]);
                        false ->
                            filepath_utils:join(PathTokens ++ [<<"other">>, FileUuid])
                    end
            end
    end.


-spec filter_space_id(file_meta:path(), od_space:id()) -> file_meta:path().
filter_space_id(FilePath, SpaceId) ->
    case filepath_utils:split(FilePath) of
        [Sep, SpaceId | Path] ->
            filepath_utils:join([Sep | Path]);
        _ ->
            FilePath
    end.


-spec ensure_starts_with_space_id(file_meta:path(), od_space:id()) -> file_meta:path().
ensure_starts_with_space_id(FilePath, SpaceId) ->
    case filepath_utils:split(FilePath) of
        [_Sep, SpaceId | _Path] ->
            FilePath;
        [Sep | Path] ->
            filepath_utils:join([Sep, SpaceId | Path])
    end.