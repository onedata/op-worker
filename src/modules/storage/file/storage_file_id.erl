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
-export([space_dir_id/2, flat/1, canonical/1]).

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
            ?DIRECTORY_SEPARATOR_BINARY;
        false ->
            <<(?DIRECTORY_SEPARATOR_BINARY)/binary, SpaceId/binary>>
    end.

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
-spec flat(file_ctx:ctx()) -> {helpers:file_id(), file_ctx:ctx()}.
flat(FileCtx) ->
    case file_ctx:is_root_dir_const(FileCtx) of
        true ->
            {?DIRECTORY_SEPARATOR_BINARY, FileCtx};
        false ->
            SpaceId = file_ctx:get_space_id_const(FileCtx),
            {IsSpaceMountedInRoot, FileCtx2} = file_ctx:is_imported_storage(FileCtx),
            PathTokens = case IsSpaceMountedInRoot of
                true -> [?DIRECTORY_SEPARATOR_BINARY, SpaceId];
                false -> [?DIRECTORY_SEPARATOR_BINARY]
            end,
            FileId = case file_ctx:is_space_dir_const(FileCtx2) of
                true ->
                    fslogic_path:join(PathTokens);
                false ->
                    FileUuid = file_ctx:get_uuid_const(FileCtx2),
                    case size(FileUuid) > 3 of
                        true ->
                            fslogic_path:join(PathTokens ++ [
                                binary_part(FileUuid, 0, 1),
                                binary_part(FileUuid, 1, 1),
                                binary_part(FileUuid, 2, 1),
                                FileUuid]);
                        false ->
                            fslogic_path:join(PathTokens ++ [<<"other">>, FileUuid])
                    end
            end,
            {FileId, FileCtx2}
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
-spec canonical(file_ctx:ctx()) -> {helpers:file_id(), file_ctx:ctx()}.
canonical(FileCtx) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    {IsSpaceMountedInRoot, FileCtx2} = file_ctx:is_imported_storage(FileCtx),
    {CanonicalPath, FileCtx3} = file_ctx:get_canonical_path(FileCtx2),
    case IsSpaceMountedInRoot of
        true ->
            {filter_space_id(CanonicalPath, SpaceId), FileCtx3};
        false ->
            {CanonicalPath, FileCtx3}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec filter_space_id(file_meta:path(), od_space:id()) -> file_meta:path().
filter_space_id(FilePath, SpaceId) ->
    case fslogic_path:split(FilePath) of
        [Sep, SpaceId | Path] ->
            fslogic_path:join([Sep | Path]);
        _ ->
            FilePath
    end.
