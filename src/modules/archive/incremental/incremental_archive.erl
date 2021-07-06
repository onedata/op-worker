%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module has utility functions used when creating an
%%% incremental archive. 
%%% Incremental archive is an archive that is created based on other archive 
%%% (base archive) in the same dataset. If file in dataset did not change 
%%% since the time the base archive was created, just a hardlink to the file 
%%% in base archive is created instead of copying whole file content. If base archive 
%%% is not provided during creation but incremental value is set to true last 
%%% successfully preserved archive in dataset (if exists) is selected as base archive.
%%% @end
%%%-------------------------------------------------------------------
-module(incremental_archive).
-author("Jakub Kudzia").

-include("modules/dataset/archive.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([find_base_archive_id/1, has_file_changed/3, find_base_for_nested_archive/3]).


%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns base archive for creating an incremental archive from the
%% dataset. The base archive will be the most recent successfully
%% preserved archive of the dataset.
%% @end
%%--------------------------------------------------------------------
-spec find_base_archive_id(dataset:id()) -> archive:id() | undefined.
find_base_archive_id(DatasetId) ->
    find_most_recent_preserved_archive(DatasetId, #{start_index => <<>>, limit => 1000}).


%%--------------------------------------------------------------------
%% @doc
%% Returns id of the archive, nested in the ParentBaseArchiveDoc
%% that is created from the same dataset as NestedArchiveDoc.
%% Found archive will become base for NestedArchiveDoc.
%% If corresponding archive is not found, undefined is returned.
%% @end
%%--------------------------------------------------------------------
-spec find_base_for_nested_archive(archive:doc(), archive:doc(), user_ctx:ctx()) -> archive:id() | undefined.
find_base_for_nested_archive(NestedArchiveDoc, ParentBaseArchiveDoc, UserCtx) ->
    {ok, ParentDatasetRootFileCtx} = archive:get_dataset_root_file_ctx(ParentBaseArchiveDoc),
    {ParentDatasetRootPath, _} = file_ctx:get_logical_path(ParentDatasetRootFileCtx, UserCtx),
    {_, ParentDatasetRootParentPath} = filepath_utils:basename_and_parent_dir(ParentDatasetRootPath),
    {ok, NestedDatasetRootFileCtx} = archive:get_dataset_root_file_ctx(NestedArchiveDoc),
    {NestedDatasetRootPath, _} = file_ctx:get_logical_path(NestedDatasetRootFileCtx, UserCtx),
    NestedDatasetRelativePath = filepath_utils:relative(ParentDatasetRootParentPath, NestedDatasetRootPath),

    case archive:find_file(ParentBaseArchiveDoc, NestedDatasetRelativePath, UserCtx) of
        {ok, NestedDatasetRootInBaseArchiveFileCtx} ->
            {CanonicalPath, _} = file_ctx:get_canonical_path(NestedDatasetRootInBaseArchiveFileCtx),
            case archivisation_tree:extract_archive_id(CanonicalPath) of
                {ok, ArchiveId} ->
                    {ok, Doc} = archive:get(ArchiveId),
                    Doc;
                ?ERROR_NOT_FOUND ->
                    undefined
            end;
        {error, _} ->
            undefined
    end.


-spec has_file_changed(file_ctx:ctx(), file_ctx:ctx(), user_ctx:ctx()) -> boolean().
has_file_changed(BaseArchiveFileCtx, CurrentFileCtx, UserCtx) ->
    try
        has_checksum_changed(BaseArchiveFileCtx, CurrentFileCtx, UserCtx) orelse
        has_metadata_changed(BaseArchiveFileCtx, CurrentFileCtx, UserCtx)
    catch
        Class:Reason ->
            CurrentFileGuid = file_ctx:get_logical_guid_const(CurrentFileCtx),
            BaseArchiveFileGuid = file_ctx:get_logical_guid_const(BaseArchiveFileCtx),
            ?error_stacktrace("Error ~p:~p occured when comparing file ~s with its archived version ~s.",
                [Class, Reason, CurrentFileGuid, BaseArchiveFileGuid]),
            true
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec find_most_recent_preserved_archive(dataset:id(), archive_api:listing_opts()) -> archive:id() | undefined.
find_most_recent_preserved_archive(DatasetId, Opts) ->
    {ok, Archives, IsLast} = archive_api:list_archives(DatasetId, Opts, ?EXTENDED_INFO),

    {BaseArchiveOrUndefined, LastArchiveIndex} = lists_utils:foldl_while(fun
        (#archive_info{state = ?ARCHIVE_PRESERVED, id = Id, index = Index}, {undefined, _}) ->
            {halt, {Id, Index}};
        (#archive_info{index = Index}, {undefined, _}) ->
            {cont, {undefined, Index}}
    end, {undefined, maps:get(start_index, Opts)}, Archives),

    case {BaseArchiveOrUndefined, IsLast} of
        {undefined, true} -> undefined;
        {undefined, false} -> find_most_recent_preserved_archive(DatasetId, Opts#{start_index => LastArchiveIndex});
        {BaseArchiveId, _} -> BaseArchiveId
    end.

-spec has_checksum_changed(file_ctx:ctx(), file_ctx:ctx(), user_ctx:ctx()) -> boolean().
has_checksum_changed(BaseFileCtx, CurrentFileCtx, UserCtx) ->
    BaseFileChecksum = archivisation_checksum:get(BaseFileCtx),
    CurrentFileChecksum = archivisation_checksum:calculate(CurrentFileCtx, UserCtx),
    BaseFileChecksum =/= CurrentFileChecksum.


-spec has_metadata_changed(file_ctx:ctx(), file_ctx:ctx(), user_ctx:ctx()) -> boolean().
has_metadata_changed(BaseFileCtx, CurrentFileCtx, UserCtx) ->
    % currently only json metadata are archived
    get_json_metadata(BaseFileCtx, UserCtx) =/= get_json_metadata(CurrentFileCtx, UserCtx).


-spec get_json_metadata(file_ctx:ctx(), user_ctx:ctx()) -> json_utils:json_term() | undefined.
get_json_metadata(FileCtx, UserCtx) ->
    SessionId = user_ctx:get_session_id(UserCtx),
    FileGuid = file_ctx:get_logical_guid_const(FileCtx),
    case lfm:get_metadata(SessionId, ?FILE_REF(FileGuid), json, [], false) of
        {ok, JsonMetadata} -> JsonMetadata;
        {error, ?ENODATA} -> undefined
    end.