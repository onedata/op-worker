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
%%% When such archive is not found, files from dataset are copied.
%%% @end
%%%-------------------------------------------------------------------
-module(incremental_archive).
-author("Jakub Kudzia").

-include("modules/dataset/archive.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([has_file_changed/3, find_base_for_nested_archive/3]).


%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns id of the archive, nested in the ParentBaseArchiveDoc
%% that is created from the same dataset as NestedArchiveDoc.
%% Found archive will become base for NestedArchiveDoc.
%% If corresponding archive is not found, undefined is returned. 
%% See `archivisation_tree.erl` for nested archives description.
%% @end
%%--------------------------------------------------------------------
-spec find_base_for_nested_archive(archive:doc(), archive:doc(), user_ctx:ctx()) -> archive:doc() | undefined.
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
                    #document{value = #archive{dataset_id = ExpectedDatasetId}} = NestedArchiveDoc,
                    {ok, #document{value = #archive{dataset_id = FoundDatasetId}} = Doc} = archive:get(ArchiveId),
                    case ExpectedDatasetId == FoundDatasetId of
                        true ->
                            Doc;
                        false ->
                            undefined
                    end;
                ?ERROR_NOT_FOUND ->
                    undefined
            end;
        {error, _} ->
            undefined
    end.


-spec has_file_changed(file_ctx:ctx(), file_ctx:ctx(), user_ctx:ctx()) -> boolean().
has_file_changed(BaseArchiveFileCtx, CurrentFileCtx, UserCtx) ->
    try
        archivisation_checksum:has_file_changed(BaseArchiveFileCtx, CurrentFileCtx, UserCtx)
    catch
        Class:Reason:Stacktrace ->
            CurrentFileGuid = file_ctx:get_logical_guid_const(CurrentFileCtx),
            BaseArchiveFileGuid = file_ctx:get_logical_guid_const(BaseArchiveFileCtx),
            ?error_stacktrace(
                "Error ~tp:~tp occured when comparing file ~ts with its archived version ~ts.",
                [Class, Reason, CurrentFileGuid, BaseArchiveFileGuid],
                Stacktrace
            ),
            true
    end.
