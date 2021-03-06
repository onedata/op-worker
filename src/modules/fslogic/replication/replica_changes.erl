%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Functions responsible for adding and removing changes from file_location
%%% documents.
%%% @end
%%%--------------------------------------------------------------------
-module(replica_changes).
-author("Tomasz Lichon").

-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([add_change/2, get_changes/2, get_merged_changes/2, set_last_rename/3,
    rename_or_delete/3]).

-define(MAX_CHANGES, 20).

-type change() :: fslogic_blocks:blocks() | {shrink, non_neg_integer()} | {rename, last_rename()}.
-type last_rename() :: {{FileId :: helpers:file_id(), SpaceId :: binary()}, Seq :: non_neg_integer()}.

-export_type([change/0, last_rename/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Add changelog to file_location document
%% @end
%%--------------------------------------------------------------------
-spec add_change(file_location:doc(), replica_changes:change()) ->
    file_location:doc().
add_change(Doc = #document{value = Location = #file_location{
    recent_changes = {_Backup, New}
}}, Change) when length(New) >= ?MAX_CHANGES ->
    Doc#document{value = Location#file_location{
        recent_changes = {New, [Change]}
    }};
add_change(Doc = #document{value = Location = #file_location{recent_changes = {Backup, New}}}, Change) ->
    Doc#document{value = Location#file_location{recent_changes = {Backup, [Change | New]}}}.

%%--------------------------------------------------------------------
%% @doc
%% Get N recent changes of file_location
%% @end
%%--------------------------------------------------------------------
-spec get_changes(file_location:doc(), integer()) -> [replica_changes:change()].
get_changes(_, N) when N =< 0 ->
    [];
get_changes(#document{value = #file_location{
    size = Size,
    recent_changes = {Backup, New},
    last_rename = LastRename
}} = FL, N) when N > (length(New) + length(Backup)) ->
    [fslogic_location_cache:get_blocks(FL), {shrink, Size}, {rename, LastRename}];
get_changes(#document{value = #file_location{
    recent_changes = {_Backup, New},
    last_rename = LastRename
}}, N) when N =< length(New) ->
    lists:sublist(New, N) ++ [{rename, LastRename}];
get_changes(#document{value = #file_location{
    recent_changes = {Backup, New},
    last_rename = LastRename
}}, N) ->
    lists:sublist(New ++ Backup, N) ++ [{rename, LastRename}].

%%--------------------------------------------------------------------
%% @doc
%% Get N changes merged into form:
%% {BlocksChangesList, ShrinkSize | undefined, LastRename | undefined}
%% @end
%%--------------------------------------------------------------------
-spec get_merged_changes(file_location:doc(), integer()) ->
    {fslogic_blocks:blocks(), integer() | undefined, last_rename() | undefined}.
get_merged_changes(Doc, N) ->
    Changes = get_changes(Doc, N),
    Shrink = lists:foldl(fun
        ({shrink, Size}, MinimalSize) -> min(Size, MinimalSize);
        (_, MinimalSize) -> MinimalSize
    end, undefined, Changes),
    BlockChanges = lists:filter(fun
        ({shrink, _}) -> false;
        ({rename, _}) -> false;
        (_) -> true
    end, Changes),
    AggregatedBlocks = lists:foldl(fun(Blocks, Acc) ->
        fslogic_blocks:merge(Blocks, Acc)
    end, [], BlockChanges),
    #document{value = #file_location{last_rename = LastRename}} = Doc,
    {AggregatedBlocks, Shrink, LastRename}.

%%--------------------------------------------------------------------
%% @doc
%% Sets last_rename field in given file_location
%% @end
%%--------------------------------------------------------------------
-spec set_last_rename(datastore:doc(), helpers:file_id(), binary()) -> ok.
set_last_rename(Doc = #document{value = Loc = #file_location{uuid = FileUuid}},
    TargetFileId, TargetSpaceId) ->
    {ok, Locations} = file_meta:get_locations_by_uuid(FileUuid),
    RenameNumbers = lists:map(fun(LocationId) ->
        case fslogic_location_cache:get_location(LocationId, FileUuid, false) of
            {ok, #document{value = #file_location{last_rename = LastRename}}} ->
                case LastRename of
                    undefined -> 0;
                    {_, N} -> N
                end;
            {error, not_found} ->
                0
        end
    end, Locations),
    Max = lists:max(RenameNumbers),
    {ok, _} = fslogic_location_cache:save_location(
        Doc#document{value = Loc#file_location{
            last_rename = {{TargetFileId, TargetSpaceId}, Max + 1}
        }}),
    ok.


%%--------------------------------------------------------------------
%% @doc %TODO VFS-7394 get file system logic out of here, keep only replica changes
%% Renames file on storage and updates local location if target space
%% is supported in current provider, otherwise deletes local location and file.
%% Document is unchanged if there was no rename or given rename
%% has already been applied.
%% @end
%%--------------------------------------------------------------------
-spec rename_or_delete(file_ctx:ctx(), file_location:doc(),
    {{helpers:file_id(), binary()}, non_neg_integer()} | undefined) ->
    {{renamed, file_location:doc(), file_meta:uuid(), od_space:id()}
    | skipped | deleted, file_ctx:ctx()}.
rename_or_delete(FileCtx, _, undefined) ->
    {skipped, FileCtx};
rename_or_delete(FileCtx, #document{value = #file_location{
    last_rename = {_, LocalNum}
}}, {_, ExternalNum}) when LocalNum >= ExternalNum ->
    {skipped, FileCtx};
rename_or_delete(FileCtx,
    Doc = #document{
        value = Loc = #file_location{
            uuid = FileUuid,
            file_id = SourceFileId
        }},
    {{RemoteTargetFileId, TargetSpaceId}, _} = LastRename
) ->
    case provider_logic:supports_space(TargetSpaceId) of
        true ->
            NewFileCtx = file_ctx:new_by_uuid(FileUuid, TargetSpaceId),
            case file_ctx:is_readonly_storage(NewFileCtx) of
                {true, NewFileCtx2} ->
                    {skipped, NewFileCtx2};
                {false, NewFileCtx2} ->
                    {TargetStorageId, NewFileCtx3} = file_ctx:get_storage_id(NewFileCtx2),
                    Helper = storage:get_helper(TargetStorageId),
                    case helper:get_storage_path_type(Helper) of
                        ?CANONICAL_STORAGE_PATH ->
                            TargetStorageFileId = storage_file_id:canonical(RemoteTargetFileId, TargetSpaceId, TargetStorageId),
                            % TODO VFS-6155 properly handle remote rename, target parent doc may not be synchronized yet, how do we know its mode?
                            case sd_utils:rename(user_ctx:new(?ROOT_SESS_ID), TargetSpaceId,
                                TargetStorageId, FileUuid, SourceFileId, undefined, TargetStorageFileId)
                            of
                                ok -> ok;
                                {error, ?ENOENT} -> ok
                            end,
                            RenamedDoc = Doc#document{value = Loc#file_location{
                                file_id = TargetStorageFileId,
                                space_id = TargetSpaceId,
                                storage_id = TargetStorageId,
                                last_rename = LastRename
                            }},
                            {{renamed, RenamedDoc, FileUuid, TargetSpaceId}, NewFileCtx3};
                        ?FLAT_STORAGE_PATH ->
                            {skipped, NewFileCtx2}
                    end
            end;
        false ->
            %% TODO: VFS-2299 delete file locally without triggering deletion
            %% on other providers, also make sure all locally modified blocks
            %% are synced to other providers that still support target space
            {deleted, FileCtx}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================
