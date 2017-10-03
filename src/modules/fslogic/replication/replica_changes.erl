%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @todo simplify rename logic
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
-type last_rename() :: {{FileId :: helpers:file(), SpaceId :: binary()}, Seq :: non_neg_integer()}.

-export_type([change/0]).

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
    blocks = Blocks,
    last_rename = LastRename
}}, N) when N > (length(New) + length(Backup)) ->
    [Blocks, {shrink, Size}, {rename, LastRename}];
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
-spec set_last_rename(datastore:doc(), helpers:file(), binary()) -> ok.
set_last_rename(Doc = #document{value = Loc = #file_location{uuid = FileUuid}},
    TargetFileId, TargetSpaceId) ->
    critical_section:run([set_last_rename, FileUuid], fun() ->
        {ok, Locations} = file_meta:get_locations_by_uuid(FileUuid),
        RenameNumbers = lists:map(fun(LocationId) ->
            case file_location:get(LocationId) of
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
        {ok, _} = file_location:save(
            Doc#document{value = Loc#file_location{
                last_rename = {{TargetFileId, TargetSpaceId}, Max + 1}
            }}),
        ok
    end).


%%--------------------------------------------------------------------
%% @doc %todo get file system logic out of here, keep only replica changes
%% Renames file on storage and updates local location if target space
%% is supported in current provider, otherwise deletes local location and file.
%% Document is unchanged if there was no rename or given rename
%% has already been applied.
%% @end
%%--------------------------------------------------------------------
-spec rename_or_delete(file_ctx:ctx(), file_location:doc(),
    {{helpers:file(), binary()}, non_neg_integer()} | undefined) ->
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
    {ok, Providers} = space_logic:get_provider_ids(?ROOT_SESS_ID, TargetSpaceId),
    case lists:member(oneprovider:get_provider_id(), Providers) of
        true ->
            {ok, Storage} = fslogic_storage:select_storage(TargetSpaceId),
            case sfm_utils:rename_storage_file(?ROOT_SESS_ID, TargetSpaceId,
                Storage, FileUuid, SourceFileId, RemoteTargetFileId)
            of
                ok -> ok;
                {error, ?ENOENT} -> ok
            end,
            NewFileCtx = file_ctx:new_by_guid(fslogic_uuid:uuid_to_guid(FileUuid, TargetSpaceId)),
            {#document{key = TargetStorageId}, NewFileCtx2} = file_ctx:get_storage_doc(NewFileCtx),

            RenamedDoc = Doc#document{value = Loc#file_location{
                file_id = RemoteTargetFileId,
                space_id = TargetSpaceId,
                storage_id = TargetStorageId,
                last_rename = LastRename
            }},
            {{renamed, RenamedDoc, FileUuid, TargetSpaceId}, NewFileCtx2};
        false ->
            %% TODO: VFS-2299 delete file locally without triggering deletion
            %% on other providers, also make sure all locally modified blocks
            %% are synced to other providers that still support target space
            {deleted, FileCtx}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================
