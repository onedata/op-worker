%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% file location management
%%% @end
%%%--------------------------------------------------------------------
-module(fslogic_file_location).
-author("Tomasz Lichon").

-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([add_change/2, get_changes/2, get_merged_changes/2, set_last_rename/3,
    rename_or_delete/3, prepare_location_for_client/2]).

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
-spec add_change(file_location:doc(), fslogic_file_location:change()) ->
    file_location:doc().
add_change(Doc = #document{value = Location = #file_location{recent_changes = {_Backup, New}}}, Change)
    when length(New) >= ?MAX_CHANGES ->
    Doc#document{value = Location#file_location{recent_changes = {New, [Change]}}};
add_change(Doc = #document{value = Location = #file_location{recent_changes = {Backup, New}}}, Change) ->
    Doc#document{value = Location#file_location{recent_changes = {Backup, [Change | New]}}}.

%%--------------------------------------------------------------------
%% @doc
%% Get N recent changes of file_location
%% @end
%%--------------------------------------------------------------------
-spec get_changes(file_location:doc(), integer()) -> [fslogic_file_location:change()].
get_changes(_, N) when N =< 0 ->
    [];
get_changes(#document{value = #file_location{size = Size,
    recent_changes = {Backup, New}, blocks = Blocks, last_rename = LastRename}}, N)
    when N > (length(New) + length(Backup)) ->
    [Blocks, {shrink, Size}, {rename, LastRename}];
get_changes(#document{value = #file_location{recent_changes = {_Backup, New}, last_rename = LastRename}}, N)
    when N =< length(New) ->
    lists:sublist(New, N) ++ [{rename, LastRename}];
get_changes(#document{value = #file_location{recent_changes = {Backup, New}, last_rename = LastRename}}, N) ->
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
-spec set_last_rename(datastore:document(), helpers:file(), binary()) -> ok.
set_last_rename(#document{value = #file_location{uuid = FileUuid} = Loc} = Doc,
    TargetFileId, TargetSpaceId) ->
    critical_section:run([set_last_rename, FileUuid], fun() ->
        {ok, Locations} = file_meta:get_locations({uuid, FileUuid}),
        RenameNumbers = lists:map(fun(LocationId) ->
            {ok, #document{value = #file_location{last_rename = LastRename}}} =
                file_location:get(LocationId),
            case LastRename of
                undefined -> 0;
                {_, N} -> N
            end
        end, Locations),
        Max = lists:max(RenameNumbers),
        {ok, _} = file_location:save(
            Doc#document{value = Loc#file_location{last_rename =
            {{TargetFileId, TargetSpaceId}, Max + 1}}}
        ),
        ok
    end).


%%--------------------------------------------------------------------
%% @doc
%% Renames file on storage and updates local location if target space
%% is supported in current provider, otherwise deletes local location and file.
%% Document is unchanged if there was no rename or given rename
%% has already been applied.
%% @end
%%--------------------------------------------------------------------
-spec rename_or_delete(file_ctx:ctx(), file_location:doc(),
    {{helpers:file(), binary()}, non_neg_integer()} | undefined) ->
    {renamed, file_location:doc(), file_meta:uuid(), od_space:id()}
    | skipped | deleted.
rename_or_delete(FileCtx, _, undefined) ->
    {skipped, FileCtx};
rename_or_delete(FileCtx, #document{value = #file_location{last_rename = {_, LocalNum}}},
    {_, ExternalNum}) when LocalNum >= ExternalNum ->
    {skipped, FileCtx};
rename_or_delete(FileCtx,
    Doc = #document{
        value = Loc = #file_location{
            uuid = FileUuid,
            blocks = OldBlocks
        }},
    {{RemoteTargetFileId, TargetSpaceId}, _} = LastRename
) ->
    {ok, #document{
        value = #od_space{providers = Providers}
    }} = od_space:get_or_fetch(?ROOT_SESS_ID, TargetSpaceId),
    case lists:member(oneprovider:get_provider_id(), Providers) of
        true ->
            ok = sfm_utils:rename_storage_file(?ROOT_SESS_ID, Loc, RemoteTargetFileId, TargetSpaceId), %todo refactor here and inside

            NewFileCtx = file_ctx:new_by_guid(fslogic_uuid:uuid_to_guid(FileUuid, TargetSpaceId)),
            {#document{key = TargetStorageId}, NewFileCtx2} = file_ctx:get_storage_doc(NewFileCtx),
            NewBlocks = lists:map(fun(Block) ->
                Block#file_block{
                    file_id = RemoteTargetFileId,
                    storage_id = TargetStorageId
                }
            end, OldBlocks),

            RenamedDoc = Doc#document{value = Loc#file_location{
                file_id = RemoteTargetFileId,
                space_id = TargetSpaceId,
                storage_id = TargetStorageId,
                blocks = NewBlocks,
                last_rename = LastRename
            }},
            {{renamed, RenamedDoc, FileUuid, TargetSpaceId}, NewFileCtx2};
        false ->
            %% TODO: VFS-2299 delete file locally without triggering deletion
            %% on other providers, also make sure all locally modified blocks
            %% are synced to other providers that still support target space
            {deleted, FileCtx}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Prepare location that can be understood by client.
%% @end
%%--------------------------------------------------------------------
-spec prepare_location_for_client(file_ctx:ctx(), fslogic_blocks:block() | undefined) ->
    #file_location{}.
prepare_location_for_client(FileCtx, ReqRange) ->
    % get locations
    {Locations, FileCtx2} = file_ctx:get_file_location_docs(FileCtx),
    {[FileLocationDoc = #document{
        value = FileLocation = #file_location{
            blocks = Blocks,
            size = Size
        }
    }], _FileCtx3} = file_ctx:get_local_file_location_docs(FileCtx2),

    % find gaps
    AllRanges = lists:foldl(
        fun(#document{value = #file_location{blocks = _Blocks}}, Acc) ->
            fslogic_blocks:merge(Acc, _Blocks)
        end, [], Locations),
    RequestedRange = utils:ensure_defined(ReqRange, undefined, #file_block{offset = 0, size = Size}),
    ExtendedRequestedRange = case RequestedRange of
        #file_block{offset = O, size = S} when O + S < Size ->
            RequestedRange#file_block{size = Size - O};
        _ -> RequestedRange
    end,
    FullFile = replica_updater:fill_blocks_with_storage_info(
        [ExtendedRequestedRange], FileLocationDoc),
    Gaps = fslogic_blocks:consolidate(
        fslogic_blocks:invalidate(FullFile, AllRanges)
    ),
    BlocksWithFilledGaps = fslogic_blocks:merge(Blocks, Gaps),

    % fill gaps, fill storage info, transform uid and emit
    file_location:ensure_blocks_not_empty(
        FileLocation#file_location{
            uuid = file_ctx:get_guid_const(FileCtx),
            blocks = BlocksWithFilledGaps
        }).

%%%===================================================================
%%% Internal functions
%%%===================================================================
