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
-export([add_change/2, get_changes/2, create_storage_file_if_not_exists/2,
    create_storage_file/4, get_merged_changes/2, set_last_rename/3,
    rename_or_delete/2, chown_file/3, prepare_location_for_client/2]).

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
set_last_rename(#document{value = #file_location{uuid = UUID} = Loc} = Doc,
    TargetFileId, TargetSpaceId) ->
    critical_section:run([set_last_rename, UUID], fun() ->
        {ok, Locations} = file_meta:get_locations({uuid, UUID}),
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
%% Create storage file and file_location if there is no file_location defined
%% @end
%%--------------------------------------------------------------------
-spec create_storage_file_if_not_exists(od_space:id(), datastore:document()) ->
    ok | {error, term()}.
create_storage_file_if_not_exists(SpaceId, FileDoc = #document{key = FileUuid,
    value = #file_meta{mode = Mode, uid = UserId}}) ->
    file_location:critical_section(FileUuid,
        fun() ->
            case fslogic_utils:get_local_file_locations(FileDoc) of
                [] ->
                    create_storage_file(SpaceId, FileUuid, ?ROOT_SESS_ID, Mode),
                    chown_file(FileUuid, UserId, SpaceId),
                    ok;
                _ ->
                    ok
            end
        end).


%%--------------------------------------------------------------------
%% @doc
%% Create file_location and storage file
%% @end
%%--------------------------------------------------------------------
-spec create_storage_file(binary(), file_meta:uuid(), session:id(), file_meta:posix_permissions()) ->
    {FileId :: binary(), StorageId :: storage:id()}.
create_storage_file(SpaceId, FileUuid, SessId, Mode) ->
    SpaceDirUuid = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),
    {ok, #document{key = StorageId} = Storage} = fslogic_storage:select_storage(SpaceId),

    {ok, Path} = fslogic_path:gen_storage_path({uuid, FileUuid}),
    FileId0 = fslogic_utils:gen_storage_file_id({uuid, FileUuid}, Path, 0),
    LeafLess = fslogic_path:dirname(FileId0),

    ?debug("create_storage_file (dirs) ~p ~p", [Storage, LeafLess]),
    SFMHandle0 = storage_file_manager:new_handle(?ROOT_SESS_ID, SpaceDirUuid, FileUuid, Storage, LeafLess),
    case storage_file_manager:mkdir(SFMHandle0, ?AUTO_CREATED_PARENT_DIR_MODE, true) of
        ok -> ok;
        {error, eexist} ->
            ok
    end,

    FileCreatorFun = fun FileCreator(ToCreate, Version) ->
        ?debug("create_storage_file (file) ~p ~p", [Storage, ToCreate]),
        SFMHandle1 = storage_file_manager:new_handle(SessId, SpaceDirUuid, FileUuid, Storage, ToCreate),
        case storage_file_manager:create(SFMHandle1, Mode) of
            ok -> {ok, ToCreate};
            {error, eexist} ->
                NextVersion = Version + 1,
                NextFileId = fslogic_utils:gen_storage_file_id({uuid, FileUuid}, Path, NextVersion),
                case NextFileId of
                    ToCreate ->
                        ?error("Unable to generate different conflicted fileId: ~p vs ~p",
                            [{ToCreate, Version}, {NextFileId, NextVersion}]),
                        {error, create_loop_detected};
                    _ ->
                        FileCreator(NextFileId, NextVersion)
                end
        end
    end,

    {ok, FileId} = FileCreatorFun(FileId0, 0),

    Location = #file_location{blocks = [#file_block{offset = 0, size = 0, file_id = FileId, storage_id = StorageId}],
        provider_id = oneprovider:get_provider_id(), file_id = FileId, storage_id = StorageId, uuid = FileUuid,
        space_id = SpaceId},
    {ok, LocId} = file_location:create(#document{value = Location}),
    file_meta:attach_location({uuid, FileUuid}, LocId, oneprovider:get_provider_id()),

    {StorageId, FileId}.

%%--------------------------------------------------------------------
%% @doc
%% Renames file on storage and updates local location if target space
%% is supported in current provider, otherwise deletes local location and file.
%% Document is unchanged if there was no rename or given rename
%% has already been applied.
%% @end
%%--------------------------------------------------------------------
-spec rename_or_delete(file_location:doc(),
    {{helpers:file(), binary()}, non_neg_integer()} | undefined) ->
    {renamed, file_location:doc(), file_meta:uuid(), od_user:id(), od_space:id()}
    | skipped | deleted.
rename_or_delete(_, undefined) ->
    skipped;
rename_or_delete(#document{value = #file_location{last_rename = {_, LocalNum}}},
    {_, ExternalNum}) when LocalNum >= ExternalNum ->
    skipped;
rename_or_delete(Doc = #document{value = Loc = #file_location{uuid = UUID,
    blocks = OldBlocks}}, {{RemoteTargetFileId, TargetSpaceId}, _} = LastRename) ->
    {ok, Auth} = session:get_auth(?ROOT_SESS_ID),
    {ok, #document{value = #od_space{providers = Providers}}} = od_space:get_or_fetch(Auth, TargetSpaceId, ?ROOT_USER_ID),
    TargetSpaceProviders = ordsets:from_list(Providers),
    case ordsets:is_element(oneprovider:get_provider_id(), TargetSpaceProviders) of
        true ->
            {ok, TargetFileId} = sfm_utils:rename_storage_file(?ROOT_SESS_ID, Loc, RemoteTargetFileId, TargetSpaceId, 0),

            {ok, #document{key = TargetStorageId}} = fslogic_storage:select_storage(TargetSpaceId),
            NewBlocks = lists:map(fun(Block) ->
                Block#file_block{
                    file_id = TargetFileId,
                    storage_id = TargetStorageId
                }
            end, OldBlocks),

            RenamedDoc = Doc#document{value = Loc#file_location{
                file_id = TargetFileId,
                space_id = TargetSpaceId,
                storage_id = TargetStorageId,
                blocks = NewBlocks,
                last_rename = LastRename
            }},
            {ok, #document{value = #file_meta{uid = UserId}}} = file_meta:get(UUID),
            {renamed, RenamedDoc, UUID, UserId, TargetSpaceId};
        false ->
            %% TODO: VFS-2299 delete file locally without triggering deletion
            %% on other providers, also make sure all locally modified blocks
            %% are synced to other providers that still support target space
            deleted
    end.

%%--------------------------------------------------------------------
%% @doc
%% If given UserId is present in provider, then file owner is changes.
%% Otherwise, file is added to files awaiting owner change.
%% @end
%%--------------------------------------------------------------------
-spec chown_file(file_meta:uuid(), od_user:id(), od_space:id()) -> ok.
chown_file(FileUuid, UserId, SpaceId) ->
    case od_user:exists(UserId) of
        true ->
            files_to_chown:chown_file(FileUuid, UserId, SpaceId);
        false ->
            case files_to_chown:add(UserId, FileUuid) of
                {ok, _} ->
                    ok;
                AddAns ->
                    AddAns
            end
    end.


%%--------------------------------------------------------------------
%% @doc
%% Prepare location that can be understood by client.
%% @end
%%--------------------------------------------------------------------
-spec prepare_location_for_client(file_meta:entry(), fslogic_blocks:block() | undefined) -> #file_location{}.
prepare_location_for_client(FileEntry, ReqRange) ->
    % get locations
    {ok, #document{} = File} = file_meta:get(FileEntry),
    {ok, LocationIds} = file_meta:get_locations(File),
    Locations = lists:map(
        fun(LocId) ->
            {ok, Location} = file_location:get(LocId),
            Location
        end, LocationIds),
    [FileLocationDoc = #document{value = FileLocation = #file_location{blocks = Blocks, uuid = FileUuid, size = Size}}] = %todo VFS-2813 support multi location
        lists:filter(
            fun(#document{value = #file_location{provider_id = ProviderId}}) ->
                ProviderId =:= oneprovider:get_provider_id()
            end, Locations),

    % find gaps
    AllRanges = lists:foldl(
        fun(#document{value = #file_location{blocks = Blocks}}, Acc) ->
            fslogic_blocks:merge(Acc, Blocks)
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
            uuid = fslogic_uuid:uuid_to_guid(FileUuid),
            blocks = BlocksWithFilledGaps
        }).

%%%===================================================================
%%% Internal functions
%%%===================================================================
