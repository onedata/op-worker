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
    rename_or_delete/2, chown_file/3]).

-define(MAX_CHANGES, 20).

-type change() :: fslogic_blocks:blocks() | {shrink, non_neg_integer()} |
    {rename, {{helpers:file(), binary()}, non_neg_integer()}}.

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
    {fslogic_blocks:blocks(), integer() | undefined,
        {{helpers:file(), binary()}, non_neg_integer()} | undefined}.
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
-spec create_storage_file_if_not_exists(space_info:id(), datastore:document()) -> ok.
create_storage_file_if_not_exists(SpaceId, FileDoc) ->
    create_storage_file_if_not_exists(SpaceId, FileDoc, 30).

%%--------------------------------------------------------------------
%% @doc
%% Create storage file and file_location if there is no file_location defined
%% @end
%%--------------------------------------------------------------------
-spec create_storage_file_if_not_exists(space_info:id(), datastore:document(), integer()) ->
    ok | {error, term()}.
create_storage_file_if_not_exists(SpaceId, FileDoc, 0) ->
    create_storage_file_if_not_exists_once(SpaceId, FileDoc);
create_storage_file_if_not_exists(SpaceId, FileDoc, Num) ->
    try
        ok = create_storage_file_if_not_exists_once(SpaceId, FileDoc)
    catch
        _:_ ->
            timer:sleep(500),
            create_storage_file_if_not_exists(SpaceId, FileDoc, Num - 1)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Create storage file and file_location if there is no file_location defined
%% @end
%%--------------------------------------------------------------------
-spec create_storage_file_if_not_exists_once(space_info:id(), datastore:document()) ->
    ok | {error, term()}.
create_storage_file_if_not_exists_once(SpaceId, FileDoc = #document{key = FileUuid,
    value = #file_meta{mode = Mode, uid = UserId}}) ->
    file_location:run_synchronized(FileUuid,
        fun() ->
            case fslogic_utils:get_local_file_locations_once(FileDoc) of
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
    {ok, #document{key = StorageId} = Storage} = fslogic_storage:select_storage(SpaceId),
    FileId = fslogic_utils:gen_storage_file_id({uuid, FileUuid}),
    SpaceDirUuid = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),
    Location = #file_location{blocks = [#file_block{offset = 0, size = 0, file_id = FileId, storage_id = StorageId}],
        provider_id = oneprovider:get_provider_id(), file_id = FileId, storage_id = StorageId, uuid = FileUuid,
        space_id = SpaceId},
    {ok, LocId} = file_location:create(#document{value = Location}),
    file_meta:attach_location({uuid, FileUuid}, LocId, oneprovider:get_provider_id()),

    LeafLess = fslogic_path:dirname(FileId),
    SFMHandle0 = storage_file_manager:new_handle(?ROOT_SESS_ID, SpaceDirUuid, FileUuid, Storage, LeafLess),
    ?debug("create_storage_file (dirs) ~p ~p", [Storage, LeafLess]),
    case storage_file_manager:mkdir(SFMHandle0, ?AUTO_CREATED_PARENT_DIR_MODE, true) of
        ok -> ok;
        {error, eexist} ->
            ok
    end,
    ?debug("create_storage_file (file) ~p ~p", [Storage, FileId]),
    SFMHandle1 = storage_file_manager:new_handle(SessId, SpaceDirUuid, FileUuid, Storage, FileId),
    storage_file_manager:unlink(SFMHandle1),
    ok = storage_file_manager:create(SFMHandle1, Mode),
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
    {renamed, file_location:doc(), file_meta:uuid(), onedata_user:id(), space_info:id()}
    | skipped | deleted .
rename_or_delete(_, undefined) ->
    skipped;
rename_or_delete(#document{value = #file_location{last_rename = {_, LocalNum}}},
    {_, ExternalNum}) when LocalNum >= ExternalNum ->
    skipped;
rename_or_delete(Doc = #document{value = Loc = #file_location{uuid = UUID,
    blocks = OldBlocks}}, {{TargetFileId, TargetSpaceId}, _} = LastRename) ->
    {ok, Auth} = session:get_auth(?ROOT_SESS_ID),
    {ok, #document{value = #space_info{providers = Providers}}} = space_info:get_or_fetch(Auth, TargetSpaceId, ?ROOT_USER_ID),
    TargetSpaceProviders = ordsets:from_list(Providers),
    case ordsets:is_element(oneprovider:get_provider_id(), TargetSpaceProviders) of
        true ->
            ok = fslogic_rename:rename_storage_file(?ROOT_SESS_ID, Loc, TargetFileId, TargetSpaceId),

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
-spec chown_file(file_meta:uuid(), onedata_user:id(), space_info:id()) -> ok.
chown_file(FileUuid, UserId, SpaceId) ->
    case onedata_user:exists(UserId) of
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

%%%===================================================================
%%% Internal functions
%%%===================================================================
