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
    create_storage_file/4, get_merged_changes/2]).

-define(MAX_CHANGES, 20).

-type change() :: fslogic_blocks:blocks() | {shrink, non_neg_integer()}.

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
get_changes(#document{value = #file_location{size = Size, recent_changes = {Backup, New}, blocks = Blocks}}, N)
    when N > (length(New) + length(Backup)) ->
    [Blocks, {shrink, Size}];
get_changes(#document{value = #file_location{recent_changes = {_Backup, New}}}, N)
    when N =< length(New) ->
    lists:sublist(New, N);
get_changes(#document{value = #file_location{recent_changes = {Backup, New}}}, N) ->
    lists:sublist(New ++ Backup, N).

%%--------------------------------------------------------------------
%% @doc
%% Get N changes merged into form: {BlocksChangesList, ShrinkSize | undefined}
%% @end
%%--------------------------------------------------------------------
-spec get_merged_changes(file_location:doc(), integer()) ->
    {fslogic_blocks:blocks(), integer() | undefined}.
get_merged_changes(Doc, N) ->
    Changes = get_changes(Doc, N),
    Shrink = lists:foldl(fun
        ({shrink, Size}, MinimalSize) -> min(Size, MinimalSize);
        (_, MinimalSize) -> MinimalSize
    end, undefined, Changes),
    BlockChanges = lists:filter(fun
        ({shrink, _}) -> false;
        (_) -> true
    end, Changes),
    AggregatedBlocks = lists:foldl(fun(Blocks, Acc) ->
        aggregate(Acc, Blocks)
    end, [], BlockChanges),
    {AggregatedBlocks, Shrink}.


%%--------------------------------------------------------------------
%% @doc
%% Create storage file and file_location if there is no file_location defined
%% @end
%%--------------------------------------------------------------------
-spec create_storage_file_if_not_exists(space_info:id(), file_meta:file_meta()) -> ok.
create_storage_file_if_not_exists(SpaceId, FileDoc = #document{key = FileUuid,
    value = #file_meta{mode = Mode, uid = UserId}}) ->
    file_location:run_synchronized(FileUuid,
        fun() ->
            case fslogic_utils:get_local_file_locations(FileDoc) of
                [] ->
                    create_storage_file(SpaceId, FileUuid, ?ROOT_SESS_ID, Mode),
                    case onedata_user:exists(UserId) of
                        true ->
                            files_to_chown:chown_file(FileUuid, UserId, SpaceId);
                        false ->
                            files_to_chown:add(UserId, FileUuid)
                    end,
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
        space_id = SpaceDirUuid},
    {ok, LocId} = file_location:create(#document{value = Location}),
    file_meta:attach_location({uuid, FileUuid}, LocId, oneprovider:get_provider_id()),

    LeafLess = fslogic_path:dirname(FileId),
    SFMHandle0 = storage_file_manager:new_handle(?ROOT_SESS_ID, SpaceDirUuid, undefined, Storage, LeafLess),
    case storage_file_manager:mkdir(SFMHandle0, ?AUTO_CREATED_PARENT_DIR_MODE, true) of
        ok -> ok;
        {error, eexist} ->
            ok
    end,

    SFMHandle1 = storage_file_manager:new_handle(SessId, SpaceDirUuid, FileUuid, Storage, FileId),
    storage_file_manager:unlink(SFMHandle1),
    ok = storage_file_manager:create(SFMHandle1, Mode),
    {StorageId, FileId}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Aggregates and consolidates given blocks lists.
%% @end
%%--------------------------------------------------------------------
-spec aggregate(fslogic_blocks:blocks(), fslogic_blocks:blocks()) -> fslogic_blocks:blocks().
aggregate(Blocks1, Blocks2) ->
    AggregatedBlocks = fslogic_blocks:invalidate(Blocks1, Blocks2) ++ Blocks2,
    fslogic_blocks:consolidate(lists:sort(AggregatedBlocks)).