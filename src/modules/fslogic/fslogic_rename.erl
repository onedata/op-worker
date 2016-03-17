%%%-------------------------------------------------------------------
%%% @author Mateusz Paciorek
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc 
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(fslogic_rename).
-author("Mateusz Paciorek").

%% TODO future work:
%% 1. If any provider supporting old path does not support new path
%%    then get his changes, other providers should update their locations
%% 2. Add 'hint' for fslogic_storage:select_storage, to suggest using
%%    source storage if possible to avoid copying
%% 3. Add rollback or any other means of rescuing from failed renaming

-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("annotations/include/annotations.hrl").

%% API
-export([rename/3]).
-export([get_all_regular_files/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Executes proper rename case to check permissions.
%% For best performance use following arg types: path -> uuid -> document
%% @end
%%--------------------------------------------------------------------
-spec rename(fslogic_worker:ctx(), SourceEntry :: fslogic_worker:file(),
    TargetPath :: file_meta:path()) ->
    #fuse_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}, {traverse_ancestors, {path, 3}}, {?delete, 2}]).
rename(CTX, SourceEntry, TargetPath) ->
    ?debug("Renaming file ~p to ~p...", [SourceEntry, TargetPath]),
    case file_meta:get(SourceEntry) of
        {ok, #document{value = #file_meta{type = ?DIRECTORY_TYPE}} = FileDoc} ->
            rename_dir(CTX, FileDoc, TargetPath);
        {ok, FileDoc} ->
            rename_file(CTX, FileDoc, TargetPath)
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc Checks necessary permissions and renames directory
%%--------------------------------------------------------------------
-spec rename_dir(fslogic_worker:ctx(), fslogic_worker:file(), file_meta:path()) ->
    #fuse_response{} | no_return().
-check_permissions([{?delete_subcontainer, {parent, 2}}, {?add_subcontainer, {parent, {path, 3}}}]).
rename_dir(CTX, SourceEntry, TargetPath) ->
    case check_dir_preconditions(CTX, SourceEntry, TargetPath) of
        #fuse_response{status = #status{code = ?OK}} ->
            rename_select(CTX, SourceEntry, TargetPath);
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc Checks necessary permissions and renames file
%%--------------------------------------------------------------------
-spec rename_file(fslogic_worker:ctx(), fslogic_worker:file(), file_meta:path()) ->
    #fuse_response{} | no_return().
-check_permissions([{?delete_object, {parent, 2}}, {?add_object, {parent, {path, 3}}}]).
rename_file(CTX, SourceEntry, TargetPath) ->
    case check_reg_preconditions(CTX, SourceEntry, TargetPath) of
        #fuse_response{status = #status{code = ?OK}} ->
            rename_select(CTX, SourceEntry, TargetPath);
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc Checks preconditions for renaming directory.
%%--------------------------------------------------------------------
-spec check_dir_preconditions(fslogic_worker:ctx(), fslogic_worker:file(),
    file_meta:path()) -> #fuse_response{} | no_return().
check_dir_preconditions(CTX, SourceEntry, TargetPath) ->
    case file_meta:exists({path, TargetPath}) of
        false ->
            #fuse_response{status = #status{code = ?OK}};
        true ->
            case file_meta:get({path, TargetPath}) of
                {ok, #document{value = #file_meta{type = ?DIRECTORY_TYPE}} = TargetDoc} ->
                    case moving_into_itself(SourceEntry, TargetPath) of
                        true ->
                            #fuse_response{status = #status{code = ?EINVAL}};
                        false ->
                            fslogic_req_generic:delete(CTX, TargetDoc)
                    end;
                {ok, _TargetDoc} ->
                    #fuse_response{status = #status{code = ?ENOTDIR}}
            end
    end.


%%--------------------------------------------------------------------
%% @doc Checks preconditions for renaming regular file.
%%--------------------------------------------------------------------
-spec check_reg_preconditions(fslogic_worker:ctx(), fslogic_worker:file(),
    file_meta:path()) -> #fuse_response{} | no_return().
check_reg_preconditions(CTX, _SourceEntry, TargetPath) ->
    case file_meta:exists({path, TargetPath}) of
        false ->
            #fuse_response{status = #status{code = ?OK}};
        true ->
            case file_meta:get({path, TargetPath}) of
                {ok, #document{value = #file_meta{type = ?DIRECTORY_TYPE}}} ->
                    #fuse_response{status = #status{code = ?EISDIR}};
                {ok, TargetDoc} ->
                    fslogic_req_generic:delete(CTX, TargetDoc)
            end
    end.

%%--------------------------------------------------------------------
%% @doc Checks if renamed entry is one of target path parents.
%%--------------------------------------------------------------------
-spec moving_into_itself(SourceEntry :: fslogic_worker:file(), TargetPath :: file_meta:path()) ->
    boolean().
moving_into_itself(SourceEntry, TargetPath) ->
    {ok, #document{key = SourceUUID}} = file_meta:get(SourceEntry),
    {_, ParentPath} = fslogic_path:basename_and_parent(TargetPath),
    {ok, {_, ParentUUIDs}} = file_meta:resolve_path(ParentPath),
    lists:any(fun(ParentUUID) -> ParentUUID =:= SourceUUID end, ParentUUIDs).

%%--------------------------------------------------------------------
%% @doc Selects proper rename function - trivial, inter-space or inter-provider.
%%--------------------------------------------------------------------
-spec rename_select(fslogic_worker:ctx(), fslogic_worker:file(), file_meta:path()) ->
    #fuse_response{} | no_return().
rename_select(CTX, SourceEntry, TargetPath) ->
    {_, TargetParentPath} = fslogic_path:basename_and_parent(TargetPath),
    SourceSpaceUUID = get_space_uuid(CTX, SourceEntry),
    TargetSpaceUUID = get_space_uuid(CTX, {path, TargetParentPath}),
    ?critical("~p -> ~p", [SourceSpaceUUID, TargetSpaceUUID]),

    case SourceSpaceUUID =:= TargetSpaceUUID of
        true ->
            rename_interspace(CTX, SourceEntry, TargetPath);
        false ->
            TargetSpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(TargetSpaceUUID),
            {ok, TargetProviderIds} =
                gr_spaces:get_providers(provider, TargetSpaceId),
            TargetProvidersSet = ordsets:from_list(TargetProviderIds),
            SourceSpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(SourceSpaceUUID),
            {ok, SourceProviderIds} =
                gr_spaces:get_providers(provider, SourceSpaceId),
            SourceProvidersSet = ordsets:from_list(SourceProviderIds),
            CommonProvidersSet = ordsets:intersection([TargetProvidersSet, SourceProvidersSet]),
            case ordsets:is_element(oneprovider:get_provider_id(), CommonProvidersSet) of
                true ->
                    rename_interspace(CTX, SourceEntry, TargetPath);
                false ->
                    rename_interprovider(CTX, SourceEntry, TargetPath)
            end
    end.

%%--------------------------------------------------------------------
%% @doc Renames file within one provider.
%%--------------------------------------------------------------------
-spec rename_interspace(fslogic_worker:ctx(), fslogic_worker:file(),
    file_meta:path()) -> #fuse_response{} | no_return().
rename_interspace(CTX, SourceEntry, TargetPath) ->
    {ok, SourcePath} = file_meta:gen_path(SourceEntry),
    {ok, SourceParent} = file_meta:get_parent(SourceEntry),
    {_, TargetParentPath} = fslogic_path:basename_and_parent(TargetPath),
    SourcePathTokens = filename:split(SourcePath),
    TargetPathTokens = filename:split(TargetPath),
    SourceSpaceUUID = get_space_uuid(CTX, SourceEntry),
    TargetSpaceUUID = get_space_uuid(CTX, {path, TargetParentPath}),

    case file_meta:get(SourceEntry) of
        {ok, #document{value = #file_meta{type = ?DIRECTORY_TYPE}}} ->
            %% TODO: get all snapshots:
            SourceDirSnapshots = [SourceEntry],
            lists:foreach(
                fun(Snapshot) ->
                    ok = file_meta:rename(Snapshot, {path, TargetPath})
                end, SourceDirSnapshots);
        _ ->
            ok
    end,

    RegFiles = get_all_regular_files(SourceEntry),
    lists:foreach(
        fun(File) ->
            {ok, OldPath} = file_meta:gen_path(File),
            OldTokens = filename:split(OldPath),
            NewTokens = TargetPathTokens ++ lists:sublist(OldTokens, length(SourcePathTokens) + 1, length(OldTokens)),
            NewPath = fslogic_path:join(NewTokens),

            %% TODO: get all snapshots:
            FileSnapshots = [File],
            lists:foreach(
                fun(Snapshot) ->
                    file_meta:rename(Snapshot, {path, NewPath}),
                    rename_on_storage(CTX, SourceSpaceUUID, TargetSpaceUUID, Snapshot, NewPath)
                end, FileSnapshots)
        end, RegFiles),

    CurrTime = erlang:system_time(seconds),
    update_ctime({path, TargetPath}, CurrTime),
    update_mtime_ctime(SourceParent, CurrTime),
    update_mtime_ctime({path, TargetParentPath}, CurrTime),

    #fuse_response{status = #status{code = ?OK}}.

%%--------------------------------------------------------------------
%% @doc Renames file moving it to another space supported by another provider.
%%--------------------------------------------------------------------
-spec rename_interprovider(fslogic_worker:ctx(), fslogic_worker:file(),
    file_meta:path()) -> #fuse_response{} | no_return().
rename_interprovider(_CTX, _SourceEntry, _TargetPath) ->
    %% TODO: implement
    #fuse_response{status = #status{code = ?OK}}.

%%--------------------------------------------------------------------
%% @doc returns space UUID for given entry
%%--------------------------------------------------------------------
-spec get_space_uuid(fslogic_worker:ctx(), fslogic_worker:file()) ->
    binary().
get_space_uuid(CTX, Entry) ->
    UserId = fslogic_context:get_user_id(CTX),
    {ok, Doc} = file_meta:get(Entry),
    {ok, #document{key = SpaceUUID}} =
        fslogic_spaces:get_space(Doc, UserId),
    SpaceUUID.

%%--------------------------------------------------------------------
%% @doc Renames file on storage and all its locations.
%%--------------------------------------------------------------------
-spec rename_on_storage(fslogic_worker:ctx(), SourceSpaceUUID :: binary(),
    TargetSpaceUUID :: binary(), SourceEntry :: file_meta:entry(),
    TargetPath :: file_meta:path()) -> ok.
rename_on_storage(CTX, SourceSpaceUUID, TargetSpaceUUID, SourceEntry, TargetPath) ->
    #fslogic_ctx{session_id = SessId} = CTX,
    lists:foreach(
        fun(LocationDoc) ->
            #document{value = #file_location{storage_id = SourceStorageId,
                file_id = SourceFileId}} = LocationDoc,
            {ok, #document{key = SourceUUID}} = file_meta:get(SourceEntry),

            {ok, SourceStorage} = storage:get(SourceStorageId),
            TargetSpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(TargetSpaceUUID),
            {ok, #document{key = TargetStorageId}} = fslogic_storage:select_storage(TargetSpaceId),

            SourceHandle = storage_file_manager:new_handle(SessId, SourceSpaceUUID, SourceUUID, SourceStorage, SourceFileId),

            Name = fslogic_path:basename(TargetPath),

            CTime = erlang:system_time(seconds),
            file_meta:update(SourceUUID, #{name => Name, mtime => CTime, ctime => CTime}),

            TargetFileId = fslogic_utils:gen_storage_file_id({uuid, SourceUUID}),

            %% TODO: try: link and delete, mv, copy, panic
            storage_file_manager:mv(SourceHandle, TargetFileId),

            update_location(LocationDoc, TargetFileId, TargetSpaceUUID, TargetStorageId)

        end, fslogic_utils:get_local_file_locations(SourceEntry)),

    ok.

%%--------------------------------------------------------------------
%% @doc Updates file location.
%%--------------------------------------------------------------------
-spec update_location(LocationDoc :: datastore:document(),
    TargetFileId :: helpers:file(), TargetSpaceUUID :: binary(),
    TargetStorageId :: storage:id()) -> ok.
update_location(LocationDoc, TargetFileId, TargetSpaceUUID, TargetStorageId) ->
    #document{key = LocationId,
        value = #file_location{blocks = Blocks}} = LocationDoc,
    UpdatedBlocks = lists:map(
        fun(Block) ->
            Block#file_block{file_id = TargetFileId, storage_id = TargetStorageId}
        end, Blocks),
    file_location:update(LocationId, #{
        file_id => TargetFileId,
        space_id => TargetSpaceUUID,
        storage_id => TargetStorageId,
        blocks => UpdatedBlocks
    }),
    ok.

%%--------------------------------------------------------------------
%% @doc Returns all regular files from directory tree starting in given entry
%%--------------------------------------------------------------------
-spec get_all_regular_files(Entry :: fslogic_worker:file()) ->
    [fslogic_worker:file()].
get_all_regular_files(Entry) ->
    get_all_regular_files(Entry, []).

-spec get_all_regular_files(Entry :: fslogic_worker:file(),
    AccIn :: [fslogic_worker:file()]) -> [fslogic_worker:file()].
get_all_regular_files(Entry, AccIn) ->
    case file_meta:get(Entry) of
        {ok, #document{value = #file_meta{type = ?REGULAR_FILE_TYPE}} = FileDoc} ->
            [FileDoc | AccIn];
        {ok, DirDoc} ->
            {ok, ChildrenLinks} = list_all_children(DirDoc),
            lists:foldl(
                fun(#child_link{uuid = ChildUUID}, Acc) ->
                    get_all_regular_files({uuid, ChildUUID}, Acc)
                end, AccIn, ChildrenLinks)
    end.

%%--------------------------------------------------------------------
%% @doc Lists all children of given entry
%%--------------------------------------------------------------------
-spec list_all_children(Entry :: fslogic_worker:file()) ->
    {ok, [#child_link{}]}.
list_all_children(Entry) ->
    list_all_children(Entry, 0, 100, []).

-spec list_all_children(Entry :: fslogic_worker:file(),
    Offset :: non_neg_integer(), Count :: non_neg_integer(),
    AccIn :: [#child_link{}]) -> {ok, [#child_link{}]}.
list_all_children(Entry, Offset, Size, AccIn) ->
    {ok, ChildrenLinks} = file_meta:list_children(Entry, Offset, Size),
    case length(ChildrenLinks) of
        Size ->
            list_all_children(Entry, Offset+Size, Size, AccIn ++ ChildrenLinks);
        _ ->
            {ok, AccIn ++ ChildrenLinks}
    end.

%%--------------------------------------------------------------------
%% @doc Updates entry mtime and ctime
%%--------------------------------------------------------------------
-spec update_mtime_ctime(fslogic_worker:file(), CurrTime :: integer()) -> ok.
update_mtime_ctime(Entry, CurrTime) ->
    {ok, #document{value = Meta} = Doc} = file_meta:get(Entry),
    {ok, _} = file_meta:update(Doc, #{mtime => CurrTime, ctime => CurrTime}),

    spawn(
        fun() ->
            fslogic_event:emit_file_sizeless_attrs_update(
                Doc#document{value = Meta#file_meta{
                    mtime = CurrTime, ctime = CurrTime
                }})
        end),
    ok.

%%--------------------------------------------------------------------
%% @doc Updates entry ctime
%%--------------------------------------------------------------------
-spec update_ctime(fslogic_worker:file(), CurrTime :: integer()) -> ok.
update_ctime(Entry, CurrTime) ->
    {ok, #document{value = Meta} = Doc} = file_meta:get(Entry),
    {ok, _} = file_meta:update(Doc, #{ctime => CurrTime}),

    spawn(
        fun() ->
            fslogic_event:emit_file_sizeless_attrs_update(
                Doc#document{value = Meta#file_meta{ctime = CurrTime}})
        end),
    ok.