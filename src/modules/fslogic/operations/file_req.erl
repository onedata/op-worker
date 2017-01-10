%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Requests operating on file attributes.
%%% @end
%%%--------------------------------------------------------------------
-module(file_req).
-author("Tomasz Lichon").

-include("proto/oneclient/fuse_messages.hrl").
-include_lib("annotations/include/annotations.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([create_file/5, make_file/4, get_file_location/2, open_file/3, release/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Create and open file. Return handle to the file, its attributes
%% and location.
%% @end
%%--------------------------------------------------------------------
-spec create_file(fslogic_context:ctx(), ParentFile :: file_context:ctx(), Name :: file_meta:name(),
    Mode :: file_meta:posix_permissions(), Flags :: fslogic_worker:open_flag()) ->
    fslogic_worker:fuse_response().
-check_permissions([{traverse_ancestors, 2}, {?add_object, 2}, {?traverse_container, 2}]).
create_file(Ctx, ParentFile, Name, Mode, _Flag) ->
    {File, ParentFile2} = create_file_doc(Ctx, ParentFile, Name, Mode),
    SessId = fslogic_context:get_session_id(Ctx),
    SpaceId = file_context:get_space_id(ParentFile2),
    {{uuid, FileUuid}, File2} = file_context:get_uuid_entry(File),
    {StorageId, FileId} = sfm_utils:create_storage_file(SpaceId, FileUuid, SessId, Mode), %todo pass file_context
    {ParentFileEntry, _ParentFile3} = file_context:get_uuid_entry(ParentFile2),
    fslogic_times:update_mtime_ctime(ParentFileEntry, fslogic_context:get_user_id(Ctx)), %todo pass file_context
    {ok, Storage} = fslogic_storage:select_storage(SpaceId),
    SpaceDirUuid = file_context:get_space_dir_uuid(ParentFile2),
    SFMHandle = storage_file_manager:new_handle(SessId, SpaceDirUuid, FileUuid, Storage, FileId),
    {ok, Handle} = storage_file_manager:open_at_creation(SFMHandle),
    {ok, HandleId} = save_handle(SessId, Handle),
    #fuse_response{fuse_response = #file_attr{} = FileAttr} = attr_req:get_file_attr_no_permission_check(Ctx, File2),
    {FileGuid, _File3} = file_context:get_guid(File2),
    FileLocation = #file_location{
        uuid = FileGuid,
        provider_id = oneprovider:get_provider_id(),
        storage_id = StorageId,
        file_id = FileId,
        blocks = [#file_block{offset = 0, size = 0}],
        space_id = SpaceId
    },
    ok = file_handles:register_open(FileUuid, SessId, 1), %todo pass file_context
    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = #file_created{
            handle_id = HandleId,
            file_attr = FileAttr,
            file_location = FileLocation
        }
    }.

%%--------------------------------------------------------------------
%% @doc
%% Create file. Return its attributes.
%% @end
%%--------------------------------------------------------------------
-spec make_file(fslogic_context:ctx(), ParentFile :: file_context:ctx(), Name :: file_meta:name(),
    Mode :: file_meta:posix_permissions()) -> fslogic_worker:fuse_response().
-check_permissions([{traverse_ancestors, 2}, {?add_object, 2}, {?traverse_container, 2}]).
make_file(Ctx, ParentFile, Name, Mode) ->
    {File, ParentFile2} = create_file_doc(Ctx, ParentFile, Name, Mode),

    SessId = fslogic_context:get_session_id(Ctx),
    SpaceId = file_context:get_space_id(ParentFile2),
    {{uuid, FileUuid}, File2} = file_context:get_uuid_entry(File),
    sfm_utils:create_storage_file(SpaceId, FileUuid, SessId, Mode), %todo pass file_context

    {ParentFileEntry, _ParentFile3} = file_context:get_uuid_entry(ParentFile2),
    fslogic_times:update_mtime_ctime(ParentFileEntry, fslogic_context:get_user_id(Ctx)), %todo pass file_context

    attr_req:get_file_attr_no_permission_check(Ctx, File2).

%%--------------------------------------------------------------------
%% @doc Returns file location.
%% @end
%%--------------------------------------------------------------------
-spec get_file_location(fslogic_context:ctx(), file_context:ctx()) ->
    fslogic_worker:fuse_response().
-check_permissions([{traverse_ancestors, 2}]).
get_file_location(_Ctx, File) ->
    {#document{key = StorageId}, File2} = file_context:get_storage_doc(File),
    {#document{value = #file_location{
        blocks = Blocks, file_id = FileId
    }}, File3} = file_context:get_local_file_location_doc(File2),
    {FileGuid, File4} = file_context:get_guid(File3),
    SpaceId = file_context:get_space_id(File4),

    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = file_location:ensure_blocks_not_empty(#file_location{
            uuid = FileGuid,
            provider_id = oneprovider:get_provider_id(),
            storage_id = StorageId,
            file_id = FileId,
            blocks = Blocks,
            space_id = SpaceId
        })
    }.

%%--------------------------------------------------------------------
%% @doc @equiv open_file(Ctx, File, CreateHandle) with permission check
%% depending on the open flag
%%--------------------------------------------------------------------
-spec open_file(fslogic_context:ctx(), File :: fslogic_worker:file(),
    OpenFlag :: fslogic_worker:open_flag()) -> no_return() | #fuse_response{}.
open_file(Ctx, File, read) ->
    open_file_for_read(Ctx, File);
open_file(Ctx, File, write) ->
    open_file_for_write(Ctx, File);
open_file(Ctx, File, rdwr) ->
    open_file_for_rdwr(Ctx, File).

%%--------------------------------------------------------------------
%% @doc Remove file handle saved in session.
%% @end
%%--------------------------------------------------------------------
-spec release(fslogic_context:ctx(), file_context:ctx(), HandleId :: binary()) ->
    fslogic_worker:fuse_response().
release(Ctx, File, HandleId) ->
    SessId = fslogic_context:get_session_id(Ctx),
    ok = session:remove_handle(SessId, HandleId),
    {{uuid, FileUuid}, _File2} = file_context:get_uuid_entry(File),
    ok = file_handles:register_release(FileUuid, SessId, 1), %todo pass file_context
    #fuse_response{status = #status{code = ?OK}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Create file_meta and times documents for the new file.
%% @end
%%--------------------------------------------------------------------
-spec create_file_doc(fslogic_context:ctx(), file_context:ctx(), file_meta:name(), file_meta:mode()) ->
    {ChildFile :: file_context:ctx(), NewParentFile :: file_context:ctx()}.
create_file_doc(Ctx, ParentFile, Name, Mode)  ->
    File = #document{value = #file_meta{
        name = Name,
        type = ?REGULAR_FILE_TYPE,
        mode = Mode,
        uid = fslogic_context:get_user_id(Ctx)
    }},
    {ParentFileEntry, ParentFile2} = file_context:get_uuid_entry(ParentFile),
    {ok, FileUuid} = file_meta:create(ParentFileEntry, File), %todo pass file_context

    CTime = erlang:system_time(seconds),
    {ok, _} = times:create(#document{key = FileUuid, value = #times{
        mtime = CTime, atime = CTime, ctime = CTime
    }}),

    SpaceId = file_context:get_space_id(ParentFile2),
    {file_context:new_by_guid(fslogic_uuid:uuid_to_guid(FileUuid, SpaceId)), ParentFile2}.

%%--------------------------------------------------------------------
%% @doc Saves file handle in user's session, returns id of saved handle
%% @end
%%--------------------------------------------------------------------
-spec save_handle(session:id(), storage_file_manager:handle()) ->
    {ok, binary()}.
save_handle(SessId, Handle) ->
    HandleId = base64:encode(crypto:rand_bytes(20)),
    case session:is_special(SessId) of
        true -> ok;
        _ -> session:add_handle(SessId, HandleId, Handle)
    end,
    {ok, HandleId}.

%%--------------------------------------------------------------------
%% @equiv open_file_impl(Ctx, File, read, CreateHandle) with permission check
%%--------------------------------------------------------------------
-spec open_file_for_read(fslogic_context:ctx(), fslogic_worker:file()) ->
    no_return() | #fuse_response{}.
-check_permissions([{traverse_ancestors, 2}, {?read_object, 2}]).
open_file_for_read(Ctx, File) ->
    open_file_impl(Ctx, File, read).

%%--------------------------------------------------------------------
%% @equiv open_file_impl(Ctx, File, write, CreateHandle) with permission check
%%--------------------------------------------------------------------
-spec open_file_for_write(fslogic_context:ctx(), fslogic_worker:file()) ->
    no_return() | #fuse_response{}.
-check_permissions([{traverse_ancestors, 2}, {?write_object, 2}]).
open_file_for_write(Ctx, File) ->
    open_file_impl(Ctx, File, write).

%%--------------------------------------------------------------------
%% @equiv open_file_impl(Ctx, File, rdwr, CreateHandle) with permission check
%%--------------------------------------------------------------------
-spec open_file_for_rdwr(fslogic_context:ctx(), fslogic_worker:file()) ->
    no_return() | #fuse_response{}.
-check_permissions([{traverse_ancestors, 2}, {?read_object, 2}, {?write_object, 2}]).
open_file_for_rdwr(Ctx, File) ->
    open_file_impl(Ctx, File, rdwr).

%%--------------------------------------------------------------------
%% @doc Opens a file and returns a handle to it.
%% For best performance use following arg types: document -> uuid -> path
%% @end
%%--------------------------------------------------------------------
-spec open_file_impl(fslogic_context:ctx(), File :: file_context:ctx(),
    fslogic_worker:open_flag()) -> no_return() | #fuse_response{}.
open_file_impl(Ctx, File, Flag) ->
    {StorageDoc, File2} = file_context:get_storage_doc(File),
    {{uuid, FileUuid}, File3} = file_context:get_uuid_entry(File2),
    {#document{value = #file_location{
        file_id = FileId}
    }, File4} = file_context:get_local_file_location_doc(File3),
    SpaceDirUuid = file_context:get_space_dir_uuid(File4),
    SessId = fslogic_context:get_session_id(Ctx),
    ShareId = file_context:get_share_id(File4),

    SFMHandle = storage_file_manager:new_handle(SessId, SpaceDirUuid, FileUuid, StorageDoc, FileId, ShareId), %todo pass file_context
    {ok, Handle} = storage_file_manager:open(SFMHandle, Flag),
    {ok, HandleId} = save_handle(SessId, Handle),

    ok = file_handles:register_open(FileUuid, SessId, 1), %todo pass file_context

    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = #file_opened{handle_id = HandleId}
    }.