%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handing requests operating on file attributes.
%%% @end
%%%--------------------------------------------------------------------
-module(file_req).
-author("Tomasz Lichon").

-include("proto/oneclient/fuse_messages.hrl").
-include_lib("annotations/include/annotations.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([create_file/5, make_file/4, get_file_location/2, open_file/3,
    release/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates and open file. Returns handle to the file, its attributes
%% and location.
%% @end
%%--------------------------------------------------------------------
-spec create_file(user_ctx:ctx(), ParentFileCtx :: file_ctx:ctx(), Name :: file_meta:name(),
    Mode :: file_meta:posix_permissions(), Flags :: fslogic_worker:open_flag()) ->
    fslogic_worker:fuse_response().
-check_permissions([traverse_ancestors, ?traverse_container, ?add_object]).
create_file(UserCtx, ParentFileCtx, Name, Mode, _Flag) ->
    FileCtx = create_file_doc(UserCtx, ParentFileCtx, Name, Mode),
    SessId = user_ctx:get_session_id(UserCtx),
    SpaceId = file_ctx:get_space_id_const(ParentFileCtx),
    {uuid, FileUuid} = file_ctx:get_uuid_entry_const(FileCtx),
    {{StorageId, FileId}, FileCtx2} = sfm_utils:create_storage_file(UserCtx, FileCtx),
    fslogic_times:update_mtime_ctime(ParentFileCtx, user_ctx:get_user_id(UserCtx)),
    {ok, Storage} = fslogic_storage:select_storage(SpaceId),
    SpaceDirUuid = file_ctx:get_space_dir_uuid_const(ParentFileCtx),
    SFMHandle = storage_file_manager:new_handle(SessId, SpaceDirUuid, FileUuid, Storage, FileId),
    {ok, Handle} = storage_file_manager:open_at_creation(SFMHandle),
    {ok, HandleId} = save_handle(SessId, Handle),
    #fuse_response{fuse_response = #file_attr{} = FileAttr} = attr_req:get_file_attr_insecure(UserCtx, FileCtx2),
    FileGuid = file_ctx:get_guid_const(FileCtx2),
    FileLocation = #file_location{
        uuid = FileGuid,
        provider_id = oneprovider:get_provider_id(),
        storage_id = StorageId,
        file_id = FileId,
        blocks = [#file_block{offset = 0, size = 0}],
        space_id = SpaceId
    },
    ok = file_handles:register_open(FileUuid, SessId, 1), %todo pass file_ctx
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
%% Creates file. Returns its attributes.
%% @end
%%--------------------------------------------------------------------
-spec make_file(user_ctx:ctx(), ParentFileCtx :: file_ctx:ctx(), Name :: file_meta:name(),
    Mode :: file_meta:posix_permissions()) -> fslogic_worker:fuse_response().
-check_permissions([traverse_ancestors, ?traverse_container, ?add_object]).
make_file(UserCtx, ParentFileCtx, Name, Mode) ->
    FileCtx = create_file_doc(UserCtx, ParentFileCtx, Name, Mode),
    {_, FileCtx2} = sfm_utils:create_storage_file(UserCtx, FileCtx),
    fslogic_times:update_mtime_ctime(ParentFileCtx, user_ctx:get_user_id(UserCtx)),
    attr_req:get_file_attr_insecure(UserCtx, FileCtx2).

%%--------------------------------------------------------------------
%% @doc
%% Returns file location.
%% @end
%%--------------------------------------------------------------------
-spec get_file_location(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:fuse_response().
-check_permissions([traverse_ancestors]).
get_file_location(_UserCtx, FileCtx) ->
    {#document{key = StorageId}, FileCtx2} = file_ctx:get_storage_doc(FileCtx),
    {[#document{value = #file_location{
        blocks = Blocks, file_id = FileId
    }}], File3} = file_ctx:get_local_file_location_docs(FileCtx2),
    FileGuid = file_ctx:get_guid_const(File3),
    SpaceId = file_ctx:get_space_id_const(File3),

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
%% @equiv open_file(UserCtx, FileCtx, CreateHandle) with permission check
%% depending on the open flag.
%% @end
%%--------------------------------------------------------------------
-spec open_file(user_ctx:ctx(), FileCtx :: file_ctx:ctx(),
    OpenFlag :: fslogic_worker:open_flag()) -> no_return() | #fuse_response{}.
open_file(UserCtx, FileCtx, read) ->
    open_file_for_read(UserCtx, FileCtx);
open_file(UserCtx, FileCtx, write) ->
    open_file_for_write(UserCtx, FileCtx);
open_file(UserCtx, FileCtx, rdwr) ->
    open_file_for_rdwr(UserCtx, FileCtx).

%%--------------------------------------------------------------------
%% @doc
%% Removes file handle saved in session.
%% @end
%%--------------------------------------------------------------------
-spec release(user_ctx:ctx(), file_ctx:ctx(), HandleId :: binary()) ->
    fslogic_worker:fuse_response().
release(UserCtx, FileCtx, HandleId) ->
    SessId = user_ctx:get_session_id(UserCtx),
    ok = session:remove_handle(SessId, HandleId),
    {uuid, FileUuid} = file_ctx:get_uuid_entry_const(FileCtx),
    ok = file_handles:register_release(FileUuid, SessId, 1), %todo pass file_ctx
    #fuse_response{status = #status{code = ?OK}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates file_meta and times documents for the new file.
%% @end
%%--------------------------------------------------------------------
-spec create_file_doc(user_ctx:ctx(), file_ctx:ctx(), file_meta:name(), file_meta:mode()) ->
    ChildFile :: file_ctx:ctx().
create_file_doc(UserCtx, ParentFileCtx, Name, Mode)  ->
    File = #document{value = #file_meta{
        name = Name,
        type = ?REGULAR_FILE_TYPE,
        mode = Mode,
        owner = user_ctx:get_user_id(UserCtx)
    }},
    ParentFileEntry = file_ctx:get_uuid_entry_const(ParentFileCtx),
    {ok, FileUuid} = file_meta:create(ParentFileEntry, File), %todo pass file_ctx

    CTime = erlang:system_time(seconds),
    {ok, _} = times:create(#document{key = FileUuid, value = #times{
        mtime = CTime, atime = CTime, ctime = CTime
    }}),

    SpaceId = file_ctx:get_space_id_const(ParentFileCtx),
    file_ctx:new_by_guid(fslogic_uuid:uuid_to_guid(FileUuid, SpaceId)).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Saves file handle in user's session, returns id of saved handle.
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
%% @private
%% @equiv open_file_insecure(UserCtx, FileCtx, read, CreateHandle)
%% with permission check.
%% @end
%%--------------------------------------------------------------------
-spec open_file_for_read(user_ctx:ctx(), file_ctx:ctx()) ->
    no_return() | #fuse_response{}.
-check_permissions([traverse_ancestors, ?read_object]).
open_file_for_read(UserCtx, FileCtx) ->
    open_file_insecure(UserCtx, FileCtx, read).

%%--------------------------------------------------------------------
%% @private
%% @equiv open_file_insecure(UserCtx, FileCtx, write, CreateHandle)
%% with permission check.
%% @end
%%--------------------------------------------------------------------
-spec open_file_for_write(user_ctx:ctx(), file_ctx:ctx()) ->
    no_return() | #fuse_response{}.
-check_permissions([traverse_ancestors, ?write_object]).
open_file_for_write(UserCtx, FileCtx) ->
    open_file_insecure(UserCtx, FileCtx, write).

%%--------------------------------------------------------------------
%% @private
%% @equiv open_file_insecure(UserCtx, FileCtx, rdwr, CreateHandle)
%% with permission check.
%% @end
%%--------------------------------------------------------------------
-spec open_file_for_rdwr(user_ctx:ctx(), file_ctx:ctx()) ->
    no_return() | #fuse_response{}.
-check_permissions([traverse_ancestors, ?read_object, ?write_object]).
open_file_for_rdwr(UserCtx, FileCtx) ->
    open_file_insecure(UserCtx, FileCtx, rdwr).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Opens a file and returns a handle to it.
%% @end
%%--------------------------------------------------------------------
-spec open_file_insecure(user_ctx:ctx(), FileCtx :: file_ctx:ctx(),
    fslogic_worker:open_flag()) -> no_return() | #fuse_response{}.
open_file_insecure(UserCtx, FileCtx, Flag) ->
    {StorageDoc, FileCtx2} = file_ctx:get_storage_doc(FileCtx),
    {uuid, FileUuid} = file_ctx:get_uuid_entry_const(FileCtx2),
    {[#document{value = #file_location{
        file_id = FileId}
    }], FileCtx3} = file_ctx:get_local_file_location_docs(FileCtx2),
    SpaceDirUuid = file_ctx:get_space_dir_uuid_const(FileCtx3),
    SessId = user_ctx:get_session_id(UserCtx),
    ShareId = file_ctx:get_share_id_const(FileCtx3),

    SFMHandle = storage_file_manager:new_handle(SessId, SpaceDirUuid, FileUuid, StorageDoc, FileId, ShareId), %todo pass file_ctx
    {ok, Handle} = storage_file_manager:open(SFMHandle, Flag),
    {ok, HandleId} = save_handle(SessId, Handle),

    ok = file_handles:register_open(FileUuid, SessId, 1), %todo pass file_ctx

    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = #file_opened{handle_id = HandleId}
    }.