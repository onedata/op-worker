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
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([create_file/5, make_file/4, get_file_location/2, open_file/3,
    open_file_insecure/3, fsync/4, release/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @equiv create_file_insecure/5 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec create_file(user_ctx:ctx(), ParentFileCtx :: file_ctx:ctx(), Name :: file_meta:name(),
    Mode :: file_meta:posix_permissions(), Flags :: fslogic_worker:open_flag()) ->
    fslogic_worker:fuse_response().
create_file(UserCtx, ParentFileCtx, Name, Mode, _Flag) ->
    check_permissions:execute(
        [traverse_ancestors, ?traverse_container, ?add_object],
        [UserCtx, ParentFileCtx, Name, Mode, _Flag],
        fun create_file_insecure/5).

%%--------------------------------------------------------------------
%% @equiv make_file_insecure/4 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec make_file(user_ctx:ctx(), ParentFileCtx :: file_ctx:ctx(), Name :: file_meta:name(),
    Mode :: file_meta:posix_permissions()) -> fslogic_worker:fuse_response().
make_file(UserCtx, ParentFileCtx, Name, Mode) ->
    check_permissions:execute(
        [traverse_ancestors, ?traverse_container, ?add_object],
        [UserCtx, ParentFileCtx, Name, Mode],
        fun make_file_insecure/4).

%%--------------------------------------------------------------------
%% @equiv get_file_location_insecure/2 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec get_file_location(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:fuse_response().
get_file_location(_UserCtx, FileCtx) ->
    check_permissions:execute(
        [traverse_ancestors],
        [_UserCtx, FileCtx],
        fun get_file_location_insecure/2).

%%--------------------------------------------------------------------
%% @equiv open_file(UserCtx, FileCtx) with permission check
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
%% Opens a file and returns a handle to it.
%% @end
%%--------------------------------------------------------------------
-spec open_file_insecure(user_ctx:ctx(), FileCtx :: file_ctx:ctx(),
    fslogic_worker:open_flag()) -> no_return() | #fuse_response{}.
open_file_insecure(UserCtx, FileCtx, Flag) ->
    SessId = user_ctx:get_session_id(UserCtx),
    SFMHandle = storage_file_manager:new_handle(SessId, FileCtx),
    {ok, Handle} = storage_file_manager:open(SFMHandle, Flag),
    {ok, HandleId} = save_handle(SessId, Handle),
    ok = file_handles:register_open(FileCtx, SessId, 1),
    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = #file_opened{handle_id = HandleId}
    }.

%%--------------------------------------------------------------------
%% @equiv fsync_insecure(UserCtx, FileCtx, DataOnly) with permission check
%% @end
%%--------------------------------------------------------------------
-spec fsync(user_ctx:ctx(), FileCtx :: file_ctx:ctx(),
    boolean(), binary()) -> no_return() | #fuse_response{}.
fsync(UserCtx, FileCtx, DataOnly, HandleId) ->
    check_permissions:execute(
        [traverse_ancestors],
        [UserCtx, FileCtx, DataOnly, HandleId],
        fun fsync_insecure/4).

%%--------------------------------------------------------------------
%% @doc
%% Removes file handle saved in session.
%% @end
%%--------------------------------------------------------------------
-spec release(user_ctx:ctx(), file_ctx:ctx(), HandleId :: binary()) ->
    fslogic_worker:fuse_response().
release(UserCtx, FileCtx, HandleId) ->
    SessId = user_ctx:get_session_id(UserCtx),
    {ok, SfmHandle} = session:get_handle(SessId, HandleId),
    ok = session:remove_handle(SessId, HandleId),
    ok = file_handles:register_release(FileCtx, SessId, 1),
    ok = storage_file_manager:release(SfmHandle),
    #fuse_response{status = #status{code = ?OK}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates and open file. Returns handle to the file, its attributes
%% and location.
%% @end
%%--------------------------------------------------------------------
-spec create_file_insecure(user_ctx:ctx(), ParentFileCtx :: file_ctx:ctx(), Name :: file_meta:name(),
    Mode :: file_meta:posix_permissions(), Flags :: fslogic_worker:open_flag()) ->
    fslogic_worker:fuse_response().
create_file_insecure(UserCtx, ParentFileCtx, Name, Mode, _Flag) ->
    FileCtx = create_file_doc(UserCtx, ParentFileCtx, Name, Mode),
    SpaceId = file_ctx:get_space_id_const(ParentFileCtx),
    {{StorageId, FileId}, FileCtx2} = sfm_utils:create_storage_file(UserCtx, FileCtx),
    fslogic_times:update_mtime_ctime(ParentFileCtx),

    FileGuid = file_ctx:get_guid_const(FileCtx2),
    Node = consistent_hasing:get_node(FileGuid),
    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = #file_opened{handle_id = HandleId}
    } = rpc:call(Node, file_req, open_file_insecure,
        [UserCtx, FileCtx2, rdwr]),

    #fuse_response{fuse_response = #file_attr{} = FileAttr} =
        attr_req:get_file_attr_insecure(UserCtx, FileCtx2),
    FileLocation = #file_location{
        uuid = file_ctx:get_uuid_const(FileCtx2),
        provider_id = oneprovider:get_provider_id(),
        storage_id = StorageId,
        file_id = FileId,
        space_id = SpaceId
    },
    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = #file_created{
            handle_id = HandleId,
            file_attr = FileAttr,
            file_location = FileLocation
        }
    }.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates file. Returns its attributes.
%% @end
%%--------------------------------------------------------------------
-spec make_file_insecure(user_ctx:ctx(), ParentFileCtx :: file_ctx:ctx(), Name :: file_meta:name(),
    Mode :: file_meta:posix_permissions()) -> fslogic_worker:fuse_response().
make_file_insecure(UserCtx, ParentFileCtx, Name, Mode) ->
    FileCtx = create_file_doc(UserCtx, ParentFileCtx, Name, Mode),
    {_, FileCtx2} = sfm_utils:create_storage_file(UserCtx, FileCtx),
    fslogic_times:update_mtime_ctime(ParentFileCtx),
    attr_req:get_file_attr_insecure(UserCtx, FileCtx2).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns file location.
%% @end
%%--------------------------------------------------------------------
-spec get_file_location_insecure(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:fuse_response().
get_file_location_insecure(_UserCtx, FileCtx) ->
    throw_if_not_exists(FileCtx),
    {#document{key = StorageId}, FileCtx2} = file_ctx:get_storage_doc(FileCtx),
    {[#document{value = #file_location{
        blocks = Blocks, file_id = FileId
    }}], FileCtx3} = file_ctx:get_local_file_location_docs(FileCtx2),
    FileUuid = file_ctx:get_uuid_const(FileCtx3),
    SpaceId = file_ctx:get_space_id_const(FileCtx3),

    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = #file_location{
            uuid = FileUuid,
            provider_id = oneprovider:get_provider_id(),
            storage_id = StorageId,
            file_id = FileId,
            blocks = Blocks,
            space_id = SpaceId
        }
    }.

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
    ParentFileUuid = file_ctx:get_uuid_const(ParentFileCtx),
    {ok, FileUuid} = file_meta:create({uuid, ParentFileUuid}, File), %todo pass file_ctx

    CTime = erlang:system_time(seconds),
    SpaceId = file_ctx:get_space_id_const(ParentFileCtx),
    {ok, _} = times:create(#document{key = FileUuid, value = #times{
        mtime = CTime, atime = CTime, ctime = CTime
    }, scope = SpaceId}),

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
    HandleId = base64:encode(crypto:strong_rand_bytes(20)),
    session:add_handle(SessId, HandleId, Handle),
    {ok, HandleId}.

%%--------------------------------------------------------------------
%% @private
%% @equiv open_file_insecure/3 with permission check.
%% @end
%%--------------------------------------------------------------------
-spec open_file_for_read(user_ctx:ctx(), file_ctx:ctx()) ->
    no_return() | #fuse_response{}.
open_file_for_read(UserCtx, FileCtx) ->
    check_permissions:execute(
        [traverse_ancestors, ?read_object],
        [UserCtx, FileCtx, read],
        fun open_file_insecure/3).

%%--------------------------------------------------------------------
%% @private
%% @equiv open_file_insecure/3 with permission check.
%% @end
%%--------------------------------------------------------------------
-spec open_file_for_write(user_ctx:ctx(), file_ctx:ctx()) ->
    no_return() | #fuse_response{}.
open_file_for_write(UserCtx, FileCtx) ->
    check_permissions:execute(
        [traverse_ancestors, ?write_object],
        [UserCtx, FileCtx, write],
        fun open_file_insecure/3).

%%--------------------------------------------------------------------
%% @private
%% @equiv open_file_insecure/3 with permission check.
%% @end
%%--------------------------------------------------------------------
-spec open_file_for_rdwr(user_ctx:ctx(), file_ctx:ctx()) ->
    no_return() | #fuse_response{}.
open_file_for_rdwr(UserCtx, FileCtx) ->
    check_permissions:execute(
        [traverse_ancestors, ?read_object, ?write_object],
        [UserCtx, FileCtx, rdwr],
        fun open_file_insecure/3).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Flushes events and fsyncs file on storage
%% @end
%%--------------------------------------------------------------------
-spec fsync_insecure(user_ctx:ctx(), FileCtx :: file_ctx:ctx(),
    boolean(), binary()) -> #fuse_response{}.
fsync_insecure(UserCtx, FileCtx, _DataOnly, undefined) ->
    flush_event_queue(UserCtx, FileCtx);
fsync_insecure(UserCtx, FileCtx, DataOnly, HandleId) ->
    SessId = user_ctx:get_session_id(UserCtx),
    {ok, Handle} = session:get_handle(SessId, HandleId),
    storage_file_manager:fsync(Handle, DataOnly),
    flush_event_queue(UserCtx, FileCtx).

%%--------------------------------------------------------------------
%% @doc
%% Flush event queue of session
%% @end
%%--------------------------------------------------------------------
-spec flush_event_queue(user_ctx:ctx(), file_ctx:ctx()) -> #fuse_response{}.
flush_event_queue(UserCtx, FileCtx) ->
    SessId = user_ctx:get_session_id(UserCtx),
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    lfm_event_utils:flush_event_queue(SessId, oneprovider:get_provider_id(), FileUuid),
    #fuse_response{
        status = #status{code = ?OK}
    }.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Throws ?ENOENT if file does not exist.
%% @end
%%--------------------------------------------------------------------
-spec throw_if_not_exists(file_ctx:ctx()) -> ok | no_return().
throw_if_not_exists(FileCtx) ->
    case file_ctx:file_exists_const(FileCtx) of
        true -> ok;
        false -> throw(?ENOENT)
    end.