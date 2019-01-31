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
-export([create_file/5, storage_file_created/2, make_file/4,
    get_file_location/2, open_file/3, open_file_with_extended_info/3,
    open_file_insecure/3, storage_file_created_insecure/2,
    open_file_insecure/4, open_file_with_extended_info_insecure/3,
    fsync/4, release/3, flush_event_queue/2]).

%% Test API
-export([create_file_doc/4]).

-define(NEW_HANDLE_ID, base64:encode(crypto:strong_rand_bytes(20))).

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
%% @equiv storage_file_created_insecure/5 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec storage_file_created(user_ctx:ctx(), FileCtx :: file_ctx:ctx()) ->
    fslogic_worker:fuse_response().
storage_file_created(UserCtx, FileCtx) ->
    check_permissions:execute(
        [traverse_ancestors, ?traverse_container, ?add_object],
        [UserCtx, FileCtx],
        fun storage_file_created_insecure/2).

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
%% @equiv open_file_with_extended_info(UserCtx, FileCtx) with permission check
%% depending on the open flag.
%% @end
%%--------------------------------------------------------------------
-spec open_file_with_extended_info(user_ctx:ctx(), FileCtx :: file_ctx:ctx(),
    OpenFlag :: fslogic_worker:open_flag()) -> no_return() | #fuse_response{}.
open_file_with_extended_info(UserCtx, FileCtx, read) ->
    open_file_with_extended_info_for_read(UserCtx, FileCtx);
open_file_with_extended_info(UserCtx, FileCtx, write) ->
    open_file_with_extended_info_for_write(UserCtx, FileCtx);
open_file_with_extended_info(UserCtx, FileCtx, rdwr) ->
    open_file_with_extended_info_for_rdwr(UserCtx, FileCtx).

%%--------------------------------------------------------------------
%% @equiv open_file_insecure(UserCtx, FileCtx, Flag, undefined).
%% @end
%%--------------------------------------------------------------------
-spec open_file_insecure(user_ctx:ctx(), FileCtx :: file_ctx:ctx(),
    fslogic_worker:open_flag()) -> no_return() | #fuse_response{}.
open_file_insecure(UserCtx, FileCtx, Flag) ->
    open_file_insecure(UserCtx, FileCtx, Flag, undefined).

%%--------------------------------------------------------------------
%% @doc
%% Opens a file and returns a handle to it.
%% @end
%%--------------------------------------------------------------------
-spec open_file_insecure(user_ctx:ctx(), FileCtx :: file_ctx:ctx(),
    fslogic_worker:open_flag(), storage_file_manager:handle_id() | undefined) ->
    no_return() | #fuse_response{}.
open_file_insecure(UserCtx, FileCtx, Flag, HandleId0) ->
    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = #file_opened_extended{handle_id = HandleId}
    } = open_file_with_extended_info_insecure(UserCtx, FileCtx, Flag, HandleId0),
    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = #file_opened{handle_id = HandleId}
    }.

%%--------------------------------------------------------------------
%% @doc
%% Opens a file and returns a handle to it with extended information about location.
%% @end
%%--------------------------------------------------------------------
-spec open_file_with_extended_info_insecure(user_ctx:ctx(),
    FileCtx :: file_ctx:ctx(), fslogic_worker:open_flag()) ->
    no_return() | #fuse_response{}.
open_file_with_extended_info_insecure(UserCtx, FileCtx, Flag) ->
    open_file_with_extended_info_insecure(UserCtx, FileCtx, Flag, undefined).

open_file_with_extended_info_insecure(UserCtx, FileCtx, Flag, HandleId0) ->
    SessId = user_ctx:get_session_id(UserCtx),
    {#document{value = #file_location{provider_id = ProviderId, file_id = FileId,
        storage_id = StorageId}}, FileCtx2} =
        sfm_utils:create_delayed_storage_file(FileCtx),
    {SFMHandle, FileCtx3} = storage_file_manager:new_handle(SessId, FileCtx2),
    SFMHandle2 = storage_file_manager:set_size(SFMHandle),
    {ok, Handle} = storage_file_manager:open(SFMHandle2, Flag),
    {ok, HandleId} = save_handle(SessId, Handle, HandleId0),
    ok = file_handles:register_open(FileCtx3, SessId, 1),
    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = #file_opened_extended{handle_id = HandleId,
            provider_id = ProviderId, file_id = FileId, storage_id = StorageId}
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
    ok = case session_handles:get(SessId, HandleId) of
        {ok, SfmHandle} ->
            ok = session_handles:remove(SessId, HandleId),
            ok = file_handles:register_release(FileCtx, SessId, 1),
            ok = storage_file_manager:release(SfmHandle);
        {error, link_not_found} ->
            ok;
        {error, {not_found, _}} ->
            ok;
        Other ->
            Other
    end,
    {CanonicalPath, _} = file_ctx:get_canonical_path(FileCtx),
    case file_meta:is_child_of_hidden_dir(CanonicalPath) of
        true ->
            ok;
        false ->
            ok = file_popularity:increment_open(FileCtx)
    end,
    #fuse_response{status = #status{code = ?OK}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates and opens file. Returns handle to the file, its attributes
%% and location.
%% @end
%%--------------------------------------------------------------------
-spec create_file_insecure(user_ctx:ctx(), ParentFileCtx :: file_ctx:ctx(), Name :: file_meta:name(),
    Mode :: file_meta:posix_permissions(), Flags :: fslogic_worker:open_flag()) ->
    fslogic_worker:fuse_response().
create_file_insecure(UserCtx, ParentFileCtx, Name, Mode, _Flag) ->
    FileCtx = create_file_doc(UserCtx, ParentFileCtx, Name, Mode),
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    try
        ExtDIO = file_ctx:get_extended_direct_io_const(FileCtx),
        FileCtx2 = case ExtDIO of
            true ->
                FileCtx;
            _ ->
                sfm_utils:create_storage_file(UserCtx, FileCtx)
        end,
        {FileLocation, FileCtx3} =
            file_location_utils:get_new_file_location_doc(FileCtx2, not ExtDIO, true),
        fslogic_times:update_mtime_ctime(ParentFileCtx),

        HandleId = case user_ctx:is_direct_io(UserCtx) of
            true ->
                ?NEW_HANDLE_ID;
            _ ->
                % open file on adequate node
                Node = consistent_hasing:get_node(FileUuid),
                #fuse_response{
                    status = #status{code = ?OK},
                    fuse_response = #file_opened{handle_id = HId}
                } = rpc:call(Node, file_req, open_file_insecure,
                    [UserCtx, FileCtx3, rdwr]),
                HId
        end,

        #fuse_response{fuse_response = #file_attr{size = Size} = FileAttr} =
            attr_req:get_file_attr_insecure(UserCtx, FileCtx3, false, false),
        FileAttr2 = case Size of
            undefined ->
                FileAttr#file_attr{size = 0};
            _ ->
                FileAttr
        end,
        #fuse_response{
        status = #status{code = ?OK},
            fuse_response = #file_created{
                handle_id = HandleId,
                file_attr = FileAttr2,
                file_location = FileLocation
            }
        }
    catch
        Error:Reason ->
            ?error_stacktrace("create_file_insecure error: ~p:~p",
                [Error, Reason]),
            sfm_utils:delete_storage_file(FileCtx, UserCtx),
            file_meta:delete(FileUuid),
            times:delete(FileUuid),
            erlang:Error(Reason)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Confirms that file was created on storage.
%% @end
%%--------------------------------------------------------------------
-spec storage_file_created_insecure(user_ctx:ctx(), FileCtx :: file_ctx:ctx()) ->
    fslogic_worker:fuse_response().
storage_file_created_insecure(_UserCtx, FileCtx) ->
    {#document{
        key = FileLocationId,
        value = #file_location{storage_file_created = StorageFileCreated}
    }, FileCtx2} = file_ctx:get_or_create_local_file_location_doc(FileCtx, false),

    case StorageFileCreated of
        false ->
            FileUuid = file_ctx:get_uuid_const(FileCtx),
            UpdateAns = fslogic_location_cache:update_location(FileUuid, FileLocationId, fun
                (#file_location{storage_file_created = true}) ->
                    {error, already_created};
                (FileLocation = #file_location{storage_file_created = false}) ->
                    {ok, FileLocation#file_location{storage_file_created = true}}
            end, false),

            case UpdateAns of
                {ok, #document{}} ->
                    #fuse_response{
                        status = #status{code = ?OK}
                    };
                {error, already_created} ->
                    #fuse_response{
                        status = #status{code = ?EAGAIN,
                            description = <<"Location_update_error">>}
                    };
                _ ->
                    FileCtx2
            end;
        true ->
            #fuse_response{
                status = #status{code = ?OK}
            }
    end.

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
    try
        {_, FileCtx2} = file_location_utils:get_new_file_location_doc(FileCtx, false, true),
        fslogic_times:update_mtime_ctime(ParentFileCtx),
        attr_req:get_file_attr_insecure(UserCtx, FileCtx2)
    catch
        Error:Reason ->
            FileUuid = file_ctx:get_uuid_const(FileCtx),
            file_location_utils:delete_file_location(FileCtx),
            file_meta:delete(FileUuid),
            times:delete(FileUuid),
            erlang:Error(Reason)
    end.

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
    {#document{
        value = #file_location{
            blocks = Blocks,
            file_id = FileId
    }}, FileCtx3} = file_ctx:get_or_create_local_file_location_doc(FileCtx2),
    FileUuid = file_ctx:get_uuid_const(FileCtx3),
    SpaceId = file_ctx:get_space_id_const(FileCtx3),

    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = #file_location{
            uuid = FileUuid,
            provider_id = oneprovider:get_id(),
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

    CTime = time_utils:cluster_time_seconds(),
    SpaceId = file_ctx:get_space_id_const(ParentFileCtx),
    {ok, _} = times:save(#document{key = FileUuid, value = #times{
        mtime = CTime, atime = CTime, ctime = CTime
    }, scope = SpaceId}),

    file_ctx:new_by_guid(fslogic_uuid:uuid_to_guid(FileUuid, SpaceId)).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Saves file handle in user's session, returns id of saved handle.
%% @end
%%--------------------------------------------------------------------
-spec save_handle(session:id(), storage_file_manager:handle(),
    storage_file_manager:handle_id() | undefined) -> {ok, binary()}.
save_handle(SessId, Handle, HandleId0) ->
    HandleId = case HandleId0 of
        undefined ->
            ?NEW_HANDLE_ID;
        _ ->
            HandleId0
    end,
    session_handles:add(SessId, HandleId, Handle),
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
%% @equiv open_file_with_extended_info_insecure/3 with permission check.
%% @end
%%--------------------------------------------------------------------
-spec open_file_with_extended_info_for_read(user_ctx:ctx(), file_ctx:ctx()) ->
    no_return() | #fuse_response{}.
open_file_with_extended_info_for_read(UserCtx, FileCtx) ->
    check_permissions:execute(
        [traverse_ancestors, ?read_object],
        [UserCtx, FileCtx, read],
        fun open_file_with_extended_info_insecure/3).

%%--------------------------------------------------------------------
%% @private
%% @equiv open_file_with_extended_info_insecure/3 with permission check.
%% @end
%%--------------------------------------------------------------------
-spec open_file_with_extended_info_for_write(user_ctx:ctx(), file_ctx:ctx()) ->
    no_return() | #fuse_response{}.
open_file_with_extended_info_for_write(UserCtx, FileCtx) ->
    check_permissions:execute(
        [traverse_ancestors, ?write_object],
        [UserCtx, FileCtx, write],
        fun open_file_with_extended_info_insecure/3).

%%--------------------------------------------------------------------
%% @private
%% @equiv open_file_with_extended_info_insecure/3 with permission check.
%% @end
%%--------------------------------------------------------------------
-spec open_file_with_extended_info_for_rdwr(user_ctx:ctx(), file_ctx:ctx()) ->
    no_return() | #fuse_response{}.
open_file_with_extended_info_for_rdwr(UserCtx, FileCtx) ->
    check_permissions:execute(
        [traverse_ancestors, ?read_object, ?write_object],
        [UserCtx, FileCtx, rdwr],
        fun open_file_with_extended_info_insecure/3).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Flushes events and fsyncs file on storage
%% @end
%%--------------------------------------------------------------------
-spec fsync_insecure(user_ctx:ctx(), FileCtx :: file_ctx:ctx(),
    boolean(), binary()) -> #fuse_response{}.
fsync_insecure(UserCtx, FileCtx, _DataOnly, undefined) ->
    Ans = flush_event_queue(UserCtx, FileCtx),
    case fslogic_location_cache:force_flush(file_ctx:get_uuid_const(FileCtx)) of
        ok ->
            Ans;
        _ ->
            #fuse_response{
                status = #status{code = ?EAGAIN,
                    description = <<"Blocks_flush_error">>}
            }
    end;
fsync_insecure(UserCtx, FileCtx, DataOnly, HandleId) ->
    SessId = user_ctx:get_session_id(UserCtx),
    ok = case session_handles:get(SessId, HandleId) of
        {ok, Handle} ->
            storage_file_manager:fsync(Handle, DataOnly);
        {error, link_not_found} ->
            ok;
        {error, {not_found, _}} ->
            ok;
        Other ->
            Other
    end,

    Ans = flush_event_queue(UserCtx, FileCtx),
    case fslogic_location_cache:force_flush(file_ctx:get_uuid_const(FileCtx)) of
        ok ->
            Ans;
        _ ->
            #fuse_response{
                status = #status{code = ?EAGAIN,
                    description = <<"Blocks_flush_error">>}
            }
    end.

%%--------------------------------------------------------------------
%% @doc
%% Flush event queue of session
%% @end
%%--------------------------------------------------------------------
-spec flush_event_queue(user_ctx:ctx(), file_ctx:ctx()) -> #fuse_response{}.
flush_event_queue(UserCtx, FileCtx) ->
    SessId = user_ctx:get_session_id(UserCtx),
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    case lfm_event_controller:flush_event_queue(SessId, oneprovider:get_id(),
        FileUuid) of
        ok ->
            #fuse_response{
                status = #status{code = ?OK}
            };
        _ ->
            #fuse_response{
                status = #status{code = ?EAGAIN,
                    description = <<"Events_flush_error">>}
            }
    end.

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