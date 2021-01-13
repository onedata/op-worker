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
    get_file_location/2, open_file/3, open_file/4, open_file_insecure/4,
    open_file_with_extended_info/3, storage_file_created_insecure/2,
    fsync/4, release/3, flush_event_queue/2]).

%% Export for RPC
-export([open_on_storage/4]).

%% Test API
-export([create_file_doc/4]).

-type handle_id() :: storage_driver:handle_id() | undefined.
-type new_file() :: boolean(). % opening new file requires changes in procedure (see file_handles:creation_handle/0).
-export_type([handle_id/0]).

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
create_file(UserCtx, ParentFileCtx0, Name, Mode, Flag) ->
    ParentFileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, ParentFileCtx0,
        [traverse_ancestors, ?traverse_container, ?add_object]
    ),
    create_file_insecure(UserCtx, ParentFileCtx1, Name, Mode, Flag).


%%--------------------------------------------------------------------
%% @equiv storage_file_created_insecure/5 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec storage_file_created(user_ctx:ctx(), FileCtx :: file_ctx:ctx()) ->
    fslogic_worker:fuse_response().
storage_file_created(UserCtx, FileCtx0) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [traverse_ancestors, ?traverse_container, ?add_object]
    ),
    storage_file_created_insecure(UserCtx, FileCtx1).


%%--------------------------------------------------------------------
%% @equiv make_file_insecure/4 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec make_file(user_ctx:ctx(), ParentFileCtx :: file_ctx:ctx(), Name :: file_meta:name(),
    Mode :: file_meta:posix_permissions()) -> fslogic_worker:fuse_response().
make_file(UserCtx, ParentFileCtx0, Name, Mode) ->
    ParentFileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, ParentFileCtx0,
        [traverse_ancestors, ?traverse_container, ?add_object]
    ),
    make_file_insecure(UserCtx, ParentFileCtx1, Name, Mode).


%%--------------------------------------------------------------------
%% @equiv get_file_location_insecure/2 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec get_file_location(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:fuse_response().
get_file_location(UserCtx, FileCtx0) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [traverse_ancestors]
    ),
    get_file_location_insecure(UserCtx, FileCtx1).


%%--------------------------------------------------------------------
%% @equiv open_file(UserCtx, FileCtx, OpenFlag, undefined).
%% @end
%%--------------------------------------------------------------------
-spec open_file(user_ctx:ctx(), FileCtx :: file_ctx:ctx(),
    OpenFlag :: fslogic_worker:open_flag()) -> no_return() | #fuse_response{}.
open_file(UserCtx, FileCtx, OpenFlag) ->
    open_file(UserCtx, FileCtx, OpenFlag, undefined).


%%--------------------------------------------------------------------
%% @equiv open_file_insecure(UserCtx, FileCtx, OpenFlag, HandleId)
%% with permission check depending on the open flag.
%% @end
%%--------------------------------------------------------------------
-spec open_file(user_ctx:ctx(), FileCtx :: file_ctx:ctx(),
    OpenFlag :: fslogic_worker:open_flag(), handle_id()) ->
    no_return() | #fuse_response{}.
open_file(UserCtx, FileCtx, read, HandleId) ->
    open_file_for_read(UserCtx, FileCtx, HandleId);
open_file(UserCtx, FileCtx, write, HandleId) ->
    open_file_for_write(UserCtx, FileCtx, HandleId);
open_file(UserCtx, FileCtx, rdwr, HandleId) ->
    open_file_for_rdwr(UserCtx, FileCtx, HandleId).


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
%% @equiv fsync_insecure(UserCtx, FileCtx, DataOnly) with permission check
%% @end
%%--------------------------------------------------------------------
-spec fsync(user_ctx:ctx(), FileCtx :: file_ctx:ctx(),
    boolean(), binary()) -> no_return() | #fuse_response{}.
fsync(UserCtx, FileCtx0, DataOnly, HandleId) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [traverse_ancestors]
    ),
    fsync_insecure(UserCtx, FileCtx1, DataOnly, HandleId).


%%--------------------------------------------------------------------
%% @doc
%% Removes file handle saved in session.
%% @end
%%--------------------------------------------------------------------
-spec release(user_ctx:ctx(), file_ctx:ctx(), HandleId :: binary()) ->
    fslogic_worker:fuse_response().
release(UserCtx, FileCtx, HandleId) ->
    SessId = user_ctx:get_session_id(UserCtx),
    ok = file_handles:register_release(FileCtx, SessId, 1),
    ok = case session_handles:get(SessId, HandleId) of
        {ok, SDHandle} ->
            ok = session_handles:remove(SessId, HandleId),
            ok = storage_driver:release(SDHandle);
        {error, {not_found, _}} ->
            ok;
        {error, not_found} ->
            ok;
        Other ->
            Other
    end,
    {CanonicalPath, _} = file_ctx:get_canonical_path(FileCtx),
    case file_meta:is_child_of_hidden_dir(CanonicalPath) of
        true ->
            ok;
        false ->
            case file_popularity:increment_open(FileCtx) of
                ok -> ok;
                {error, not_found} -> ok % file might have been deleted
            end
    end,
    #fuse_response{status = #status{code = ?OK}}.


%%%===================================================================
%%% Private insecure API functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates and opens file.
%% As a result the file's metadata is created as well as the file on
%% storage.
%% Returns handle to the file, its attributes and location.
%% @end
%%--------------------------------------------------------------------
-spec create_file_insecure(user_ctx:ctx(), ParentFileCtx :: file_ctx:ctx(), Name :: file_meta:name(),
    Mode :: file_meta:posix_permissions(), Flags :: fslogic_worker:open_flag()) ->
    fslogic_worker:fuse_response().
create_file_insecure(UserCtx, ParentFileCtx, Name, Mode, _Flag) ->
    ParentFileCtx2 = file_ctx:assert_not_readonly_storage(ParentFileCtx),
    {FileCtx, ParentFileCtx3} = ?MODULE:create_file_doc(UserCtx, ParentFileCtx2, Name, Mode),
    try
        % TODO VFS-5267 - default open mode will fail if read-only file is created
        {HandleId, FileLocation, FileCtx2} = open_file_internal(UserCtx, FileCtx, rdwr, undefined, true, false),
        fslogic_times:update_mtime_ctime(ParentFileCtx3),

        #fuse_response{fuse_response = FileAttr} = attr_req:get_file_attr_insecure(UserCtx, FileCtx, #{
            allow_deleted_files => false,
            include_size => false,
            name_conflicts_resolution_policy => allow_name_conflicts
        }),
        FileAttr2 = FileAttr#file_attr{size = 0, fully_replicated = true},
        ok = fslogic_event_emitter:emit_file_attr_changed(FileCtx2, FileAttr2, [user_ctx:get_session_id(UserCtx)]),
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
            sd_utils:unlink(FileCtx, UserCtx),
            FileUuid = file_ctx:get_uuid_const(FileCtx),
            fslogic_location_cache:delete_local_location(FileUuid),
            file_meta:delete(FileUuid),
            times:delete(FileUuid),
            case Reason of
                {badmatch, {error, not_found}} -> erlang:Error(?ECANCELED);
                _ -> erlang:Error(Reason)
            end
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
    ParentFileCtx2 = file_ctx:assert_not_readonly_storage(ParentFileCtx),
    {FileCtx, ParentFileCtx3} = ?MODULE:create_file_doc(UserCtx, ParentFileCtx2, Name, Mode),
    try
        {_, FileCtx2} = location_and_link_utils:create_new_file_location_doc(FileCtx, false, true),
        fslogic_times:update_mtime_ctime(ParentFileCtx3),
        #fuse_response{fuse_response = FileAttr} = Ans = attr_req:get_file_attr_insecure(UserCtx, FileCtx, #{
            allow_deleted_files => false,
            include_size => false,
            name_conflicts_resolution_policy => allow_name_conflicts
        }),
        FileAttr2 = FileAttr#file_attr{size = 0, fully_replicated = true},
        ok = fslogic_event_emitter:emit_file_attr_changed(FileCtx2, FileAttr2, [user_ctx:get_session_id(UserCtx)]),
        Ans#fuse_response{fuse_response = FileAttr2}
    catch
        Error:Reason ->
            FileUuid = file_ctx:get_uuid_const(FileCtx),
            fslogic_location_cache:delete_local_location(FileUuid),
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
get_file_location_insecure(UserCtx, FileCtx) ->
    {ok, FileCtx2} = check_if_file_exists_or_is_opened(FileCtx, user_ctx:get_session_id(UserCtx)),
    {StorageId, FileCtx3} = file_ctx:get_storage_id(FileCtx2),
    {#document{
        value = #file_location{
            blocks = Blocks,
            file_id = FileId
    }}, FileCtx4} = file_ctx:get_or_create_local_file_location_doc(FileCtx3),
    FileUuid = file_ctx:get_uuid_const(FileCtx4),
    SpaceId = file_ctx:get_space_id_const(FileCtx4),

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
%% Opens a file and returns a handle to it.
%% @end
%%--------------------------------------------------------------------
-spec open_file_insecure(user_ctx:ctx(), FileCtx :: file_ctx:ctx(),
    fslogic_worker:open_flag(), handle_id()) ->
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
%% @private
%% @doc
%% @equiv open_file_with_extended_info_insecure(UserCtx, FileCtx, Flag, undefined).
%% @end
%%--------------------------------------------------------------------
-spec open_file_with_extended_info_insecure(user_ctx:ctx(),
    FileCtx :: file_ctx:ctx(), fslogic_worker:open_flag()) ->
    no_return() | #fuse_response{}.
open_file_with_extended_info_insecure(UserCtx, FileCtx, Flag) ->
    open_file_with_extended_info_insecure(UserCtx, FileCtx, Flag, undefined).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Opens a file and returns a handle to it with extended information about location.
%% @end
%%--------------------------------------------------------------------
-spec open_file_with_extended_info_insecure(user_ctx:ctx(),
    FileCtx :: file_ctx:ctx(), fslogic_worker:open_flag(), handle_id()) ->
    no_return() | #fuse_response{}.
open_file_with_extended_info_insecure(UserCtx, FileCtx, Flag, HandleId0) ->
    {HandleId, #file_location{provider_id = ProviderId, file_id = FileId, storage_id = StorageId}, _} =
        open_file_internal(UserCtx, FileCtx, Flag, HandleId0, false),
    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = #file_opened_extended{handle_id = HandleId,
            provider_id = ProviderId, file_id = FileId, storage_id = StorageId}
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% @equiv open_file_internal(UserCtx, FileCtx, Flag, HandleId, VerifyDeletionLink, true)
%% @end
%%--------------------------------------------------------------------
-spec open_file_internal(user_ctx:ctx(),
    FileCtx :: file_ctx:ctx(), fslogic_worker:open_flag(), handle_id(), boolean()) ->
    no_return() | {storage_driver:handle_id(), file_location:record(), file_ctx:ctx()}.
open_file_internal(UserCtx, FileCtx, Flag, HandleId, VerifyDeletionLink) ->
    open_file_internal(UserCtx, FileCtx, Flag, HandleId, VerifyDeletionLink, true).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Opens a file and returns a handle id and location.
%% @end
%%--------------------------------------------------------------------
-spec open_file_internal(user_ctx:ctx(),
    FileCtx :: file_ctx:ctx(), fslogic_worker:open_flag(), handle_id(), new_file(), boolean()) ->
    no_return() | {storage_driver:handle_id(), file_location:record(), file_ctx:ctx()}.
open_file_internal(UserCtx, FileCtx0, Flag, HandleId0, NewFile, CheckLocationExists) ->
    FileCtx1 = case Flag == read of
        true -> FileCtx0;
        false -> file_ctx:assert_not_readonly_storage(FileCtx0)
    end,
    FileCtx2 = verify_file_exists(FileCtx1, HandleId0),
    SpaceID = file_ctx:get_space_id_const(FileCtx2),
    SessId = user_ctx:get_session_id(UserCtx),
    HandleId = check_and_register_open(FileCtx2, SessId, HandleId0, NewFile),
    try
        {FileLocation, FileCtx3} = create_location(FileCtx2, UserCtx, NewFile, CheckLocationExists),
        IsDirectIO = user_ctx:is_direct_io(UserCtx, SpaceID) andalso HandleId0 =:= undefined,
        maybe_open_on_storage(FileCtx3, SessId, Flag, IsDirectIO, HandleId),
        {HandleId, FileLocation, FileCtx3}
    catch
        throw:?EROFS ->
            % this error is thrown on attempt to open file for writing on a readonly storage
            throw(?EROFS);
        throw:?ENOENT ->
            % this error is thrown on race between opening the file and deleting it on storage
            ?debug_stacktrace("Open file error: ENOENT for uuid ~p", [file_ctx:get_uuid_const(FileCtx2)]),
            check_and_register_release(FileCtx2, SessId, HandleId0),
            throw(?ENOENT);
        Error:Reason ->
            ?error_stacktrace("Open file error: ~p:~p for uuid ~p",
                [Error, Reason, file_ctx:get_uuid_const(FileCtx2)]),
            check_and_register_release(FileCtx2, SessId, HandleId0),
            throw(Reason)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Opens a file on storage if needed. Chooses appropriate node.
%% @end
%%--------------------------------------------------------------------
-spec maybe_open_on_storage(file_ctx:ctx(), session:id(), fslogic_worker:open_flag(),
    DirectIO :: boolean(), handle_id()) -> ok | no_return().
maybe_open_on_storage(_FileCtx, _SessId, _Flag, true, _) ->
    ok; % Files are not open on server-side when client uses directIO
maybe_open_on_storage(FileCtx, SessId, Flag, _DirectIO, HandleId) ->
    Node = read_write_req:get_proxyio_node(file_ctx:get_uuid_const(FileCtx)),
    case rpc:call(Node, ?MODULE, open_on_storage, [FileCtx, SessId, Flag, HandleId]) of
        ok -> ok;
        {error, Reason} ->
            throw(Reason)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Opens a file on storage.
%% @end
%%--------------------------------------------------------------------
-spec open_on_storage(file_ctx:ctx(), session:id(), fslogic_worker:open_flag(),
    handle_id()) -> ok | no_return().
open_on_storage(FileCtx, SessId, Flag, HandleId) ->
    {SDHandle, _FileCtx2} = storage_driver:new_handle(SessId, FileCtx),
    SDHandle2 = storage_driver:set_size(SDHandle),
    case storage_driver:open(SDHandle2, Flag) of
        {ok, Handle} ->
            ok = session_handles:add(SessId, HandleId, Handle);
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Verifies handle file exists (only for newly opened files).
%% @end
%%--------------------------------------------------------------------
-spec verify_file_exists(file_ctx:ctx(), handle_id()) ->
    file_ctx:ctx() | no_return().
verify_file_exists(FileCtx, undefined) ->
    {#document{}, FileCtx2} = file_ctx:get_file_doc(FileCtx),
    FileCtx2;
verify_file_exists(FileCtx, _HandleId) ->
    FileCtx.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Verifies handle id and registers it.
%% @end
%%--------------------------------------------------------------------
-spec check_and_register_open(file_ctx:ctx(), session:id(), handle_id(), new_file()) ->
    storage_driver:handle_id() | no_return().
check_and_register_open(FileCtx, SessId, undefined, true) ->
    HandleId = ?NEW_HANDLE_ID,
    ok = file_handles:register_open(FileCtx, SessId, 1, HandleId),
    HandleId;
check_and_register_open(FileCtx, SessId, undefined, false) ->
    ok = file_handles:register_open(FileCtx, SessId, 1, undefined),
    ?NEW_HANDLE_ID;
check_and_register_open(_FileCtx, _SessId, HandleId, _NewFile) ->
    HandleId.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Verifies handle id and releases it.
%% @end
%%--------------------------------------------------------------------
-spec check_and_register_release(file_ctx:ctx(), session:id(), handle_id()) ->
    ok | no_return().
check_and_register_release(FileCtx, SessId, undefined) ->
    ok = file_handles:register_release(FileCtx, SessId, 1);
check_and_register_release(_FileCtx, _SessId, _HandleId) ->
    ok.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates location and storage file if extended directIO is set.
%% @end
%%--------------------------------------------------------------------
-spec create_location(file_ctx:ctx(), user_ctx:ctx(), boolean(), boolean()) ->
    {file_location:record(), file_ctx:ctx()}.
create_location(FileCtx, UserCtx, VerifyDeletionLink, CheckLocationExists) ->
    case sd_utils:create_deferred(FileCtx, UserCtx, VerifyDeletionLink, CheckLocationExists) of
        {#document{value = FL}, FileCtx2} ->
            {FL, FileCtx2};
        {error, Reason} ->
            throw(Reason)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates file_meta and times documents for the new file.
%% @end
%%--------------------------------------------------------------------
-spec create_file_doc(user_ctx:ctx(), file_ctx:ctx(), file_meta:name(), file_meta:mode()) ->
    {ChildFile :: file_ctx:ctx(), ParentFileCtx2 :: file_ctx:ctx()} | no_return().
create_file_doc(UserCtx, ParentFileCtx, Name, Mode)  ->
    case file_ctx:is_dir(ParentFileCtx) of
        {true, ParentFileCtx2} ->
            Owner = user_ctx:get_user_id(UserCtx),
            ParentUuid = file_ctx:get_uuid_const(ParentFileCtx2),
            SpaceId = file_ctx:get_space_id_const(ParentFileCtx2),
            File = file_meta:new_doc(Name, ?REGULAR_FILE_TYPE, Mode, Owner, ParentUuid, SpaceId),
            {ok, #document{key = FileUuid}} = file_meta:create({uuid, ParentUuid}, File), %todo pass file_ctx
            CTime = global_clock:timestamp_seconds(),
            {ok, _} = times:save(#document{key = FileUuid, value = #times{
                mtime = CTime, atime = CTime, ctime = CTime
            }, scope = SpaceId}),

            {file_ctx:new_by_guid(file_id:pack_guid(FileUuid, SpaceId)), ParentFileCtx2};
        {false, _} ->
            throw(?ENOTDIR)
    end.


%%--------------------------------------------------------------------
%% @private
%% @equiv open_file_insecure/3 with permission check.
%% @end
%%--------------------------------------------------------------------
-spec open_file_for_read(user_ctx:ctx(), file_ctx:ctx(), handle_id()) ->
    no_return() | #fuse_response{}.
open_file_for_read(UserCtx, FileCtx0, HandleId) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [traverse_ancestors, ?read_object]
    ),
    open_file_insecure(UserCtx, FileCtx1, read, HandleId).


%%--------------------------------------------------------------------
%% @private
%% @equiv open_file_insecure/3 with permission check.
%% @end
%%--------------------------------------------------------------------
-spec open_file_for_write(user_ctx:ctx(), file_ctx:ctx(), handle_id()) ->
    no_return() | #fuse_response{}.
open_file_for_write(UserCtx, FileCtx0, HandleId) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [traverse_ancestors, ?write_object]
    ),
    open_file_insecure(UserCtx, FileCtx1, write, HandleId).


%%--------------------------------------------------------------------
%% @private
%% @equiv open_file_insecure/3 with permission check.
%% @end
%%--------------------------------------------------------------------
-spec open_file_for_rdwr(user_ctx:ctx(), file_ctx:ctx(), handle_id()) ->
    no_return() | #fuse_response{}.
open_file_for_rdwr(UserCtx, FileCtx0, HandleId) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [traverse_ancestors, ?read_object, ?write_object]
    ),
    open_file_insecure(UserCtx, FileCtx1, rdwr, HandleId).


%%--------------------------------------------------------------------
%% @private
%% @equiv open_file_with_extended_info_insecure/3 with permission check.
%% @end
%%--------------------------------------------------------------------
-spec open_file_with_extended_info_for_read(user_ctx:ctx(), file_ctx:ctx()) ->
    no_return() | #fuse_response{}.
open_file_with_extended_info_for_read(UserCtx, FileCtx0) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [traverse_ancestors, ?read_object]
    ),
    open_file_with_extended_info_insecure(UserCtx, FileCtx1, read).


%%--------------------------------------------------------------------
%% @private
%% @equiv open_file_with_extended_info_insecure/3 with permission check.
%% @end
%%--------------------------------------------------------------------
-spec open_file_with_extended_info_for_write(user_ctx:ctx(), file_ctx:ctx()) ->
    no_return() | #fuse_response{}.
open_file_with_extended_info_for_write(UserCtx, FileCtx0) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [traverse_ancestors, ?write_object]
    ),
    open_file_with_extended_info_insecure(UserCtx, FileCtx1, write).


%%--------------------------------------------------------------------
%% @private
%% @equiv open_file_with_extended_info_insecure/3 with permission check.
%% @end
%%--------------------------------------------------------------------
-spec open_file_with_extended_info_for_rdwr(user_ctx:ctx(), file_ctx:ctx()) ->
    no_return() | #fuse_response{}.
open_file_with_extended_info_for_rdwr(UserCtx, FileCtx0) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [traverse_ancestors, ?read_object, ?write_object]
    ),
    open_file_with_extended_info_insecure(UserCtx, FileCtx1, rdwr).


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
            storage_driver:fsync(Handle, DataOnly);
        {error, {not_found, _}} ->
            ok;
        {error, not_found} ->
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
%% Throws ?ENOENT if file does not exist or
%% has been deleted and is not opened within session.
%% @end
%%--------------------------------------------------------------------
-spec check_if_file_exists_or_is_opened(file_ctx:ctx(), session:id()) -> {ok, file_ctx:ctx()} | no_return().
check_if_file_exists_or_is_opened(FileCtx, SessionId) ->
    case file_ctx:file_exists_or_is_deleted(FileCtx) of
        {?FILE_EXISTS, FileCtx2} ->
            {ok, FileCtx2};
        {?FILE_NEVER_EXISTED, _} ->
            throw(?ENOENT);
        {?FILE_DELETED, FileCtx2} ->
            case file_handles:is_used_by_session(FileCtx2, SessionId) of
                true -> {ok, FileCtx2};
                false -> throw(?ENOENT)
            end
    end.

