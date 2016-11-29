%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc FSLogic request handlers for regular files.
%%% @end
%%%-------------------------------------------------------------------
-module(fslogic_req_regular).
-author("Rafal Slota").

-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("annotations/include/annotations.hrl").

%% API
-export([get_file_location/2, open_file/3, create_file/5, make_file/4,
    truncate/3, get_helper_params/3, release/3]).
-export([get_parent/2, synchronize_block/4, synchronize_block_and_compute_checksum/3,
    get_file_distribution/2]).

%%%===================================================================
%%% API functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc Truncates file on storage and returns only if operation is complete. Does not change file size in
%%      #file_meta model. Model's size should be changed by write events.
%% For best performance use following arg types: document -> uuid -> path
%% @end
%%--------------------------------------------------------------------
-spec truncate(fslogic_worker:ctx(), File :: fslogic_worker:file(), Size :: non_neg_integer()) ->
    FuseResponse :: #fuse_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}, {?write_object, 2}]).
truncate(CTX = #fslogic_ctx{session_id = SessionId}, Entry, Size) ->
    {ok, #document{key = FileUUID} = FileDoc} = file_meta:get(Entry),
    {ok, #document{key = SpaceUUID}} = fslogic_spaces:get_space(FileDoc, fslogic_context:get_user_id(CTX)),

    %% START -> Quota check
    SpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(SpaceUUID),
    OldSize = fslogic_blocks:get_file_size(FileDoc),
    ok = space_quota:assert_write(SpaceId, Size - OldSize),
    %% END   -> Quota check

    Results = lists:map(
        fun({SID, FID} = Loc) ->
            {ok, Storage} = storage:get(SID),
            SFMHandle = storage_file_manager:new_handle(SessionId, SpaceUUID, FileUUID, Storage, FID),
            case storage_file_manager:open(SFMHandle, write) of
                {ok, Handle} ->
                    {Loc, storage_file_manager:truncate(Handle, Size)};
                Error ->
                    {Loc, Error}
            end
        end, fslogic_utils:get_local_storage_file_locations(Entry)),

    case [{Loc, Error} || {Loc, {error, _} = Error} <- Results] of
        [] -> ok;
        Errors ->
            [?error("Unable to truncate [FileId: ~p] [StoragId: ~p] to size ~p due to: ~p", [FID, SID, Size, Reason])
                || {{SID, FID}, {error, Reason}} <- Errors],
            ok
    end,

    fslogic_times:update_mtime_ctime(FileDoc, fslogic_context:get_user_id(CTX)),
    #fuse_response{status = #status{code = ?OK}}.


%%--------------------------------------------------------------------
%% @doc Gets helper params based on given storage ID.
%% @end
%%--------------------------------------------------------------------
-spec get_helper_params(fslogic_worker:ctx(), StorageId :: storage:id(),
    ForceCL :: boolean()) -> FuseResponse :: #fuse_response{} | no_return().
get_helper_params(_Ctx, StorageId, true = _ForceProxy) ->
    #fuse_response{status = #status{code = ?OK}, fuse_response = #helper_params{
        helper_name = <<"ProxyIO">>,
        helper_args = [#helper_arg{key = <<"storage_id">>, value = StorageId}]
    }};
get_helper_params(#fslogic_ctx{session_id = SessId, space_id = SpaceId},
    StorageId, false = _ForceProxy) ->
    {ok, StorageDoc} = storage:get(StorageId),
    {ok, HelperInit} = fslogic_storage:select_helper(StorageDoc),
    SpaceUUID = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),
    UserCtx = fslogic_storage:new_user_ctx(HelperInit, SessId, SpaceUUID),
    HelperParams = helpers_utils:get_params(HelperInit, UserCtx),
    #fuse_response{status = #status{code = ?OK}, fuse_response = HelperParams}.

%%--------------------------------------------------------------------
%% @doc Returns file location.
%% @end
%%--------------------------------------------------------------------
-spec get_file_location(fslogic_worker:ctx(), File :: fslogic_worker:file()) ->
    no_return() | #fuse_response{}.
-check_permissions([{traverse_ancestors, 2}]).
get_file_location(#fslogic_ctx{space_id = SpaceId}, File) ->
    {ok, #document{key = FileUUID}} = file_meta:get(File),
    {ok, #document{key = StorageId}} = fslogic_storage:select_storage(SpaceId),
    #document{value = #file_location{
        blocks = Blocks, file_id = FileId
    }} = fslogic_utils:get_local_file_location({uuid, FileUUID}),

    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = file_location:ensure_blocks_not_empty(#file_location{
            uuid = fslogic_uuid:uuid_to_guid(FileUUID, SpaceId),
            provider_id = oneprovider:get_provider_id(),
            storage_id = StorageId,
            file_id = FileId,
            blocks = Blocks,
            space_id = SpaceId
        })
    }.

%%--------------------------------------------------------------------
%% @doc @equiv open_file(CTX, File, CreateHandle) with permission check
%% depending on the open flag
%%--------------------------------------------------------------------
-spec open_file(fslogic_worker:ctx(), File :: fslogic_worker:file(),
    OpenFlag :: fslogic_worker:open_flag()) -> no_return() | #fuse_response{}.
open_file(CTX, File, read) ->
    open_file_for_read(CTX, File);
open_file(CTX, File, write) ->
    open_file_for_write(CTX, File);
open_file(CTX, File, rdwr) ->
    open_file_for_rdwr(CTX, File).

%%--------------------------------------------------------------------
%% @doc Creates and opens file. Returns handle to the file, its attributes
%% and location.
%% @end
%%--------------------------------------------------------------------
-spec create_file(fslogic_worker:ctx(), Parent :: file_meta:entry(), Name :: file_meta:name(),
    Mode :: file_meta:posix_permissions(), Flags :: fslogic_worker:open_flag()) ->
    no_return() | #fuse_response{}.
-check_permissions([{traverse_ancestors, 2}, {?add_object, 2}, {?traverse_container, 2}]).
create_file(#fslogic_ctx{session_id = SessId, space_id = SpaceId} = CTX, {uuid, ParentUUID}, Name, Mode, _Flag) ->
    {ok, #document{key = SpaceUUID}} = fslogic_spaces:get_space({uuid, ParentUUID}, fslogic_context:get_user_id(CTX)),
    CTime = erlang:system_time(seconds),
    File = #document{value = #file_meta{
        name = Name,
        type = ?REGULAR_FILE_TYPE,
        mode = Mode,
        uid = fslogic_context:get_user_id(CTX)
    }},

    {ok, FileUUID} = file_meta:create({uuid, ParentUUID}, File),
    {ok, _} = times:create(#document{key = FileUUID, value = #times{
        mtime = CTime, atime = CTime, ctime = CTime
    }}),

    try fslogic_file_location:create_storage_file(SpaceId, FileUUID, SessId, Mode) of
        {StorageId, FileId} ->
            fslogic_times:update_mtime_ctime({uuid, ParentUUID}, fslogic_context:get_user_id(CTX)),

            {ok, Storage} = fslogic_storage:select_storage(SpaceId),
            SFMHandle = storage_file_manager:new_handle(SessId, SpaceUUID, FileUUID,
                Storage, FileId),
            {ok, Handle} = storage_file_manager:open_at_creation(SFMHandle),
            {ok, HandleId} = save_handle(SessId, Handle),

            #fuse_response{fuse_response = #file_attr{} = FileAttr} =
                fslogic_req_generic:get_file_attr(CTX, {uuid, FileUUID}),

            FileLocation = #file_location{
                uuid = fslogic_uuid:uuid_to_guid(FileUUID, SpaceId),
                provider_id = oneprovider:get_provider_id(),
                storage_id = StorageId,
                file_id = FileId,
                blocks = [#file_block{offset = 0, size = 0}],
                space_id = SpaceId
            },

            ok = file_handles:register_open(FileUUID, SessId, 1),

            #fuse_response{
                status = #status{code = ?OK},
                fuse_response = #file_created{
                    handle_id = HandleId,
                    file_attr = FileAttr,
                    file_location = FileLocation
                }
            }
    catch
        T:M ->
            ?error_stacktrace("Cannot create file on storage - ~p:~p", [T, M]),
            {ok, FileLocations} = file_meta:get_locations({uuid, FileUUID}),
            lists:map(fun(Id) -> file_location:delete(Id) end, FileLocations),
            file_meta:delete({uuid, FileUUID}),
            throw(?EACCES)
    end.

%%--------------------------------------------------------------------
%% @doc Creates file. Returns its attributes.
%% @end
%%--------------------------------------------------------------------
-spec make_file(fslogic_worker:ctx(), Parent :: file_meta:entry(), Name :: file_meta:name(),
    Mode :: file_meta:posix_permissions()) -> no_return() | #fuse_response{}.
-check_permissions([{traverse_ancestors, 2}, {?add_object, 2}, {?traverse_container, 2}]).
make_file(#fslogic_ctx{session_id = SessId, space_id = SpaceId} = CTX, {uuid, ParentUUID}, Name, Mode) ->
    CTime = erlang:system_time(seconds),
    File = #document{value = #file_meta{
        name = Name,
        type = ?REGULAR_FILE_TYPE,
        mode = Mode,
        uid = fslogic_context:get_user_id(CTX)
    }},

    {ok, FileUUID} = file_meta:create({uuid, ParentUUID}, File),
    {ok, _} = times:create(#document{key = FileUUID, value = #times{
        mtime = CTime, atime = CTime, ctime = CTime
    }}),

    try fslogic_file_location:create_storage_file(SpaceId, FileUUID, SessId, Mode) of
        _ ->
            fslogic_times:update_mtime_ctime({uuid, ParentUUID}, fslogic_context:get_user_id(CTX)),
            fslogic_req_generic:get_file_attr(CTX, {uuid, FileUUID})
    catch
        T:M ->
            {ok, FileLocations} = file_meta:get_locations({uuid, FileUUID}),
            lists:map(fun(Id) -> file_location:delete(Id) end, FileLocations),
            file_meta:delete({uuid, FileUUID}),
            ?error_stacktrace("Cannot create file on storage - ~p:~p", [T, M]),
            throw(?EACCES)
    end.


%%--------------------------------------------------------------------
%% @doc Removes file handle saved in session.
%% @end
%%--------------------------------------------------------------------
-spec release(#fslogic_ctx{}, FileUUID :: file_meta:uuid(), HandleId :: binary()) ->
    no_return() | #fuse_response{}.
release(#fslogic_ctx{session_id = SessId}, FileUUID, HandleId) ->
    ok = session:remove_handle(SessId, HandleId),
    ok = file_handles:register_release(FileUUID, SessId, 1),
    #fuse_response{status = #status{code = ?OK}}.


%%--------------------------------------------------------------------
%% @doc Gets parent of file
%% @end
%%--------------------------------------------------------------------
-spec get_parent(CTX :: fslogic_worker:ctx(), File :: fslogic_worker:file()) ->
    ProviderResponse :: #provider_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}]).
get_parent(#fslogic_ctx{share_id = ShareId, space_id = SpaceId}, File) when is_binary(ShareId) ->
    {ok, #document{key = FileUuid, value = #file_meta{shares = Shares}}} = file_meta:get(File),
    case lists:member(ShareId, Shares) of
        true ->
            #provider_response{
                status = #status{code = ?OK},
                provider_response = #dir{uuid =
                fslogic_uuid:uuid_to_share_guid(FileUuid, SpaceId, ShareId)}
            };
        false ->
            {ok, #document{key = ParentUUID}} = file_meta:get_parent(File),
            #provider_response{
                status = #status{code = ?OK},
                provider_response = #dir{uuid =
                fslogic_uuid:uuid_to_share_guid(ParentUUID, SpaceId, ShareId)}
            }
    end;
get_parent(CTX, File) ->
    SpacesBaseDirUUID = ?ROOT_DIR_UUID,
    {ok, #document{key = ParentUUID}} = file_meta:get_parent(File),
    case ParentUUID of
        SpacesBaseDirUUID ->
            #provider_response{
                status = #status{code = ?OK},
                provider_response = #dir{uuid =
                fslogic_uuid:uuid_to_guid(fslogic_uuid:user_root_dir_uuid(fslogic_context:get_user_id(CTX)), undefined)}
            };
        _ ->
            #provider_response{
                status = #status{code = ?OK},
                provider_response = #dir{uuid = fslogic_uuid:uuid_to_guid(ParentUUID)}
            }
    end.


%%--------------------------------------------------------------------
%% @doc
%% Synchronizes given block with remote replicas.
%% @end
%%--------------------------------------------------------------------
-spec synchronize_block(fslogic_worker:ctx(), {uuid, file_meta:uuid()}, fslogic_blocks:block(), boolean()) ->
    #fuse_response{}.
synchronize_block(CTX, {uuid, FileUUID}, undefined, Prefetch) ->
    Size = fslogic_blocks:get_file_size({uuid, FileUUID}),
    synchronize_block(CTX, {uuid, FileUUID}, #file_block{offset = 0, size = Size}, Prefetch);
synchronize_block(CTX, {uuid, FileUUID}, Block, Prefetch) ->
    ok = replica_synchronizer:synchronize(CTX, FileUUID, Block, Prefetch),
    #fuse_response{status = #status{code = ?OK}}.


%%--------------------------------------------------------------------
%% @doc
%% Synchronizes given block with remote replicas and returns checksum of
%% synchronized data.
%% @end
%%--------------------------------------------------------------------
-spec synchronize_block_and_compute_checksum(fslogic_worker:ctx(),
    {uuid, file_meta:uuid()}, fslogic_blocks:block()) -> #fuse_response{}.
synchronize_block_and_compute_checksum(#fslogic_ctx{session_id = SessId}, {uuid, FileUUID},
    Range = #file_block{offset = Offset, size = Size}) ->
    {ok, Handle} = lfm_files:open(SessId, {guid, fslogic_uuid:uuid_to_guid(FileUUID)}, read),
    {ok, _, Data} = lfm_files:read_without_events(Handle, Offset, Size), % does sync internally
    Checksum = crypto:hash(md4, Data),
    LocationToSend =
        fslogic_file_location:prepare_location_for_client({uuid, FileUUID}, Range),
    #fuse_response{status = #status{code = ?OK},
        fuse_response = #sync_response{checksum = Checksum, file_location = LocationToSend}}.


%%--------------------------------------------------------------------
%% @doc
%% Get distribution of file over providers' storages.
%% @end
%%--------------------------------------------------------------------
-spec get_file_distribution(fslogic_worker:ctx(), {uuid, file_meta:uuid()}) ->
    #provider_response{}.
get_file_distribution(_CTX, {uuid, UUID}) ->
    {ok, Locations} = file_meta:get_locations({uuid, UUID}),
    ProviderDistributions = lists:map(
        fun(LocationId) ->
            {ok, #document{value = #file_location{
                provider_id = ProviderId,
                blocks = Blocks
            }}} = file_location:get(LocationId),

            #provider_file_distribution{
                provider_id = ProviderId,
                blocks = Blocks
            }
        end, Locations),
    #provider_response{status = #status{code = ?OK}, provider_response =
    #file_distribution{provider_file_distributions = ProviderDistributions}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @equiv open_file_impl(CTX, File, read, CreateHandle) with permission check
%%--------------------------------------------------------------------
-spec open_file_for_read(fslogic_worker:ctx(), fslogic_worker:file()) ->
    no_return() | #fuse_response{}.
-check_permissions([{traverse_ancestors, 2}, {?read_object, 2}]).
open_file_for_read(CTX, File) ->
    open_file_impl(CTX, File, read).

%%--------------------------------------------------------------------
%% @equiv open_file_impl(CTX, File, write, CreateHandle) with permission check
%%--------------------------------------------------------------------
-spec open_file_for_write(fslogic_worker:ctx(), fslogic_worker:file()) ->
    no_return() | #fuse_response{}.
-check_permissions([{traverse_ancestors, 2}, {?write_object, 2}]).
open_file_for_write(CTX, File) ->
    open_file_impl(CTX, File, write).

%%--------------------------------------------------------------------
%% @equiv open_file_impl(CTX, File, rdwr, CreateHandle) with permission check
%%--------------------------------------------------------------------
-spec open_file_for_rdwr(fslogic_worker:ctx(), fslogic_worker:file()) ->
    no_return() | #fuse_response{}.
-check_permissions([{traverse_ancestors, 2}, {?read_object, 2}, {?write_object, 2}]).
open_file_for_rdwr(CTX, File) ->
    open_file_impl(CTX, File, rdwr).

%%--------------------------------------------------------------------
%% @doc Opens a file and returns a handle to it.
%% For best performance use following arg types: document -> uuid -> path
%% @end
%%--------------------------------------------------------------------
-spec open_file_impl(fslogic_worker:ctx(), File :: fslogic_worker:file(),
    fslogic_worker:open_flag()) -> no_return() | #fuse_response{}.
open_file_impl(#fslogic_ctx{session_id = SessId, space_id = SpaceId, share_id = ShareId} = CTX, File, Flag) ->
    {ok, #document{key = FileUUID} = FileDoc} = file_meta:get(File),

    {ok, StorageDoc} = fslogic_storage:select_storage(SpaceId),
    #document{value = #file_location{
        file_id = FileId}
    } = fslogic_utils:get_local_file_location({uuid, FileUUID}),
    {ok, #document{key = SpaceUUID}} = fslogic_spaces:get_space(
        FileDoc, fslogic_context:get_user_id(CTX)
    ),

    SFMHandle = storage_file_manager:new_handle(SessId, SpaceUUID, FileUUID, StorageDoc, FileId, ShareId),
    {ok, Handle} = storage_file_manager:open(SFMHandle, Flag),
    {ok, HandleId} = save_handle(SessId, Handle),

    ok = file_handles:register_open(FileUUID, SessId, 1),

    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = #file_opened{handle_id = HandleId}
    }.

%%--------------------------------------------------------------------
%% @doc Saves file handle in user's session, returns id of saved handle
%% @end
%%--------------------------------------------------------------------
-spec save_handle(session:id(), storage_file_manager:handle()) ->
    {ok, binary()}.
save_handle(SessId, Handle) ->
    HandleId = base64:encode(crypto:rand_bytes(20)),
    case SessId of
    	?ROOT_SESS_ID -> ok;
    	?GUEST_SESS_ID -> ok;
    	_ -> session:add_handle(SessId, HandleId, Handle)
    end,
    {ok, HandleId}.
