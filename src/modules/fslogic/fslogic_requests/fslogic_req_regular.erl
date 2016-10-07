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
-export([get_file_location/4, get_new_file_location/6, truncate/3,
    get_helper_params/3, release/2]).
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
%% @equiv get_file_location(CTX, File, CreateHandle) with permission
%% check depending on open mode
%%--------------------------------------------------------------------
-spec get_file_location(fslogic_worker:ctx(), File :: fslogic_worker:file(),
    OpenMode :: fslogic_worker:open_flags(), CreateHandle :: boolean()) ->
    no_return() | #fuse_response{}.
get_file_location(CTX, File, read, CreateHandle) ->
    get_file_location_for_read(CTX, File, CreateHandle);
get_file_location(CTX, File, write, CreateHandle) ->
    get_file_location_for_write(CTX, File, CreateHandle);
get_file_location(CTX, File, rdwr, CreateHandle) ->
    get_file_location_for_rdwr(CTX, File, CreateHandle).

%%--------------------------------------------------------------------
%% @doc Gets new file location (implicit mknod operation).
%% @end
%%--------------------------------------------------------------------
-spec get_new_file_location(fslogic_worker:ctx(), Parent :: file_meta:entry(), Name :: file_meta:name(),
    Mode :: file_meta:posix_permissions(), Flags :: fslogic_worker:open_flags(), CreateHandle :: boolean()) ->
    no_return() | #fuse_response{}.
-check_permissions([{traverse_ancestors, 2}, {?add_object, 2}, {?traverse_container, 2}]).
get_new_file_location(#fslogic_ctx{session_id = SessId, space_id = SpaceId} = CTX, {uuid, ParentUUID}, Name, Mode, _Flags, CreateHandle) ->
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
        mtime = CTime, atime = CTime, ctime = CTime}}),

    try fslogic_file_location:create_storage_file(SpaceId, FileUUID, SessId, Mode) of
        {StorageId, FileId} ->
            fslogic_times:update_mtime_ctime({uuid, ParentUUID}, fslogic_context:get_user_id(CTX)),

            {ok, HandleId} = case (SessId =/= ?ROOT_SESS_ID) andalso (SessId =/= ?GUEST_SESS_ID) andalso CreateHandle of
                true ->
                    {ok, Storage} = fslogic_storage:select_storage(SpaceId),
                    SFMHandle = storage_file_manager:new_handle(SessId, SpaceUUID, FileUUID,
                        Storage, FileId),
                    {ok, Handle} = storage_file_manager:open_at_creation(SFMHandle),
                    save_handle(SessId, Handle);
                false ->
                    {ok, undefined}
            end,

            #fuse_response{status = #status{code = ?OK},
                fuse_response = file_location:ensure_blocks_not_empty(#file_location{
                    uuid = fslogic_uuid:uuid_to_guid(FileUUID, SpaceId), provider_id = oneprovider:get_provider_id(),
                    storage_id = StorageId, file_id = FileId, blocks = [], handle_id = HandleId, space_id = SpaceId})}
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
-spec release(#fslogic_ctx{}, HandleId :: binary()) ->
    no_return() | #fuse_response{}.
release(#fslogic_ctx{session_id = SessId}, HandleId) ->
    ok = session:remove_handle(SessId, HandleId),
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
synchronize_block(CTX, {uuid, FileUUID}, undefined, Prefetch)  ->
    Size = fslogic_blocks:get_file_size({uuid, FileUUID}),
    synchronize_block(CTX, {uuid, FileUUID}, #file_block{offset = 0, size = Size}, Prefetch);
synchronize_block(CTX, {uuid, FileUUID}, Block, Prefetch)  ->
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
    #file_block{offset = Offset, size = Size})  ->
    {ok, Handle} = lfm_files:open(SessId, {guid, fslogic_uuid:uuid_to_guid(FileUUID)}, read),
    {ok, _, Data} = lfm_files:read_without_events(Handle, Offset, Size), % does sync internally
    Checksum = crypto:hash(md4, Data),
    #fuse_response{status = #status{code = ?OK},
        fuse_response = #checksum{value = Checksum}}.


%%--------------------------------------------------------------------
%% @doc
%% Get distribution of file over providers' storages.
%% @end
%%--------------------------------------------------------------------
-spec get_file_distribution(fslogic_worker:ctx(), {uuid, file_meta:uuid()}) ->
    #provider_response{}.
get_file_distribution(_CTX, {uuid, UUID})  ->
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
%% @equiv get_file_location_impl(CTX, File, read, CreateHandle) with permission check
%%--------------------------------------------------------------------
-spec get_file_location_for_read(fslogic_worker:ctx(), fslogic_worker:file(), boolean()) ->
    no_return() | #fuse_response{}.
-check_permissions([{traverse_ancestors, 2}, {?read_object, 2}]).
get_file_location_for_read(CTX, File, CreateHandle) ->
    get_file_location_impl(CTX, File, read, CreateHandle).

%%--------------------------------------------------------------------
%% @equiv get_file_location_impl(CTX, File, write, CreateHandle) with permission check
%%--------------------------------------------------------------------
-spec get_file_location_for_write(fslogic_worker:ctx(), fslogic_worker:file(), boolean()) ->
    no_return() | #fuse_response{}.
-check_permissions([{traverse_ancestors, 2}, {?write_object, 2}]).
get_file_location_for_write(CTX, File, CreateHandle) ->
    get_file_location_impl(CTX, File, write, CreateHandle).

%%--------------------------------------------------------------------
%% @equiv get_file_location_impl(CTX, File, rdwr, CreateHandle) with permission check
%%--------------------------------------------------------------------
-spec get_file_location_for_rdwr(fslogic_worker:ctx(), fslogic_worker:file(), boolean()) ->
    no_return() | #fuse_response{}.
-check_permissions([{traverse_ancestors, 2}, {?read_object, 2}, {?write_object, 2}]).
get_file_location_for_rdwr(CTX, File, CreateHandle) ->
    get_file_location_impl(CTX, File, rdwr, CreateHandle).

%%--------------------------------------------------------------------
%% @doc Gets file location (implicit file open operation). Allows to force-select ClusterProxy helper.
%% For best performance use following arg types: document -> uuid -> path
%% @end
%%--------------------------------------------------------------------
-spec get_file_location_impl(fslogic_worker:ctx(), File :: fslogic_worker:file(),
    helpers:open_mode(), boolean()) ->
    no_return() | #fuse_response{}.
get_file_location_impl(#fslogic_ctx{session_id = SessId, space_id = SpaceId, share_id = ShareId} = CTX, File, Mode, CreateHandle) ->
    {ok, #document{key = FileUUID} = FileDoc} = file_meta:get(File),

    {ok, #document{key = StorageId, value = Storage}} = fslogic_storage:select_storage(CTX#fslogic_ctx.space_id),
    #document{value = #file_location{blocks = Blocks, file_id = FileId}} = fslogic_utils:get_local_file_location({uuid, FileUUID}),

    {ok, #document{key = SpaceUUID}} = fslogic_spaces:get_space(FileDoc, fslogic_context:get_user_id(CTX)),

    {ok, HandleId} = case (SessId =/= ?ROOT_SESS_ID) andalso (SessId =/= ?GUEST_SESS_ID) andalso CreateHandle of
        true ->
            SFMHandle = storage_file_manager:new_handle(SessId, SpaceUUID, FileUUID, Storage, FileId, ShareId),
            {ok, Handle} = storage_file_manager:open(SFMHandle, Mode),
            save_handle(SessId, Handle);
        false ->
            {ok, undefined}
    end,

    #fuse_response{status = #status{code = ?OK},
        fuse_response = file_location:ensure_blocks_not_empty(#file_location{
            uuid = fslogic_uuid:uuid_to_guid(FileUUID, SpaceId), provider_id = oneprovider:get_provider_id(),
            storage_id = StorageId, file_id = FileId, blocks = Blocks, handle_id = HandleId, space_id = SpaceId})}.

%%--------------------------------------------------------------------
%% @doc Saves file handle in user's session, returns id of saved handle
%% @end
%%--------------------------------------------------------------------
-spec save_handle(session:id(), storage_file_manager:handle()) ->
    {ok, binary()}.
save_handle(SessionId, Handle) ->
    HandleId = base64:encode(crypto:rand_bytes(20)),
    ok = session:add_handle(SessionId, HandleId, Handle),
    {ok, HandleId}.
