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

-include("global_definitions.hrl").
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
truncate(Ctx, Entry, Size) ->
    SessId = fslogic_context:get_session_id(Ctx),
    {ok, #document{key = FileUUID} = FileDoc} = file_meta:get(Entry),
    {ok, #document{key = SpaceUUID}} = fslogic_spaces:get_space(FileDoc, fslogic_context:get_user_id(Ctx)),

    %% START -> Quota check
    SpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(SpaceUUID),
    OldSize = fslogic_blocks:get_file_size(FileDoc),
    ok = space_quota:assert_write(SpaceId, Size - OldSize),
    %% END   -> Quota check

    Results = lists:map(
        fun({SID, FID} = Loc) ->
            {ok, Storage} = storage:get(SID),
            SFMHandle = storage_file_manager:new_handle(SessId, SpaceUUID, FileUUID, Storage, FID),
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

    fslogic_times:update_mtime_ctime(FileDoc, fslogic_context:get_user_id(Ctx)),
    #fuse_response{status = #status{code = ?OK}}.


%%--------------------------------------------------------------------
%% @doc Gets helper params based on given storage ID.
%% @end
%%--------------------------------------------------------------------
-spec get_helper_params(fslogic_worker:ctx(), StorageId :: storage:id(),
    ForceCL :: boolean()) -> FuseResponse :: #fuse_response{} | no_return().
get_helper_params(_Ctx, StorageId, true = _ForceProxy) ->
    {ok, StorageDoc} = storage:get(StorageId),
    {ok, #helper_init{args = Args}} = fslogic_storage:select_helper(StorageDoc),
    Timeout = helpers_utils:get_timeout(Args),
    {ok, Latency} = application:get_env(?APP_NAME, proxy_helper_latency_milliseconds),
    #fuse_response{status = #status{code = ?OK}, fuse_response = #helper_params{
        helper_name = <<"ProxyIO">>,
        helper_args = [
            #helper_arg{key = <<"storage_id">>, value = StorageId},
            #helper_arg{key = <<"timeout">>, value = integer_to_binary(Timeout + Latency)}
        ]
    }};
get_helper_params(Ctx, StorageId, false = _ForceProxy) ->
    SessId = fslogic_context:get_session_id(Ctx),
    SpaceId = fslogic_context:get_space_id(Ctx),
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
get_file_location(Ctx, File) ->
    SpaceId = fslogic_context:get_space_id(Ctx),
    {ok, #document{key = FileUUID}} = file_meta:get(File),
    {ok, #document{key = StorageId}} = fslogic_storage:select_storage(SpaceId),
    #document{value = #file_location{
        blocks = Blocks, file_id = FileId
    }} = fslogic_utils:get_local_file_location({uuid, FileUUID}), %todo VFS-2813 support multi location

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
%% @doc @equiv open_file(Ctx, File, CreateHandle) with permission check
%% depending on the open flag
%%--------------------------------------------------------------------
-spec open_file(fslogic_worker:ctx(), File :: fslogic_worker:file(),
    OpenFlag :: fslogic_worker:open_flag()) -> no_return() | #fuse_response{}.
open_file(Ctx, File, read) ->
    open_file_for_read(Ctx, File);
open_file(Ctx, File, write) ->
    open_file_for_write(Ctx, File);
open_file(Ctx, File, rdwr) ->
    open_file_for_rdwr(Ctx, File).

%%--------------------------------------------------------------------
%% @doc Creates and opens file. Returns handle to the file, its attributes
%% and location.
%% @end
%%--------------------------------------------------------------------
-spec create_file(fslogic_worker:ctx(), Parent :: file_meta:entry(), Name :: file_meta:name(),
    Mode :: file_meta:posix_permissions(), Flags :: fslogic_worker:open_flag()) ->
    no_return() | #fuse_response{}.
-check_permissions([{traverse_ancestors, 2}, {?add_object, 2}, {?traverse_container, 2}]).
create_file(Ctx, {uuid, ParentUUID}, Name, Mode, _Flag) ->
    SessId = fslogic_context:get_session_id(Ctx),
    SpaceId = fslogic_context:get_space_id(Ctx),
    {ok, #document{key = SpaceUUID}} = fslogic_spaces:get_space({uuid, ParentUUID}, fslogic_context:get_user_id(Ctx)),
    CTime = erlang:system_time(seconds),
    File = #document{value = #file_meta{
        name = Name,
        type = ?REGULAR_FILE_TYPE,
        mode = Mode,
        uid = fslogic_context:get_user_id(Ctx)
    }},

    {ok, FileUUID} = file_meta:create({uuid, ParentUUID}, File),
    {ok, _} = times:create(#document{key = FileUUID, value = #times{
        mtime = CTime, atime = CTime, ctime = CTime
    }}),

    try fslogic_file_location:create_storage_file(SpaceId, FileUUID, SessId, Mode) of
        {StorageId, FileId} ->
            fslogic_times:update_mtime_ctime({uuid, ParentUUID}, fslogic_context:get_user_id(Ctx)),

            {ok, Storage} = fslogic_storage:select_storage(SpaceId),
            SFMHandle = storage_file_manager:new_handle(SessId, SpaceUUID, FileUUID,
                Storage, FileId),
            {ok, Handle} = storage_file_manager:open_at_creation(SFMHandle),
            {ok, HandleId} = save_handle(SessId, Handle),

            #fuse_response{fuse_response = #file_attr{} = FileAttr} =
                fslogic_req_generic:get_file_attr(Ctx, {uuid, FileUUID}),

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
make_file(Ctx, {uuid, ParentUUID}, Name, Mode) ->
    SessId = fslogic_context:get_session_id(Ctx),
    SpaceId = fslogic_context:get_space_id(Ctx),
    CTime = erlang:system_time(seconds),
    File = #document{value = #file_meta{
        name = Name,
        type = ?REGULAR_FILE_TYPE,
        mode = Mode,
        uid = fslogic_context:get_user_id(Ctx)
    }},

    {ok, FileUUID} = file_meta:create({uuid, ParentUUID}, File),
    {ok, _} = times:create(#document{key = FileUUID, value = #times{
        mtime = CTime, atime = CTime, ctime = CTime
    }}),

    try fslogic_file_location:create_storage_file(SpaceId, FileUUID, SessId, Mode) of
        _ ->
            fslogic_times:update_mtime_ctime({uuid, ParentUUID}, fslogic_context:get_user_id(Ctx)),
            fslogic_req_generic:get_file_attr(Ctx, {uuid, FileUUID})
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
-spec release(fslogic_worker:ctx(), FileUUID :: file_meta:uuid(), HandleId :: binary()) ->
    no_return() | #fuse_response{}.
release(Ctx, FileUUID, HandleId) ->
    SessId = fslogic_context:get_session_id(Ctx),
    ok = session:remove_handle(SessId, HandleId),
    ok = file_handles:register_release(FileUUID, SessId, 1),
    #fuse_response{status = #status{code = ?OK}}.


%%--------------------------------------------------------------------
%% @doc Gets parent of file
%% @end
%%--------------------------------------------------------------------
-spec get_parent(Ctx :: fslogic_worker:ctx(), File :: fslogic_worker:file()) ->
    ProviderResponse :: #provider_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}]).
get_parent(Ctx, File) ->
    ShareId = file_info:get_share_id(Ctx),
    case ShareId of
        undefined ->
            SpacesBaseDirUUID = ?ROOT_DIR_UUID,
            {ok, #document{key = ParentUUID}} = file_meta:get_parent(File),
            case ParentUUID of
                SpacesBaseDirUUID ->
                    #provider_response{
                        status = #status{code = ?OK},
                        provider_response = #dir{uuid =
                        fslogic_uuid:uuid_to_guid(fslogic_uuid:user_root_dir_uuid(fslogic_context:get_user_id(Ctx)), undefined)}
                    };
                _ ->
                    #provider_response{
                        status = #status{code = ?OK},
                        provider_response = #dir{uuid = fslogic_uuid:uuid_to_guid(ParentUUID)}
                    }
            end;
        _ ->
            SpaceId = fslogic_context:get_space_id(Ctx),
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
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Synchronizes given block with remote replicas.
%% @end
%%--------------------------------------------------------------------
-spec synchronize_block(fslogic_worker:ctx(), {uuid, file_meta:uuid()}, fslogic_blocks:block(), boolean()) ->
    #fuse_response{}.
synchronize_block(Ctx, {uuid, FileUUID}, undefined, Prefetch) ->
    Size = fslogic_blocks:get_file_size({uuid, FileUUID}),
    synchronize_block(Ctx, {uuid, FileUUID}, #file_block{offset = 0, size = Size}, Prefetch);
synchronize_block(Ctx, {uuid, FileUUID}, Block, Prefetch) ->
    ok = replica_synchronizer:synchronize(Ctx, FileUUID, Block, Prefetch),
    #fuse_response{status = #status{code = ?OK}}.


%%--------------------------------------------------------------------
%% @doc
%% Synchronizes given block with remote replicas and returns checksum of
%% synchronized data.
%% @end
%%--------------------------------------------------------------------
-spec synchronize_block_and_compute_checksum(fslogic_worker:ctx(),
    {uuid, file_meta:uuid()}, fslogic_blocks:block()) -> #fuse_response{}.
synchronize_block_and_compute_checksum(Ctx, {uuid, FileUUID},
    Range = #file_block{offset = Offset, size = Size}) ->

    SessId = fslogic_context:get_session_id(Ctx),
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
get_file_distribution(_Ctx, {uuid, UUID}) ->
    {ok, Locations} = file_meta:get_locations({uuid, UUID}),
    ProviderDistributions = lists:map( %todo VFS-2813 support multi location
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
%% @equiv open_file_impl(Ctx, File, read, CreateHandle) with permission check
%%--------------------------------------------------------------------
-spec open_file_for_read(fslogic_worker:ctx(), fslogic_worker:file()) ->
    no_return() | #fuse_response{}.
-check_permissions([{traverse_ancestors, 2}, {?read_object, 2}]).
open_file_for_read(Ctx, File) ->
    open_file_impl(Ctx, File, read).

%%--------------------------------------------------------------------
%% @equiv open_file_impl(Ctx, File, write, CreateHandle) with permission check
%%--------------------------------------------------------------------
-spec open_file_for_write(fslogic_worker:ctx(), fslogic_worker:file()) ->
    no_return() | #fuse_response{}.
-check_permissions([{traverse_ancestors, 2}, {?write_object, 2}]).
open_file_for_write(Ctx, File) ->
    open_file_impl(Ctx, File, write).

%%--------------------------------------------------------------------
%% @equiv open_file_impl(Ctx, File, rdwr, CreateHandle) with permission check
%%--------------------------------------------------------------------
-spec open_file_for_rdwr(fslogic_worker:ctx(), fslogic_worker:file()) ->
    no_return() | #fuse_response{}.
-check_permissions([{traverse_ancestors, 2}, {?read_object, 2}, {?write_object, 2}]).
open_file_for_rdwr(Ctx, File) ->
    open_file_impl(Ctx, File, rdwr).

%%--------------------------------------------------------------------
%% @doc Opens a file and returns a handle to it.
%% For best performance use following arg types: document -> uuid -> path
%% @end
%%--------------------------------------------------------------------
-spec open_file_impl(fslogic_worker:ctx(), File :: fslogic_worker:file(),
    fslogic_worker:open_flag()) -> no_return() | #fuse_response{}.
open_file_impl(Ctx, File, Flag) ->
    SessId = fslogic_context:get_session_id(Ctx),
    SpaceId = fslogic_context:get_space_id(Ctx),
    ShareId = file_info:get_share_id(Ctx),
    {ok, #document{key = FileUUID} = FileDoc} = file_meta:get(File),

    {ok, StorageDoc} = fslogic_storage:select_storage(SpaceId),
    #document{value = #file_location{
        file_id = FileId}
    } = fslogic_utils:get_local_file_location({uuid, FileUUID}), %todo VFS-2813 support multi location
    {ok, #document{key = SpaceUUID}} = fslogic_spaces:get_space(
        FileDoc, fslogic_context:get_user_id(Ctx)
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
    case session:is_special(SessId) of
    	true -> ok;
    	_ -> session:add_handle(SessId, HandleId, Handle)
    end,
    {ok, HandleId}.
