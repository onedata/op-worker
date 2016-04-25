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
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("annotations/include/annotations.hrl").

%% API
-export([get_file_location/3, get_new_file_location/5, truncate/3,
    get_helper_params/3, release/2]).
-export([get_parent/2, synchronize_block/3]).

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

    CurrTime = erlang:system_time(seconds),
    #document{value = FileMeta} = FileDoc,
    {ok, _} = file_meta:update(FileDoc, #{mtime => CurrTime, ctime => CurrTime}),

    spawn(fun() -> fslogic_event:emit_file_sizeless_attrs_update(
        FileDoc#document{value = FileMeta#file_meta{
            mtime = CurrTime, ctime = CurrTime
        }}
    ) end),

    #fuse_response{status = #status{code = ?OK}}.


%%--------------------------------------------------------------------
%% @doc Gets helper params based on given storage ID.
%% @end
%%--------------------------------------------------------------------
-spec get_helper_params(fslogic_worker:ctx(),
    StorageId :: storage:id(), ForceCL :: boolean()) ->
    FuseResponse :: #fuse_response{} | no_return().
get_helper_params(_Ctx, StorageId, true = _ForceProxy) ->
    #fuse_response{status = #status{code = ?OK},
        fuse_response = #helper_params{helper_name = <<"ProxyIO">>,
            helper_args = [
                #helper_arg{key = <<"storage_id">>, value = StorageId}
            ]}};
get_helper_params(#fslogic_ctx{session = #session{identity = #identity{user_id = UserId}}},
    StorageId, false = _ForceProxy) ->
    {ok, #document{value = #storage{}} = StorageDoc} = storage:get(StorageId),
    {HelperName, HelperArgsMap} = case fslogic_storage:select_helper(StorageDoc) of
        {ok, #helper_init{name = ?CEPH_HELPER_NAME, args = Args}} ->
            {ok, #document{value = #ceph_user{credentials = UserCredentials}}} = ceph_user:get(UserId),
            {ok, Credentials} = maps:find(StorageId, UserCredentials),
            {?CEPH_HELPER_NAME, Args#{
                <<"user_name">> => ceph_user:name(Credentials),
                <<"key">> => ceph_user:key(Credentials)
            }};
        {ok, #helper_init{name = ?S3_HELPER_NAME, args = Args}} ->
            {ok, #document{value = #s3_user{credentials = UserCredentials}}} = s3_user:get(UserId),
            {ok, Credentials} = maps:find(StorageId, UserCredentials),
            {?S3_HELPER_NAME, Args#{
                <<"access_key">> => s3_user:access_key(Credentials),
                <<"secret_key">> => s3_user:secret_key(Credentials)
            }};
        {ok, #helper_init{name = Name, args = Args}} ->
            {Name, Args}
    end,

    HelperArgs = [#helper_arg{key = K, value = V} || {K, V} <- maps:to_list(HelperArgsMap)],

    #fuse_response{status = #status{code = ?OK},
        fuse_response = #helper_params{helper_name = HelperName, helper_args = HelperArgs}}.


%%--------------------------------------------------------------------
%% @equiv get_file_location(CTX, File) with permission check depending on open mode
%%--------------------------------------------------------------------
-spec get_file_location(fslogic_worker:ctx(), File :: fslogic_worker:file(), OpenMode :: fslogic_worker:open_flags()) ->
    no_return() | #fuse_response{}.
get_file_location(CTX, File, read) ->
    get_file_location_for_read(CTX, File);
get_file_location(CTX, File, write) ->
    get_file_location_for_write(CTX, File);
get_file_location(CTX, File, rdwr) ->
    get_file_location_for_rdwr(CTX, File).

%%--------------------------------------------------------------------
%% @doc Gets new file location (implicit mknod operation).
%% @end
%%--------------------------------------------------------------------
-spec get_new_file_location(fslogic_worker:ctx(), Parent :: file_meta:entry(), Name :: file_meta:name(),
    Mode :: file_meta:posix_permissions(), Flags :: fslogic_worker:open_flags()) ->
    no_return() | #fuse_response{}.
-check_permissions([{traverse_ancestors, 2}, {?add_object, 2}, {?traverse_container, 2}]).
get_new_file_location(#fslogic_ctx{session_id = SessId, space_id = SpaceId} = CTX, {uuid, ParentUUID}, Name, Mode, _Flags) ->
    NormalizedParentUUID =
        case fslogic_uuid:default_space_uuid(fslogic_context:get_user_id(CTX)) =:= ParentUUID of
            true ->
                {ok, #document{key = DefaultSpaceUUID}} = fslogic_spaces:get_default_space(CTX),
                DefaultSpaceUUID;
            false ->
                ParentUUID
        end,

    {ok, #document{key = SpaceUUID}} = fslogic_spaces:get_space({uuid, NormalizedParentUUID}, fslogic_context:get_user_id(CTX)),
    CTime = erlang:system_time(seconds),
    File = #document{value = #file_meta{
        name = Name,
        type = ?REGULAR_FILE_TYPE,
        mode = Mode,
        mtime = CTime,
        atime = CTime,
        ctime = CTime,
        uid = fslogic_context:get_user_id(CTX)
    }},

    {ok, UUID} = file_meta:create({uuid, NormalizedParentUUID}, File),

    {StorageId, FileId} = fslogic_file_location:create_storage_file(SpaceId, UUID, SessId, Mode),

    {ok, ParentDoc} = file_meta:get(NormalizedParentUUID),
    CurrTime = erlang:system_time(seconds),
    #document{value = ParentMeta} = ParentDoc,
    {ok, _} = file_meta:update(ParentDoc, #{mtime => CurrTime, ctime => CurrTime}),

    spawn(fun() -> fslogic_event:emit_file_sizeless_attrs_update(
        ParentDoc#document{value = ParentMeta#file_meta{
            mtime = CurrTime, ctime = CurrTime}
        }
    ) end),

    {ok, HandleId} = case SessId =:= ?ROOT_SESS_ID of
        false ->
            {ok, Storage} = fslogic_storage:select_storage(SpaceId),
            SFMHandle = storage_file_manager:new_handle(SessId, fslogic_uuid:to_file_guid(UUID, SpaceId), Storage, FileId),
            {ok, Handle} = storage_file_manager:open_at_creation(SFMHandle),
            save_handle(SessId, Handle);
        true ->
            {ok, undefined}
    end,

    #fuse_response{status = #status{code = ?OK},
        fuse_response = file_location:ensure_blocks_not_empty(#file_location{
            uuid = fslogic_uuid:to_file_guid(UUID, SpaceId), provider_id = oneprovider:get_provider_id(),
            storage_id = StorageId, file_id = FileId, blocks = [],
            space_uuid = SpaceUUID, handle_id = HandleId})}.


%%--------------------------------------------------------------------
%% @doc Removes file handle saved in session.
%% @end
%%--------------------------------------------------------------------
-spec release(#fslogic_ctx{}, HandleId :: binary()) ->
    no_return() | #fuse_response{}.
release(#fslogic_ctx{session_id = SessId}, HandleId) ->
    {ok, #document{value = #session{handles = Handles}}} = session:get(SessId),
    UpdatedHandles = maps:remove(HandleId, Handles),
    {ok, SessId} = session:update(SessId, #{handles => UpdatedHandles}),
    #fuse_response{status = #status{code = ?OK}}.


%%--------------------------------------------------------------------
%% @doc Gets parent of file
%% @end
%%--------------------------------------------------------------------
-spec get_parent(CTX :: fslogic_worker:ctx(), File :: fslogic_worker:file()) ->
    FuseResponse :: #fuse_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}]).
get_parent(_CTX, File) ->
    {ok, #document{key = ParentUUID}} = file_meta:get_parent(File),
    #fuse_response{status = #status{code = ?OK}, fuse_response =
    #dir{uuid = ParentUUID}}.


%%--------------------------------------------------------------------
%% @doc
%% Synchronizes given block with remote replicas.
%% @end
%%--------------------------------------------------------------------
-spec synchronize_block(fslogic_worker:ctx(), {uuid, file_meta:uuid()}, fslogic_blocks:block()) ->
    #fuse_response{}.
synchronize_block(_Ctx, {uuid, Uuid}, Block)  ->
    ok = replica_synchronizer:synchronize(Uuid, Block),
    #fuse_response{status = #status{code = ?OK}}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @equiv get_file_location(CTX, File, Mode) with permission check
%%--------------------------------------------------------------------
-spec get_file_location_for_read(fslogic_worker:ctx(), fslogic_worker:file()) ->
    no_return() | #fuse_response{}.
-check_permissions([{traverse_ancestors, 2}, {?read_object, 2}]).
get_file_location_for_read(CTX, File) ->
    get_file_location_impl(CTX, File, read).

%%--------------------------------------------------------------------
%% @equiv get_file_location(CTX, File, Mode) with permission check
%%--------------------------------------------------------------------
-spec get_file_location_for_write(fslogic_worker:ctx(), fslogic_worker:file()) ->
    no_return() | #fuse_response{}.
-check_permissions([{traverse_ancestors, 2}, {?write_object, 2}]).
get_file_location_for_write(CTX, File) ->
    get_file_location_impl(CTX, File, write).

%%--------------------------------------------------------------------
%% @equiv get_file_location_impl(CTX, File, Mode) with permission check
%%--------------------------------------------------------------------
-spec get_file_location_for_rdwr(fslogic_worker:ctx(), fslogic_worker:file()) ->
    no_return() | #fuse_response{}.
-check_permissions([{traverse_ancestors, 2}, {?read_object, 2}, {?write_object, 2}]).
get_file_location_for_rdwr(CTX, File) ->
    get_file_location_impl(CTX, File, rdwr).

%%--------------------------------------------------------------------
%% @doc Gets file location (implicit file open operation). Allows to force-select ClusterProxy helper.
%% For best performance use following arg types: document -> uuid -> path
%% @end
%%--------------------------------------------------------------------
-spec get_file_location_impl(fslogic_worker:ctx(), File :: fslogic_worker:file(),
    helpers:open_mode()) ->
    no_return() | #fuse_response{}.
get_file_location_impl(#fslogic_ctx{session_id = SessId, space_id = SpaceId} = CTX, File, Mode) ->
    {ok, #document{key = UUID} = FileDoc} = file_meta:get(File),

    {ok, #document{key = StorageId, value = Storage}} = fslogic_storage:select_storage(CTX#fslogic_ctx.space_id),
    FileId = fslogic_utils:gen_storage_file_id({uuid, UUID}),

    #document{value = #file_location{blocks = Blocks}} = fslogic_utils:get_local_file_location({uuid, UUID}),

    {ok, #document{key = SpaceUUID}} = fslogic_spaces:get_space(FileDoc, fslogic_context:get_user_id(CTX)),

    {ok, HandleId} = case SessId =:= ?ROOT_SESS_ID of
        false ->
            SFMHandle = storage_file_manager:new_handle(SessId, SpaceUUID, UUID, Storage, FileId),
            {ok, Handle} = storage_file_manager:open(SFMHandle, Mode),
            save_handle(SessId, Handle);
        true ->
            {ok, undefined}
    end,

    #fuse_response{status = #status{code = ?OK},
        fuse_response = file_location:ensure_blocks_not_empty(#file_location{
            uuid = fslogic_uuid:to_file_guid(UUID, SpaceId), provider_id = oneprovider:get_provider_id(),
            storage_id = StorageId, file_id = FileId, blocks = Blocks,
            space_uuid = SpaceUUID, handle_id = HandleId})}.

%%--------------------------------------------------------------------
%% @doc Saves file handle in user's session, returns id of saved handle
%% @end
%%--------------------------------------------------------------------
-spec save_handle(session:id(), storage_file_manager:handle()) ->
    {ok, binary()}.
save_handle(SessionId, Handle) ->
    HandleId = base64:encode(crypto:rand_bytes(20)),
    {ok, #document{value = #session{handles = Handles}}} = session:get(SessionId),
    UpdatedHandles = maps:put(HandleId, Handle, Handles),
    {ok, SessionId} = session:update(SessionId, #{handles => UpdatedHandles}),
    {ok, HandleId}.
