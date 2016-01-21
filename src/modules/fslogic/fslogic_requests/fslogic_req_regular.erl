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

%% API
-export([get_file_location/3, get_new_file_location/5, truncate/3, get_helper_params/4]).
-export([get_parent/2]).

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
-check_permissions([{write, 2}]).
truncate(#fslogic_ctx{session_id = SessionId}, Entry, Size) ->
    {ok, #document{key = SpaceUUID}} = fslogic_spaces:get_space(Entry),
    Results = lists:map(
        fun({SID, FID} = Loc) ->
            {ok, Storage} = storage:get(SID),
            SFMHandle = storage_file_manager:new_handle(SessionId, SpaceUUID, Storage, FID),
            {Loc, storage_file_manager:truncate(SFMHandle, Size)}
        end, fslogic_utils:get_local_storage_file_locations(Entry)),

    case [{Loc, Error} || {Loc, {error, _} = Error} <- Results] of
        [] -> ok;
        Errors ->
            [?error("Unable to truncate [FileId: ~p] [StoragId: ~p] to size ~p due to: ~p", [FID, SID, Size, Reason])
                || {{SID, FID}, {error, Reason}} <- Errors],
            ok
    end,

    #fuse_response{status = #status{code = ?OK}}.


%%--------------------------------------------------------------------
%% @doc Gets helper params based on given session and storage ID.
%% @end
%%--------------------------------------------------------------------
-spec get_helper_params(fslogic_worker:ctx(), SpaceId :: file_meta:uuid(),
    StorageId :: storage:id(), ForceCL :: boolean()) ->
    FuseResponse :: #fuse_response{} | no_return().
get_helper_params(_Ctx, SpaceId, StorageId, true = _ForceCL) ->
    #fuse_response{status = #status{code = ?OK},
        fuse_response = #helper_params{helper_name = <<"ProxyIO">>,
            helper_args = [
                #helper_arg{key = <<"storage_id">>, value = StorageId}
                #helper_arg{key = <<"space_id">>, value = SpaceId}]}};
get_helper_params(_Ctx, _SpaceId, StorageId, false = _ForceCL) ->
    {ok, #document{value = #storage{}} = StorageDoc} = storage:get(StorageId),
    {ok, #helper_init{name = Name, args = HelperArgsMap}} = fslogic_storage:select_helper(StorageDoc),

    HelperArgs = [#helper_arg{key = K, value = V} || {K, V} <- maps:to_list(HelperArgsMap)],

    #fuse_response{status = #status{code = ?OK},
        fuse_response = #helper_params{helper_name = Name, helper_args = HelperArgs}}.


%%--------------------------------------------------------------------
%% @doc Gets file location (implicit file open operation). Allows to force-select ClusterProxy helper.
%% For best performance use following arg types: document -> uuid -> path
%% @end
%%--------------------------------------------------------------------
-spec get_file_location(fslogic_worker:ctx(), File :: fslogic_worker:file(), OpenMode :: fslogic_worker:open_flags()) ->
    no_return() | #fuse_response{}.
-check_permissions([{none, 2}]).
get_file_location(#fslogic_ctx{} = CTX, File, OpenFlags) ->
    ?debug("get_file_location for ~p ~p", [File, OpenFlags]),
    {ok, #document{key = UUID} = FileDoc} = file_meta:get(File),

    ok = check_permissions:validate_posix_access(OpenFlags, FileDoc, fslogic_context:get_user_id(CTX)),

    {ok, #document{key = StorageId, value = _Storage}} = fslogic_storage:select_storage(CTX),
    FileId = fslogic_utils:gen_storage_file_id({uuid, UUID}),

    #document{value = #file_location{blocks = Blocks}} = fslogic_utils:get_local_file_location({uuid, UUID}),

    {ok, #document{key = SpaceUUID}} = fslogic_spaces:get_space(FileDoc),

    #fuse_response{status = #status{code = ?OK},
        fuse_response = #file_location{
            uuid = UUID, provider_id = oneprovider:get_provider_id(),
            storage_id = StorageId, file_id = FileId, blocks = Blocks,
            space_id = SpaceUUID}}.


%%--------------------------------------------------------------------
%% @doc Gets new file location (implicit mknod operation).
%% @end
%%--------------------------------------------------------------------
-spec get_new_file_location(fslogic_worker:ctx(), ParentUUID :: file_meta:uuid(), Name :: file_meta:name(),
    Mode :: file_meta:posix_permissions(), Flags :: fslogic_worker:open_flags()) ->
    no_return() | #fuse_response{}.
-check_permissions([{write, 2}]).
get_new_file_location(#fslogic_ctx{session_id = SessId} = CTX, ParentUUID, Name, Mode, _Flags) ->
    NormalizedParentUUID =
        case fslogic_uuid:default_space_uuid(fslogic_context:get_user_id(CTX)) =:= ParentUUID of
            true ->
                {ok, #document{key = DefaultSpaceUUID}} = fslogic_spaces:get_default_space(CTX),
                DefaultSpaceUUID;
            false ->
                ParentUUID
        end,
    {ok, #document{key = SpaceUUID}} = fslogic_spaces:get_space({uuid, NormalizedParentUUID}),
    {ok, #document{key = StorageId} = Storage} = fslogic_storage:select_storage(CTX),
    CTime = utils:time(),
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

    FileId = fslogic_utils:gen_storage_file_id({uuid, UUID}),

    Location = #file_location{blocks = [#file_block{offset = 0, size = 0, file_id = FileId, storage_id = StorageId}],
        provider_id = oneprovider:get_provider_id(), file_id = FileId, storage_id = StorageId, uuid = UUID},
    {ok, LocId} = file_location:create(#document{value = Location}),

    file_meta:attach_location({uuid, UUID}, LocId, oneprovider:get_provider_id()),

    LeafLess = fslogic_path:dirname(FileId),
    SFMHandle0 = storage_file_manager:new_handle(?ROOT_SESS_ID, SpaceUUID, Storage, LeafLess),
    case storage_file_manager:mkdir(SFMHandle0, ?AUTO_CREATED_PARENT_DIR_MODE, true) of
        ok -> ok;
        {error, eexist} ->
            ok
    end,

    SFMHandle1 = storage_file_manager:new_handle(SessId, SpaceUUID, Storage, FileId),
    storage_file_manager:unlink(SFMHandle1),
    ok = storage_file_manager:create(SFMHandle1, Mode),

    #fuse_response{status = #status{code = ?OK},
        fuse_response = #file_location{
            uuid = UUID, provider_id = oneprovider:get_provider_id(),
            storage_id = StorageId, file_id = FileId, blocks = [],
            space_id = SpaceUUID}}.


%%--------------------------------------------------------------------
%% @doc Gets new file location (implicit mknod operation).
%% @end
%%--------------------------------------------------------------------
-spec get_parent(CTX :: fslogic_worker:ctx(), File :: fslogic_worker:file()) ->
    FuseResponse :: #fuse_response{} | no_return().
-check_permissions([{none, 2}]).
get_parent(_CTX, File) ->
    {ok, #document{key = ParentUUID}} = file_meta:get_parent(File),
    #fuse_response{status = #status{code = ?OK}, fuse_response =
        #dir{uuid = ParentUUID}}.
