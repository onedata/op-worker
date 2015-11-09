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
-export([get_file_location/3, get_new_file_location/5, truncate/3, get_helper_params/3]).

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
truncate(_Ctx, Entry, Size) ->
    Results = lists:map(
                fun({SID, FID} = Loc) ->
                        {ok, Storage} = storage:get(SID),
                        {Loc, storage_file_manager:truncate(Storage, FID, Size)}
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
-spec get_helper_params(fslogic_worker:ctx(), StorageId :: storage:id(), ForceCL :: boolean()) ->
                               FuseResponse :: #fuse_response{} | no_return().
get_helper_params(_Ctx, SID, _ForceCL) ->

    {ok, #document{value = #storage{}} = StorageDoc} = storage:get(SID),
    {ok, #helper_init{name = Name, args = HelperArgsMap}} = fslogic_storage:select_helper(StorageDoc),

    HelperArgs = [#helper_arg{key = K, value = V} || {K, V} <- maps:to_list(HelperArgsMap)],

    #fuse_response{status = #status{code = ?OK},
                   fuse_response = #helper_params{helper_name = Name, helper_args = HelperArgs}}.


%%--------------------------------------------------------------------
%% @doc Gets file location (implicit file open operation). Allows to force-select ClusterProxy helper.
%% For best performance use following arg types: document -> uuid -> path
%% @end
%%--------------------------------------------------------------------
-spec get_file_location(fslogic_worker:ctx(), File :: fslogic_worker:file(), Flags :: fslogic_worker:open_flags()) ->
                               no_return() | #fuse_response{}.
-check_permissions([{read, 2}]).
get_file_location(#fslogic_ctx{session_id = SessId} = CTX, File, Flags) ->
    ?debug("get_file_location for ~p ~p", [File, Flags]),
    {ok, #document{key = UUID}} = file_meta:get(File),
    ok = file_watcher:insert_open_watcher(UUID, SessId),
    {ok, #document{key = StorageId, value = _Storage}} = fslogic_storage:select_storage(CTX),
    FileId = fslogic_utils:gen_storage_file_id({uuid, UUID}),
    #document{value = #file_location{blocks = Blocks}} = fslogic_utils:get_local_file_location({uuid, UUID}),


    #fuse_response{status = #status{code = ?OK}, fuse_response =
                       #file_location{uuid = UUID, provider_id = oneprovider:get_provider_id(), storage_id = StorageId, file_id = FileId, blocks = Blocks}}.


%%--------------------------------------------------------------------
%% @doc Gets new file location (implicit mknod operation).
%% @end
%%--------------------------------------------------------------------
-spec get_new_file_location(fslogic_worker:ctx(), ParentUUID :: file_meta:uuid(), Name :: file_meta:name(),
                            Mode :: file_meta:posix_permissions(), Flags :: fslogic_worker:open_flags()) ->
    no_return() | #fuse_response{}.
-check_permissions([{write, 2}]).
get_new_file_location(#fslogic_ctx{session = #session{identity = #identity{user_id = UUID}}} = CTX,
                      UUID, Name, Mode, _Flags) ->
    {ok, #document{key = DefaultSpaceUUID}} = fslogic_spaces:get_default_space(CTX),
    get_new_file_location(CTX, DefaultSpaceUUID, Name, Mode, _Flags);
get_new_file_location(#fslogic_ctx{session_id = SessId} = CTX, ParentUUID, Name, Mode, _Flags) ->

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

    {ok, UUID} = file_meta:create({uuid, ParentUUID}, File),

    FileId = fslogic_utils:gen_storage_file_id({uuid, UUID}),

    Location = #file_location{blocks = [#file_block{offset = 0, size = 0, file_id = FileId, storage_id = StorageId}],
                              provider_id = oneprovider:get_provider_id(), file_id = FileId, storage_id = StorageId, uuid = UUID},
    {ok, LocId} = file_location:create(#document{value = Location}),

    file_meta:attach_location({uuid, UUID}, LocId, oneprovider:get_provider_id()),

    LeafLess = fslogic_path:dirname(FileId),
    case storage_file_manager:mkdir(Storage, LeafLess, ?AUTO_CREATED_PARENT_DIR_MODE, true) of
        ok -> ok;
        {error, eexist} ->
            ok
    end,

    storage_file_manager:unlink(Storage, FileId),
    ok = storage_file_manager:create(Storage, FileId, Mode),

    ok = file_watcher:insert_open_watcher(UUID, SessId),

    #fuse_response{status = #status{code = ?OK}, fuse_response =
                       #file_location{uuid = UUID, provider_id = oneprovider:get_provider_id(), storage_id = StorageId, file_id = FileId, blocks = []}}.


