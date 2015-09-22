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
-export([get_file_location/4, get_new_file_location/6, unlink/2, truncate/3, get_helper_params/3]).

%%%===================================================================
%%% API functions
%%%===================================================================


truncate(Ctx, Entry, Size) ->
    #fuse_response{status = #status{code = ?OK}}.

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
-spec get_file_location(fslogic_worker:ctx(), File :: fslogic_worker:file(), Flags :: fslogic_worker:open_flags(), ForceClusterProxy :: boolean()) ->
    no_return().
get_file_location(#fslogic_ctx{session_id = SessId} = CTX, File, _Flags, _ForceClusterProxy) ->
    {ok, #document{key = UUID}} = file_meta:get(File),
    ?error("get_file_location for ~p ~p", [UUID, File]),
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
    Mode :: file_meta:posix_permissions(), Flags :: fslogic_worker:open_flags(),
    ForceClusterProxy :: boolean()) -> no_return().
get_new_file_location(#fslogic_ctx{session = #session{identity = #identity{user_id = UUID}}} = CTX,
    UUID, Name, Mode, _Flags, _ForceClusterProxy) ->
    {ok, #document{key = DefaultSpaceUUID}} = fslogic_spaces:get_default_space(CTX),
    get_new_file_location(CTX, DefaultSpaceUUID, Name, Mode, _Flags, _ForceClusterProxy);
get_new_file_location(#fslogic_ctx{session_id = SessId} = CTX, ParentUUID, Name, Mode, _Flags, _ForceClusterProxy) ->

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

    Tokens = fslogic_path:split(FileId),
    LeafLess = fslogic_path:join(lists:sublist(Tokens, 1, length(Tokens) - 1)),
    case storage_file_manager:mkdir(Storage, LeafLess, 8#333, true) of
        ok -> ok;
        {error, eexist} ->
            ok
    end,

    %% @todo: remove this when this logic will be transferred to oneclient
    storage_file_manager:unlink(Storage, FileId),
    ok = storage_file_manager:create(Storage, FileId, Mode),

    ok = file_watcher:insert_open_watcher(UUID, SessId),

    #fuse_response{status = #status{code = ?OK}, fuse_response =
        #file_location{uuid = UUID, provider_id = oneprovider:get_provider_id(), storage_id = StorageId, file_id = FileId, blocks = []}}.


unlink(#fslogic_ctx{session_id = SessId} = CTX, File) ->
    #document{value = #file_location{blocks = Blocks, storage_id = DSID, file_id = DFID}} = fslogic_utils:get_local_file_location(File),

    ok = file_meta:delete(File),

    ToDelete = lists:usort([{DSID, DFID} | [{SID, FID} || #file_block{storage_id = SID, file_id = FID} <- Blocks]]),
    Results = lists:map( %% @todo: run this via task manager
        fun({StorageId, FileId}) ->
            case storage:get(StorageId) of
                {ok, Storage} ->
                    case storage_file_manager:unlink(Storage, FileId) of
                        ok -> ok;
                        {error, Reason1} ->
                            {{StorageId, FileId}, {error, Reason1}}
                    end ;
                {error, Reason2} ->
                    {{StorageId, FileId}, {error, Reason2}}
            end
        end, ToDelete),
    case Results -- [ok] of
        [] -> ok;
        Errors ->
            lists:foreach(
                fun({{SID0, FID0}, {error, Reason0}}) ->
                    ?error("Cannot unlink file ~p from storage ~p due to: ~p", [FID0, SID0, Reason0])
                end, Errors)
    end,

    #fuse_response{status = #status{code = ?OK}}.

