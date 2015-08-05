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
-export([get_file_location/3, get_new_file_location/5]).

%%%===================================================================
%%% API functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc Gets file location (implicit file open operation). Allows to force-select ClusterProxy helper.
%% For best performance use following arg types: document -> uuid -> path
%% @end
%%--------------------------------------------------------------------
-spec get_file_location(File :: fslogic_worker:file(), Flags :: fslogic_worker:open_flags(), ForceClusterProxy :: boolean()) ->
    no_return().
get_file_location(_File, _Flags, _ForceClusterProxy) ->
    ?NOT_IMPLEMENTED.


%%--------------------------------------------------------------------
%% @doc Gets new file location (implicit mknod operation).
%% @end
%%--------------------------------------------------------------------
-spec get_new_file_location(fslogic_worker:ctx(), ParentUUID :: file_meta:uuid(), Name :: file_meta:name(), Flags :: fslogic_worker:open_flags(),
    ForceClusterProxy :: boolean()) -> no_return().
get_new_file_location(#fslogic_ctx{session_id = SessId} = CTX, ParentUUID, Name, _Flags, _ForceClusterProxy) ->

    {ok, #document{key = StorageId, value = Storage}} = fslogic_storage:select_storage(CTX),
    Mode = 8#644,
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

    FileId = file_meta:gen_path({uuid, UUID}),
    ok = storage_file_manager:create(Storage, FileId, Mode, true),

    Location = #file_location{blocks = [#file_block{offset = 0, size = 0, file_id = FileId, storage_id = StorageId}],
        provider_id = cluster_manager:provider_id()},
    {ok, LocId} = file_location:create(#document{value = Location}),

    file_meta:attach_location({uuid, UUID}, LocId, cluster_manager:provider_id()),

    SessId = fslogic_context:
    ok = file_watcher:insert_open_watcher(UUID, SessId),

    #fuse_response{status = #status{code = ?OK}}.
