%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handing requests related to file guid.
%%% @end
%%%--------------------------------------------------------------------
-module(guid_req).
-author("Tomasz Lichon").

-include("modules/fslogic/data_access_control.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").

%% API
-export([resolve_guid/2, get_parent/2, get_file_path/2]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @equiv resolve_guid_insecure/2 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec resolve_guid(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:fuse_response().
resolve_guid(UserCtx, FileCtx0) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0, [?TRAVERSE_ANCESTORS], allow_ancestors
    ),
    resolve_guid_insecure(UserCtx, FileCtx1).


%%--------------------------------------------------------------------
%% @equiv get_parent_insecure/2 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec get_parent(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:provider_response().
get_parent(UserCtx, FileCtx0) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0, [?TRAVERSE_ANCESTORS], allow_ancestors
    ),
    get_parent_insecure(UserCtx, FileCtx1).


%%--------------------------------------------------------------------
%% @equiv get_file_path_insecure/2 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec get_file_path(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:provider_response().
get_file_path(UserCtx, FileCtx0) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0, [?TRAVERSE_ANCESTORS], allow_ancestors
    ),
    get_file_path_insecure(UserCtx, FileCtx1).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Resolves file guid basing on its path.
%% @end
%%--------------------------------------------------------------------
-spec resolve_guid_insecure(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:fuse_response().
resolve_guid_insecure(_UserCtx, FileCtx) ->
    Guid = file_ctx:get_logical_guid_const(FileCtx),
    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = #guid{guid = Guid}
    }.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets parent of file.
%% @end
%%--------------------------------------------------------------------
-spec get_parent_insecure(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:provider_response().
get_parent_insecure(UserCtx, FileCtx) ->
    {ParentGuid, _FileCtx2} = files_tree:get_parent_guid_if_not_root_dir(FileCtx, UserCtx),
    #provider_response{
        status = #status{code = ?OK},
        provider_response = #dir{guid = ParentGuid}
    }.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Translates given file's Guid to absolute path.
%% @end
%%--------------------------------------------------------------------
-spec get_file_path_insecure(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:provider_response().
get_file_path_insecure(UserCtx, FileCtx) ->
    {Path, _FileCtx2} = file_ctx:get_logical_path(FileCtx, UserCtx),
    #provider_response{
        status = #status{code = ?OK},
        provider_response = #file_path{value = Path}
    }.