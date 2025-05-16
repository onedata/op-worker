%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Interface for managing file tree traversal (requests are delegated to middleware_worker).
%%% @end
%%%-------------------------------------------------------------------
-module(mi_file_tree).
-author("Bartosz Walkowicz").

-include("middleware/middleware.hrl").

%% API
-export([
    get_path/2,
    get_parent/2
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec get_path(session:id(), file_id:file_guid()) ->
    file_meta:path().
get_path(SessionId, FileGuid) ->
    middleware_worker:check_exec(SessionId, FileGuid, #file_path_get_request{}).


-spec get_parent(session:id(), lfm:file_key()) ->
    file_id:file_guid().
get_parent(SessionId, FileKey) ->
    FileGuid = lfm_file_key:resolve_file_key(SessionId, FileKey, do_not_resolve_symlink),
    middleware_worker:check_exec(SessionId, FileGuid, #file_parent_get_request{}).
