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

-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("annotations/include/annotations.hrl").

%% API
-export([resolve_guid/2, get_parent/2, get_file_path/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Resolves file guid basing on its path.
%% @end
%%--------------------------------------------------------------------
-spec resolve_guid(user_ctx:ctx(), file_ctx:ctx()) -> fslogic_worker:fuse_response().
-check_permissions([{traverse_ancestors, 2}]).
resolve_guid(_UserCtx, FileCtx) ->
    Guid = file_ctx:get_guid_const(FileCtx),
    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = #uuid{uuid = Guid}
    }.

%%--------------------------------------------------------------------
%% @doc
%% Gets parent of file
%% @end
%%--------------------------------------------------------------------
-spec get_parent(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:provider_response().
-check_permissions([{traverse_ancestors, 2}]).
get_parent(UserCtx, FileCtx) ->
    UserId = user_ctx:get_user_id(UserCtx),
    {ParentGuid, _FileCtx2} = file_ctx:get_parent_guid(FileCtx, UserId),
    #provider_response{
        status = #status{code = ?OK},
        provider_response = #dir{uuid = ParentGuid}
    }.

%%--------------------------------------------------------------------
%% @doc
%% Translates given file's Guid to absolute path.
%% @end
%%--------------------------------------------------------------------
-spec get_file_path(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:provider_response().
get_file_path(UserCtx, FileCtx) ->
    {Path, _FileCtx2} = file_ctx:get_logical_path(FileCtx, UserCtx),
    #provider_response{
        status = #status{code = ?OK},
        provider_response = #file_path{value = Path}
    }.