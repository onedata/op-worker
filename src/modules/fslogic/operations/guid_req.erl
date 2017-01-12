%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Requests related to file guid.
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
%% Resolve file guid basing on its path.
%% @end
%%--------------------------------------------------------------------
-spec resolve_guid(user_ctx:ctx(), file_ctx:ctx()) -> fslogic_worker:fuse_response().
-check_permissions([{traverse_ancestors, 2}]).
resolve_guid(_Ctx, File) ->
    Guid = file_ctx:get_guid_const(File),
    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = #uuid{uuid = Guid}
    }.

%%--------------------------------------------------------------------
%% @doc
%% Get parent of file
%% @end
%%--------------------------------------------------------------------
-spec get_parent(user_ctx:ctx(), fslogic_worker:file()) ->
    fslogic_worker:provider_response().
-check_permissions([{traverse_ancestors, 2}]).
get_parent(Ctx, File) ->
    UserId = user_ctx:get_user_id(Ctx),
    {ParentGuid, _File2} = file_ctx:get_parent_guid(File, UserId),
    #provider_response{
        status = #status{code = ?OK},
        provider_response = #dir{uuid = ParentGuid}
    }.

%%--------------------------------------------------------------------
%% @doc Translates given file's UUID to absolute path.
%% @end
%%--------------------------------------------------------------------
-spec get_file_path(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:provider_response().
get_file_path(Ctx, File) ->
    {Path, _File2} = file_ctx:get_logical_path(File, Ctx),
    #provider_response{
        status = #status{code = ?OK},
        provider_response = #file_path{value = Path}
    }.

%%%===================================================================
%%% Internal functions
%%%===================================================================