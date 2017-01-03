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
-export([resolve_guid/2, get_parent/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Resolve file guid basing on its path.
%% @end
%%--------------------------------------------------------------------
-spec resolve_guid(fslogic_context:ctx(), file_info:file_info()) -> fslogic_worker:fuse_response().
-check_permissions([{traverse_ancestors, 2}]).
resolve_guid(_Ctx, File) ->
    {Guid, _} = file_info:get_guid(File),
    #fuse_response{status = #status{code = ?OK}, fuse_response = #uuid{uuid = Guid}}.

%%--------------------------------------------------------------------
%% @doc
%% Get parent of file
%% @end
%%--------------------------------------------------------------------
-spec get_parent(fslogic_context:ctx(), fslogic_worker:file()) ->
    fslogic_worker:provider_response().
-check_permissions([{traverse_ancestors, 2}]).
get_parent(Ctx, File) ->
    UserId = fslogic_context:get_user_id(Ctx),
    {ParentGuid, _File2} = file_info:get_parent_guid(File, UserId),

    #provider_response{
        status = #status{code = ?OK},
        provider_response = #dir{uuid = ParentGuid}
    }.

%%%===================================================================
%%% Internal functions
%%%===================================================================