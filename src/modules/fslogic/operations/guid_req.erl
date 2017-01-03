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
-include_lib("annotations/include/annotations.hrl").

%% API
-export([resolve_guid/2]).

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

%%%===================================================================
%%% Internal functions
%%%===================================================================