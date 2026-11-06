%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handing requests for checking file permission.
%%% @end
%%%--------------------------------------------------------------------
-module(permission_req).
-author("Tomasz Lichon").

-include("modules/fslogic/data_access_control.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneprovider/provider_messages.hrl").

%% API
-export([check_perms/3]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Checks given permission on file.
%% @end
%%--------------------------------------------------------------------
-spec check_perms(user_ctx:ctx(), file_ctx:ctx(), fslogic_worker:open_flag()) ->
    fslogic_worker:provider_response().
check_perms(UserCtx, FileCtx, OpenFlag) ->
    fslogic_authz:ensure_authorized(UserCtx, FileCtx, required_perms(OpenFlag)),
    #provider_response{status = #status{code = ?OK}}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
required_perms(read) ->
    [?TRAVERSE_ANCESTORS, ?OPERATIONS(?read_object_mask)];
required_perms(write) ->
    [?TRAVERSE_ANCESTORS, ?OPERATIONS(?write_object_mask)];
required_perms(rdwr) ->
    [?TRAVERSE_ANCESTORS, ?OPERATIONS(?read_object_mask, ?write_object_mask)].
