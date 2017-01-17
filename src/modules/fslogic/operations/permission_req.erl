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

-include("proto/oneprovider/provider_messages.hrl").
-include_lib("annotations/include/annotations.hrl").
-include_lib("ctool/include/posix/acl.hrl").

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
check_perms(UserCtx, FileCtx, read) ->
    check_perms_read(UserCtx, FileCtx);
check_perms(UserCtx, FileCtx, write) ->
    check_perms_write(UserCtx, FileCtx);
check_perms(UserCtx, FileCtx, rdwr) ->
    check_perms_rdwr(UserCtx, FileCtx).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks read permission on file.
%% @end
%%--------------------------------------------------------------------
-spec check_perms_read(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:provider_response().
-check_permissions([traverse_ancestors, ?read_object]).
check_perms_read(_UserCtx, _FileCtx) ->
    #provider_response{status = #status{code = ?OK}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks write permission on file.
%% @end
%%--------------------------------------------------------------------
-spec check_perms_write(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:provider_response().
-check_permissions([traverse_ancestors, ?write_object]).
check_perms_write(_UserCtx, _FileCtx) ->
    #provider_response{status = #status{code = ?OK}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks rdwr permission on file.
%% @end
%%--------------------------------------------------------------------
-spec check_perms_rdwr(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:provider_response().
-check_permissions([traverse_ancestors, ?read_object, ?write_object]).
check_perms_rdwr(_UserCtx, _FileCtx) ->
    #provider_response{status = #status{code = ?OK}}.