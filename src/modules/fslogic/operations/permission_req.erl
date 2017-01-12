%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Requests for checking file permission.
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
%% Check given permission on file.
%% @end
%%--------------------------------------------------------------------
-spec check_perms(user_ctx:ctx(), file_ctx:ctx(), fslogic_worker:open_flag()) ->
    fslogic_worker:provider_response().
check_perms(Ctx, Uuid, read) ->
    check_perms_read(Ctx, Uuid);
check_perms(Ctx, Uuid, write) ->
    check_perms_write(Ctx, Uuid);
check_perms(Ctx, Uuid, rdwr) ->
    check_perms_rdwr(Ctx, Uuid).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Check read permission on file.
%% @end
%%--------------------------------------------------------------------
-spec check_perms_read(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:provider_response().
-check_permissions([{traverse_ancestors, 2}, {?read_object, 2}]).
check_perms_read(_Ctx, _File) ->
    #provider_response{status = #status{code = ?OK}}.

%%--------------------------------------------------------------------
%% @doc
%% Check write permission on file.
%% @end
%%--------------------------------------------------------------------
-spec check_perms_write(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:provider_response().
-check_permissions([{traverse_ancestors, 2}, {?write_object, 2}]).
check_perms_write(_Ctx, _File) ->
    #provider_response{status = #status{code = ?OK}}.

%%--------------------------------------------------------------------
%% @doc
%% Check rdwr permission on file.
%% @end
%%--------------------------------------------------------------------
-spec check_perms_rdwr(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:provider_response().
-check_permissions([{traverse_ancestors, 2}, {?read_object, 2}, {?write_object, 2}]).
check_perms_rdwr(_Ctx, _File) ->
    #provider_response{status = #status{code = ?OK}}.