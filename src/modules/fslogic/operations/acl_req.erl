%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handing requests operating on file access
%%% control lists.
%%% @end
%%%--------------------------------------------------------------------
-module(acl_req).
-author("Tomasz Lichon").

-include("proto/oneprovider/provider_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/metadata.hrl").
-include_lib("ctool/include/posix/acl.hrl").

%% API
-export([get_acl/2, set_acl/3, remove_acl/2]).

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @equiv get_acl_insecure/2 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec get_acl(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:provider_response().
get_acl(_UserCtx, FileCtx) ->
    check_permissions:execute(
        [traverse_ancestors, ?read_acl],
        [_UserCtx, FileCtx],
        fun get_acl_insecure/2).


%%--------------------------------------------------------------------
%% @equiv set_acl_insecure/3 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec set_acl(user_ctx:ctx(), file_ctx:ctx(), acl:acl()) ->
    fslogic_worker:provider_response().
set_acl(_UserCtx, FileCtx, Acl) ->
    check_permissions:execute(
        [traverse_ancestors, ?write_acl],
        [_UserCtx, FileCtx, Acl],
        fun set_acl_insecure/3).


%%--------------------------------------------------------------------
%% @equiv remove_acl_insecure/2 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec remove_acl(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:provider_response().
remove_acl(_UserCtx, FileCtx) ->
    check_permissions:execute(
        [traverse_ancestors, ?write_acl],
        [_UserCtx, FileCtx],
        fun remove_acl_insecure/2).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets access control list of file.
%% @end
%%--------------------------------------------------------------------
-spec get_acl_insecure(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:provider_response().
get_acl_insecure(_UserCtx, FileCtx) ->
    {Acl, _} = file_ctx:get_acl(FileCtx),
    % ACLs are kept in database without names, as they might change.
    % Resolve the names here.
    #provider_response{
        status = #status{code = ?OK},
        provider_response = #acl{
            value = acl:add_names(Acl)
        }
    }.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sets access control list of file.
%% @end
%%--------------------------------------------------------------------
-spec set_acl_insecure(user_ctx:ctx(), file_ctx:ctx(), acl:acl()) ->
    fslogic_worker:provider_response().
set_acl_insecure(_UserCtx, FileCtx, Acl) ->
    % ACLs are kept in database without names, as they might change.
    % Strip the names here.
    AclWithoutNames = case Acl of
        undefined -> undefined;
        _ -> acl:strip_names(Acl)
    end,

    FileUuid = file_ctx:get_uuid_const(FileCtx),
    case file_meta:update_perms(FileUuid, undefined, AclWithoutNames, acl) of
        ok ->
            ok = permissions_cache:invalidate(),
            fslogic_times:update_ctime(FileCtx),
            #provider_response{status = #status{code = ?OK}};
        {error, not_found} ->
            #provider_response{status = #status{code = ?ENOENT}}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Clears access control list of file and changes active permissions
%% type to posix.
%% @end
%%--------------------------------------------------------------------
-spec remove_acl_insecure(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:provider_response().
remove_acl_insecure(_UserCtx, FileCtx) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    case file_meta:update_perms(FileUuid, undefined, [], posix) of
        ok ->
            ok = permissions_cache:invalidate(),
            fslogic_times:update_ctime(FileCtx),
            #provider_response{status = #status{code = ?OK}};
        {error, not_found} ->
            #provider_response{status = #status{code = ?ENOENT}}
    end.
