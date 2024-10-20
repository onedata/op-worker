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
%%% Note: this module operates on referenced uuids - all operations on hardlinks
%%% are treated as operations on original file. Thus, acls are shared
%%% between hardlinks and original file.
%%% @end
%%%--------------------------------------------------------------------
-module(acl_req).
-author("Tomasz Lichon").

-include("modules/fslogic/data_access_control.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/metadata.hrl").
-include("proto/oneprovider/provider_messages.hrl").

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
    fslogic_worker:provider_response() | no_return().
get_acl(UserCtx, FileCtx0) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [?TRAVERSE_ANCESTORS, ?OPERATIONS(?read_acl_mask)]
    ),
    get_acl_insecure(UserCtx, FileCtx1).


%%--------------------------------------------------------------------
%% @equiv set_acl_insecure/3 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec set_acl(user_ctx:ctx(), file_ctx:ctx(), acl:acl()) ->
    fslogic_worker:provider_response().
set_acl(UserCtx, FileCtx0, Acl) ->
    file_ctx:assert_not_trash_dir_const(FileCtx0),
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [?TRAVERSE_ANCESTORS, ?OPERATIONS(?write_acl_mask)]
    ),
    set_acl_insecure(UserCtx, FileCtx1, Acl).


%%--------------------------------------------------------------------
%% @equiv remove_acl_insecure/2 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec remove_acl(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:provider_response().
remove_acl(UserCtx, FileCtx0) ->
    file_ctx:assert_not_trash_dir_const(FileCtx0),
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [?TRAVERSE_ANCESTORS, ?OPERATIONS(?write_acl_mask)]
    ),
    remove_acl_insecure(UserCtx, FileCtx1).


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
    {Acl, _} = file_ctx:get_acl(file_ctx:ensure_based_on_referenced_guid(FileCtx)),
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
%% Modifies access control list of specified file.
%% @end
%%--------------------------------------------------------------------
-spec set_acl_insecure(user_ctx:ctx(), file_ctx:ctx(), acl:acl()) ->
    fslogic_worker:provider_response().
set_acl_insecure(_UserCtx, FileCtx, Acl) ->
    {IsDir, FileCtx2} = file_ctx:is_dir(FileCtx),

    case IsDir of
        true -> acl:validate(Acl, dir);
        false -> acl:validate(Acl, file)
    end,

    % ACLs are kept in database without names, as they might change.
    % Strip the names here.
    AclWithoutNames = acl:strip_names(Acl),
    FileUuid = file_ctx:get_referenced_uuid_const(FileCtx),
    case file_meta:update_acl(FileUuid, AclWithoutNames) of
        ok ->
            ok = permissions_cache:invalidate(),
            times_api:touch(FileCtx2, [?attr_ctime]),
            fslogic_event_emitter:emit_file_perm_changed(FileCtx2),
            #provider_response{status = #status{code = ?OK}};
        {error, not_found} ->
            #provider_response{status = #status{code = ?ENOENT}}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Clears access control list of specified file.
%% @end
%%--------------------------------------------------------------------
-spec remove_acl_insecure(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:provider_response().
remove_acl_insecure(_UserCtx, FileCtx) ->
    FileUuid = file_ctx:get_referenced_uuid_const(FileCtx),
    case file_meta:update_acl(FileUuid, []) of
        ok ->
            ok = permissions_cache:invalidate(),
            times_api:touch(FileCtx, [?attr_ctime]),
            fslogic_event_emitter:emit_file_perm_changed(FileCtx),
            #provider_response{status = #status{code = ?OK}};
        {error, not_found} ->
            #provider_response{status = #status{code = ?ENOENT}}
    end.
