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
-include_lib("annotations/include/annotations.hrl").
-include_lib("ctool/include/posix/acl.hrl").

%% API
-export([get_acl/2, set_acl/3, remove_acl/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Gets access control list of file.
%% @end
%%--------------------------------------------------------------------
-spec get_acl(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:provider_response().
-check_permissions([traverse_ancestors, ?read_acl]).
get_acl(_UserCtx, FileCtx) ->
    case xattr:get_by_name(FileCtx, ?ACL_KEY) of
        {ok, Val} ->
            #provider_response{
                status = #status{code = ?OK},
                provider_response = #acl{
                    value = fslogic_acl:from_json_format_to_acl(Val)
                }
            };
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOATTR}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Sets access control list of file.
%% @end
%%--------------------------------------------------------------------
-spec set_acl(user_ctx:ctx(), file_ctx:ctx(), #acl{}) ->
    fslogic_worker:provider_response().
-check_permissions([traverse_ancestors, ?write_acl]).
set_acl(UserCtx, FileCtx, #acl{value = Val}) ->
    case xattr:save(FileCtx, ?ACL_KEY, fslogic_acl:from_acl_to_json_format(Val)) of
        {ok, _} ->
            FileUuid = file_ctx:get_uuid_const(FileCtx),
            ok = permissions_cache:invalidate(custom_metadata, FileUuid), %todo pass file_ctx
            ok = sfm_utils:chmod_storage_file(
                user_ctx:new(?ROOT_SESS_ID),
                FileCtx, 8#000
            ),
            fslogic_times:update_ctime(FileCtx, user_ctx:get_user_id(UserCtx)),
            #provider_response{status = #status{code = ?OK}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOENT}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes access control list of file.
%% @end
%%--------------------------------------------------------------------
-spec remove_acl(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:provider_response().
-check_permissions([traverse_ancestors, ?write_acl]).
remove_acl(UserCtx, FileCtx) ->
    case xattr:delete_by_name(FileCtx, ?ACL_KEY) of
        ok ->
            FileUuid = file_ctx:get_uuid_const(FileCtx),
            ok = permissions_cache:invalidate(custom_metadata, FileUuid), %todo pass file_ctx
            {#document{value = #file_meta{mode = Mode}}, FileCtx2} =
                file_ctx:get_file_doc(FileCtx),
            ok = sfm_utils:chmod_storage_file(
                user_ctx:new(?ROOT_SESS_ID),
                FileCtx2, Mode
            ),
            ok = fslogic_event_emitter:emit_file_perm_changed(FileCtx2),
            fslogic_times:update_ctime(FileCtx2, user_ctx:get_user_id(UserCtx)),
            #provider_response{status = #status{code = ?OK}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOENT}}
    end.