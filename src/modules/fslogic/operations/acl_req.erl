%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Requests operating on file access control lists.
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
%% @doc Get access control list of file.
%%--------------------------------------------------------------------
-spec get_acl(fslogic_context:ctx(), file_info:file_info()) ->
    fslogic_worker:provider_response().
-check_permissions([{traverse_ancestors, 2}, {?read_acl, 2}]).
get_acl(_Ctx, File) ->
    {{uuid, FileUuid} , _File2} = file_info:get_uuid_entry(File),
    case xattr:get_by_name(FileUuid, ?ACL_KEY) of %todo pass file_info
        {ok, Val} ->
            #provider_response{status = #status{code = ?OK}, provider_response = #acl{value = fslogic_acl:from_json_format_to_acl(Val)}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOATTR}}
    end.

%%--------------------------------------------------------------------
%% @doc Sets access control list of file.
%%--------------------------------------------------------------------
-spec set_acl(fslogic_context:ctx(), file_info:file_info(), #acl{}) ->
    fslogic_worker:provider_response().
-check_permissions([{traverse_ancestors, 2}, {?write_acl, 2}]).
set_acl(Ctx, File, #acl{value = Val}) ->
    {{uuid, FileUuid} , _File2} = file_info:get_uuid_entry(File),
    case xattr:save(FileUuid, ?ACL_KEY, fslogic_acl:from_acl_to_json_format(Val)) of %todo pass file_info
        {ok, _} ->
            ok = permissions_cache:invalidate_permissions_cache(custom_metadata, FileUuid), %todo pass file_info
            ok = sfm_utils:chmod_storage_files( %todo pass file_info
                fslogic_context:set_session_id(Ctx, ?ROOT_SESS_ID),
                {uuid, FileUuid}, 8#000
            ),
            fslogic_times:update_ctime({uuid, FileUuid}, fslogic_context:get_user_id(Ctx)), %todo pass file_info
            #provider_response{status = #status{code = ?OK}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOENT}}
    end.

%%--------------------------------------------------------------------
%% @doc Removes access control list of file.
%%--------------------------------------------------------------------
-spec remove_acl(fslogic_context:ctx(), file_info:file_info()) ->
    fslogic_worker:provider_response().
-check_permissions([{traverse_ancestors, 2}, {?write_acl, 2}]).
remove_acl(Ctx, File) ->
    {{uuid, FileUuid} , _File2} = file_info:get_uuid_entry(File),
    case xattr:delete_by_name(FileUuid, ?ACL_KEY) of
        ok ->
            ok = permissions_cache:invalidate_permissions_cache(custom_metadata, FileUuid), %todo pass file_info
            {#document{value = #file_meta{mode = Mode}}, File2} = file_info:get_file_doc(File),
            ok = sfm_utils:chmod_storage_files( %todo pass file_info
                fslogic_context:set_session_id(Ctx, ?ROOT_SESS_ID),
                {uuid, FileUuid}, Mode
            ),
            ok = fslogic_event:emit_file_perm_changed(FileUuid), %todo pass file_info
            fslogic_times:update_ctime({uuid, FileUuid}, fslogic_context:get_user_id(Ctx)), %todo pass file_info
            #provider_response{status = #status{code = ?OK}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOENT}}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================