%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Request deleting file or dir.
%%% @end
%%%--------------------------------------------------------------------
-module(delete_req).
-author("Tomasz Lichon").

-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("annotations/include/annotations.hrl").

%% API
-export([delete/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Deletes file.
%% If parameter Silent is true, file_removed_event will not be emitted.
%% @end
%%--------------------------------------------------------------------
-spec delete(fslogic_context:ctx(), file_info:file_info(), Silent :: boolean()) ->
    fslogic_worker:fuse_response().
-check_permissions([{traverse_ancestors, 2}]).
delete(Ctx, File, Silent) ->
    case file_info:is_dir(File) of
        {true, File2} ->
            delete_dir(Ctx, File2, Silent);
        {false, File2} ->
            delete_file(Ctx, File2, Silent)
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @equiv delete_impl(Ctx, File, Silent) with permission check
%%--------------------------------------------------------------------
-spec delete_dir(fslogic_context:ctx(), file_info:file_info(), Silent :: boolean()) ->
    fslogic_worker:fuse_response().
-check_permissions([{?delete_subcontainer, {parent, 2}}, {?delete, 2}, {?list_container, 2}]).
delete_dir(Ctx, File, Silent) ->
    check_if_empty_and_delete(Ctx, File, Silent).

%%--------------------------------------------------------------------
%% @private
%% @equiv delete_impl(Ctx, File, Silent) with permission check
%%--------------------------------------------------------------------
-spec delete_file(fslogic_context:ctx(), file_info:file_info(), Silent :: boolean()) ->
    fslogic_worker:fuse_response().
-check_permissions([{?delete_object, {parent, 2}}, {?delete, 2}]).
delete_file(Ctx, File, Silent) ->
    delete_impl(Ctx, File, Silent).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Check if dir is empty and delete it.
%% @end
%%--------------------------------------------------------------------
-spec check_if_empty_and_delete(fslogic_context:ctx(), file_info:file_info(), Silent :: boolean()) ->
    fslogic_worker:fuse_response().
check_if_empty_and_delete(Ctx, File, Silent)  ->
    case file_info:get_file_children(File, 0, 1) of
        {[], File2} ->
            delete_impl(Ctx, File2, Silent);
        {_, _File2} ->
            #fuse_response{status = #status{code = ?ENOTEMPTY}}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Deletes file or directory
%% If parameter Silent is true, file_removed_event will not be emitted.
%% @end
%%--------------------------------------------------------------------
-spec delete_impl(fslogic_context:ctx(), file_info:file_info(), Silent :: boolean()) ->
    fslogic_worker:fuse_response().
delete_impl(Ctx, File, Silent) ->
    {Guid, _File2} = file_info:get_guid(File),
    FileUuid = fslogic_uuid:guid_to_uuid(Guid),
    ok = worker_proxy:call(fslogic_deletion_worker,
        {fslogic_deletion_request, Ctx, FileUuid, Silent}), %todo pass file_info here
    #fuse_response{status = #status{code = ?OK}}.