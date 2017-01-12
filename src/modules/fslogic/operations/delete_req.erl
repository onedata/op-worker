%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handing request deleting file or dir.
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
-spec delete(user_ctx:ctx(), file_ctx:ctx(), Silent :: boolean()) ->
    fslogic_worker:fuse_response().
-check_permissions([{traverse_ancestors, 2}]).
delete(UserCtx, FileCtx, Silent) ->
    case file_ctx:is_dir(FileCtx) of
        {true, FileCtx2} ->
            delete_dir(UserCtx, FileCtx2, Silent);
        {false, FileCtx2} ->
            delete_file(UserCtx, FileCtx2, Silent)
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @equiv delete_insecure(UserCtx, FileCtx, Silent) with permission check
%% @end
%%--------------------------------------------------------------------
-spec delete_dir(user_ctx:ctx(), file_ctx:ctx(), Silent :: boolean()) ->
    fslogic_worker:fuse_response().
-check_permissions([{?delete_subcontainer, {parent, 2}}, {?delete, 2}, {?list_container, 2}]).
delete_dir(UserCtx, FileCtx, Silent) ->
    check_if_empty_and_delete(UserCtx, FileCtx, Silent).

%%--------------------------------------------------------------------
%% @private
%% @equiv delete_insecure(UserCtx, FileCtx, Silent) with permission check
%% @end
%%--------------------------------------------------------------------
-spec delete_file(user_ctx:ctx(), file_ctx:ctx(), Silent :: boolean()) ->
    fslogic_worker:fuse_response().
-check_permissions([{?delete_object, {parent, 2}}, {?delete, 2}]).
delete_file(UserCtx, FileCtx, Silent) ->
    delete_insecure(UserCtx, FileCtx, Silent).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if dir is empty and deletes it.
%% @end
%%--------------------------------------------------------------------
-spec check_if_empty_and_delete(user_ctx:ctx(), file_ctx:ctx(), Silent :: boolean()) ->
    fslogic_worker:fuse_response().
check_if_empty_and_delete(UserCtx, FileCtx, Silent)  ->
    case file_ctx:get_file_children(FileCtx, UserCtx, 0, 1) of
        {[], FileCtx2} ->
            delete_insecure(UserCtx, FileCtx2, Silent);
        {_, _FileCtx2} ->
            #fuse_response{status = #status{code = ?ENOTEMPTY}}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Deletes file or directory
%% If parameter Silent is true, file_removed_event will not be emitted.
%% @end
%%--------------------------------------------------------------------
-spec delete_insecure(user_ctx:ctx(), file_ctx:ctx(), Silent :: boolean()) ->
    fslogic_worker:fuse_response().
delete_insecure(UserCtx, FileCtx, Silent) ->
    fslogic_deletion_worker:request_deletion(UserCtx, FileCtx, Silent),
    #fuse_response{status = #status{code = ?OK}}.