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

-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/posix/acl.hrl").

%% API
-export([delete/3, delete_using_trash/3]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Deletes file, and check permissions.
%% If parameter Silent is true, file_removed_event will not be emitted.
%% @end
%%--------------------------------------------------------------------
-spec delete(user_ctx:ctx(), file_ctx:ctx(), Silent :: boolean()) ->
    fslogic_worker:fuse_response().
delete(UserCtx, FileCtx, Silent) ->
    case file_ctx:is_dir(FileCtx) of
        {true, FileCtx2} ->
            file_ctx:assert_not_protected_const(FileCtx2),
            % TODO czemu tutaj jest delete katalogu? ??
            delete_dir(UserCtx, FileCtx2, Silent);
        {false, FileCtx2} ->
            delete_file(UserCtx, FileCtx2, Silent)
    end.


-spec delete_using_trash(user_ctx:ctx(), file_ctx:ctx(), boolean()) ->
    fslogic_worker:fuse_response().
delete_using_trash(UserCtx, FileCtx0, Silent) ->
    file_ctx:assert_not_protected_const(FileCtx0),

    {FileParentCtx, FileCtx1} = file_ctx:get_parent(FileCtx0, UserCtx),
    FileCtx2 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx1,
        [traverse_ancestors, ?delete, ?list_container]
    ),
    fslogic_authz:ensure_authorized(
        UserCtx, FileParentCtx,
        [traverse_ancestors, ?delete_subcontainer]
    ),
    delete_using_trash_insecure(UserCtx, FileCtx2, Silent).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @equiv check_if_empty_and_delete/3 with permission check.
%% @end
%%--------------------------------------------------------------------
-spec delete_dir(user_ctx:ctx(), file_ctx:ctx(), Silent :: boolean()) -> fslogic_worker:fuse_response().
delete_dir(UserCtx, FileCtx0, Silent) ->
    {FileParentCtx, FileCtx1} = file_ctx:get_parent(FileCtx0, UserCtx),
    FileCtx2 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx1,
        [traverse_ancestors, ?delete, ?list_container]
    ),
    fslogic_authz:ensure_authorized(
        UserCtx, FileParentCtx,
        [traverse_ancestors, ?delete_subcontainer]
    ),
    check_if_empty_and_delete(UserCtx, FileCtx2, Silent).


%%--------------------------------------------------------------------
%% @private
%% @equiv delete_insecure/3 with permission check.
%% @end
%%--------------------------------------------------------------------
-spec delete_file(user_ctx:ctx(), file_ctx:ctx(), Silent :: boolean()) ->
    fslogic_worker:fuse_response().
delete_file(UserCtx, FileCtx0, Silent) ->
    {FileParentCtx, FileCtx1} = file_ctx:get_parent(FileCtx0, UserCtx),
    FileCtx2 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx1,
        [traverse_ancestors, ?delete]
    ),
    fslogic_authz:ensure_authorized(
        UserCtx, FileParentCtx,
        [traverse_ancestors, ?delete_object]
    ),
    delete_insecure(UserCtx, FileCtx2, Silent).


-spec delete_using_trash_insecure(user_ctx:ctx(), file_ctx:ctx(), boolean()) ->
    fslogic_worker:fuse_response().
delete_using_trash_insecure(UserCtx, FileCtx, Silent) ->
    ok = trash:move_to_trash(FileCtx),
    {ok, _} = trash:delete_from_trash(FileCtx, UserCtx, Silent),
    ?FUSE_OK_RESP.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if dir is empty and deletes it.
%% @end
%%--------------------------------------------------------------------
-spec check_if_empty_and_delete(user_ctx:ctx(), file_ctx:ctx(), Silent :: boolean()) -> fslogic_worker:fuse_response().
check_if_empty_and_delete(UserCtx, FileCtx, Silent) ->
    case file_ctx:get_file_children(FileCtx, UserCtx, 0, 1) of
        {[], FileCtx2} ->
            delete_insecure(UserCtx, FileCtx2, Silent);
        {_, _FileCtx2} ->
            #fuse_response{status = #status{code = ?ENOTEMPTY}}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Deletes file or directory.
%% If parameter Silent is true, file_removed_event will not be emitted.
%% @end
%%--------------------------------------------------------------------
-spec delete_insecure(user_ctx:ctx(), file_ctx:ctx(), Silent :: boolean()) ->
    fslogic_worker:fuse_response().
delete_insecure(UserCtx, FileCtx, Silent) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    {ok, _} = file_meta:update(FileUuid, fun(FileMeta = #file_meta{}) ->
        {ok, FileMeta#file_meta{deleted = true}}
    end),
    fslogic_delete:delete_file_locally(UserCtx, FileCtx, Silent),
    ?FUSE_OK_RESP.