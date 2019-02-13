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

%% API
-export([delete/3]).

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
    check_permissions:execute(
        [traverse_ancestors],
        [UserCtx, FileCtx, Silent],
        fun(UserCtx_, FileCtx_, Silent_) ->
            case file_ctx:is_dir(FileCtx_) of
                {true, FileCtx2} ->
                    delete_dir(UserCtx_, FileCtx2, Silent_);
                {false, FileCtx2} ->
                    delete_file(UserCtx_, FileCtx2, Silent_)
            end
        end).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @equiv check_if_empty_and_delete/3 with permission check.
%% @end
%%--------------------------------------------------------------------
-spec delete_dir(user_ctx:ctx(), file_ctx:ctx(), Silent :: boolean()) ->
    fslogic_worker:fuse_response().
delete_dir(UserCtx, FileCtx, Silent) ->
    check_permissions:execute(
        [{?delete_subcontainer, parent}, ?delete, ?list_container],
        [UserCtx, FileCtx, Silent],
        fun check_if_empty_and_delete/3).

%%--------------------------------------------------------------------
%% @private
%% @equiv delete_insecure/3 with permission check.
%% @end
%%--------------------------------------------------------------------
-spec delete_file(user_ctx:ctx(), file_ctx:ctx(), Silent :: boolean()) ->
    fslogic_worker:fuse_response().
delete_file(UserCtx, FileCtx, Silent) ->
    check_permissions:execute(
        [{?delete_object, parent}, ?delete],
        [UserCtx, FileCtx, Silent],
        fun delete_insecure/3).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if dir is empty and deletes it.
%% @end
%%--------------------------------------------------------------------
-spec check_if_empty_and_delete(user_ctx:ctx(), file_ctx:ctx(),
    Silent :: boolean()) -> fslogic_worker:fuse_response().
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
    fslogic_delete:check_if_opened_and_remove(UserCtx, FileCtx, Silent, false),
    fslogic_delete:remove_auxiliary_documents(FileCtx),
    #fuse_response{status = #status{code = ?OK}}.