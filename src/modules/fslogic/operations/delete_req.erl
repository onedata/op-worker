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

-include("modules/fslogic/data_access_control.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([delete/3, delete_using_trash/3]).
% should only be used after permissions check
-export([delete_using_trash_insecure/3]).

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
            file_ctx:assert_not_special_const(FileCtx2),
            delete_dir(UserCtx, FileCtx2, Silent);
        {false, FileCtx2} ->
            delete_file(UserCtx, FileCtx2, Silent)
    end.


-spec delete_using_trash(user_ctx:ctx(), file_ctx:ctx(), boolean()) ->
    fslogic_worker:fuse_response().
delete_using_trash(UserCtx, FileCtx0, EmitEvents) ->
    file_ctx:assert_not_special_const(FileCtx0),
    FileCtx1 = file_ctx:assert_is_dir(FileCtx0),

    {FileParentCtx, FileCtx2} = file_tree:get_parent(FileCtx1, UserCtx),
    FileCtx3 = fslogic_authz:ensure_authorized(UserCtx, FileCtx2, [
        ?TRAVERSE_ANCESTORS,
        ?OPERATIONS(?delete_mask, ?list_container_mask, ?traverse_container_mask, ?delete_child_mask)
    ]),
    fslogic_authz:ensure_authorized(
        UserCtx, FileParentCtx,
        [?TRAVERSE_ANCESTORS, ?OPERATIONS(?delete_child_mask)]
    ),
    delete_using_trash_insecure(UserCtx, FileCtx3, EmitEvents).


-spec delete_using_trash_insecure(user_ctx:ctx(), file_ctx:ctx(), boolean()) ->
    fslogic_worker:fuse_response().
delete_using_trash_insecure(UserCtx, FileCtx, EmitEvents) ->
    {ParentGuid, FileCtx2} = file_tree:get_parent_guid_if_not_root_dir(FileCtx, UserCtx),
    {Filename, FileCtx3} = file_ctx:get_aliased_name(FileCtx2, UserCtx),
    FileCtx4 = trash:move_to_trash(FileCtx3, UserCtx),
    {ok, _} = trash:schedule_deletion_from_trash(FileCtx4, UserCtx, EmitEvents, file_id:guid_to_uuid(ParentGuid), Filename),
    ?FUSE_OK_RESP.


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
    {FileParentCtx, FileCtx1} = file_tree:get_parent(FileCtx0, UserCtx),
    FileCtx2 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx1,
        [?TRAVERSE_ANCESTORS, ?OPERATIONS(?delete_mask, ?list_container_mask)]
    ),
    fslogic_authz:ensure_authorized(
        UserCtx, FileParentCtx,
        [?TRAVERSE_ANCESTORS, ?OPERATIONS(?delete_child_mask)]
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
    {FileParentCtx, FileCtx1} = file_tree:get_parent(FileCtx0, UserCtx),
    FileCtx2 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx1,
        [?TRAVERSE_ANCESTORS, ?OPERATIONS(?delete_mask)]
    ),
    fslogic_authz:ensure_authorized(
        UserCtx, FileParentCtx,
        [?TRAVERSE_ANCESTORS, ?OPERATIONS(?delete_child_mask)]
    ),
    delete_insecure(UserCtx, FileCtx2, Silent).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if dir is empty and deletes it.
%% @end
%%--------------------------------------------------------------------
-spec check_if_empty_and_delete(user_ctx:ctx(), file_ctx:ctx(), Silent :: boolean()) -> fslogic_worker:fuse_response().
check_if_empty_and_delete(UserCtx, FileCtx, Silent) ->
    case file_tree:list_children(FileCtx, UserCtx, #{offset => 0, limit => 1, tune_for_large_continuous_listing => false}) of
        {[], _ListExtendedInfo, FileCtx2} ->
            delete_insecure(UserCtx, FileCtx2, Silent);
        {_, _, _FileCtx2} ->
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
    FileUuid = file_ctx:get_logical_uuid_const(FileCtx),
    case file_meta:update(FileUuid, fun
        (#file_meta{deleted = true}) ->
            {error, already_deleted};
        (FileMeta = #file_meta{}) ->
            {ok, FileMeta#file_meta{deleted = true}}
    end) of
        {ok, #document{value = #file_meta{provider_id = ProviderId}}} ->
            fslogic_delete:delete_file_locally(UserCtx, FileCtx, ProviderId, Silent);
        {error, already_deleted} ->
            ok
    end,
    ?FUSE_OK_RESP.