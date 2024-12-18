%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module responsible for managing QoS status uptree (bottom up).
%%% For more details consult `qos_status` module doc.
%%% @end
%%%-------------------------------------------------------------------
-module(qos_uptree_status).
-author("Michal Stanisz").

-include("modules/datastore/qos.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    check/2
]).
-export([
    report_traverse_started/3,
    report_traverse_finished/2,
    report_next_batch/5, 
    report_finished_for_dir/2,
    report_finished_for_file/3,
    report_file_deleted/3
]).

% Describes whether link should be added upon successful function execution
-type link_strategy() :: add_link | no_link.

-define(TRAVERSE_LINKS_KEY(TraverseId), <<"qos_status_traverse", TraverseId/binary>>).

%%%===================================================================
%%% API
%%%===================================================================

-spec check(file_ctx:ctx(), qos_entry:doc()) -> boolean().
check(FileCtx, QosEntryDoc) ->
    {ok, AllTraverseReqs} = qos_entry:get_traverse_reqs(QosEntryDoc),
    qos_traverse_req:are_all_started(AllTraverseReqs) andalso
        check_traverses(FileCtx, QosEntryDoc).


-spec report_traverse_started(traverse:id(), file_ctx:ctx(), [qos_entry:id()]) -> {ok, file_ctx:ctx()}.
report_traverse_started(TraverseId, FileCtx, QosEntries) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    FileUuid = file_ctx:get_logical_uuid_const(FileCtx),
    lists:foreach(fun(QosEntryId) ->
        ok = qos_entry:add_to_traverses_list(QosEntryId, TraverseId, FileUuid),
        ok = qos_entry:remove_traverse_req(QosEntryId, TraverseId)
    end, QosEntries),
    ok = qos_status_model:create(SpaceId, TraverseId, FileUuid, ?QOS_STATUS_TRAVERSE_START_DIR),
    {ok, FileCtx}.


-spec report_traverse_finished(traverse:id(), file_ctx:ctx()) -> ok | {error, term()}.
report_traverse_finished(TraverseId, FileCtx) ->
    case get_uuid_based_path(FileCtx) of
        not_synced ->
            ok;
        {Path, _} ->
            qos_status_links:delete_link(?TRAVERSE_LINKS_KEY(TraverseId), Path)
    end.


-spec report_next_batch(traverse:id(), file_ctx:ctx(),
    ChildrenDirs :: [file_meta:uuid()], ChildrenFiles :: [file_meta:uuid()], 
    BatchLastFilename :: file_meta:name()) -> ok.
report_next_batch(TraverseId, FileCtx, ChildrenDirs, ChildrenFiles, BatchLastFilename) ->
    {ok, _} = qos_status_model:update(TraverseId, file_ctx:get_logical_uuid_const(FileCtx),
        fun(#qos_status{
            child_dirs_count = ChildDirsCount, 
            current_batch_last_filename = LN
        } = Value) ->
            {ok, Value#qos_status{
                files_list = ChildrenFiles,
                previous_batch_last_filename = LN,
                current_batch_last_filename = utils:ensure_defined(BatchLastFilename, LN),
                child_dirs_count = ChildDirsCount + length(ChildrenDirs)}
            }
        end),
    lists:foreach(fun(ChildDirUuid) ->
        qos_status_model:create(file_ctx:get_space_id_const(FileCtx), TraverseId, ChildDirUuid, 
            ?QOS_STATUS_TRAVERSE_CHILD_DIR)
    end, ChildrenDirs).


-spec report_finished_for_dir(traverse:id(), file_ctx:ctx()) -> ok | {error, term()}.
report_finished_for_dir(TraverseId, FileCtx) ->
    update_status_doc_and_handle_finished(TraverseId, FileCtx, undefined,
        fun(#qos_status{} = Value) ->
            {ok, Value#qos_status{is_last_batch = true}}
        end
    ).


-spec report_finished_for_file(traverse:id(), file_ctx:ctx(), file_ctx:ctx()) ->
    ok | {error, term()}.
report_finished_for_file(TraverseId, FileCtx, OriginalRootParentCtx) ->
    {ParentFileCtx, FileCtx1} = file_tree:get_parent(FileCtx, undefined),
    FileUuid = file_ctx:get_logical_uuid_const(FileCtx1),
    ?ok_if_not_found(update_status_doc_and_handle_finished(TraverseId, ParentFileCtx, OriginalRootParentCtx,
        fun(#qos_status{files_list = FilesList} = Value) ->
            case lists:member(FileUuid, FilesList) of
                true -> {ok, Value#qos_status{files_list = FilesList -- [FileUuid]}};
                false -> ?ERROR_NOT_FOUND % file was deleted during traverse
            end
        end
    )).


-spec report_file_deleted(file_ctx:ctx(), qos_entry:doc(), file_ctx:ctx() | undefined) -> ok.
report_file_deleted(FileCtx, #document{key = QosEntryId}, OriginalRootParentCtx) ->
    {IsDir, FileCtx1} = file_ctx:is_dir(FileCtx),

    qos_entry:fold_traverses(QosEntryId, fun({TraverseId, _}, _Acc) ->
        case IsDir of
            true ->
                {ParentFileCtx, _} = file_tree:get_original_parent(FileCtx1, OriginalRootParentCtx),
                ok = report_child_dir_traversed(TraverseId, ParentFileCtx, OriginalRootParentCtx),
                ok = handle_traverse_finished_for_dir(TraverseId, FileCtx1, no_link);
            false ->
                ok = report_finished_for_file(TraverseId, FileCtx, OriginalRootParentCtx)
        end
    end, ok).

%%%===================================================================
%%% Functions concerning QoS status check during traverse
%%%===================================================================

%% @private
-spec check_traverses(file_ctx:ctx(), qos_entry:doc()) -> boolean().
check_traverses(FileCtx, #document{key = QosEntryId}) ->
    qos_entry:fold_traverses(QosEntryId, fun({TraverseId, TraverseRootUuid}, Acc) ->
        Acc andalso is_traverse_finished_for_file(TraverseId, FileCtx, TraverseRootUuid)
    end, true).


%% @private
-spec is_traverse_finished_for_file(traverse:id(), file_ctx:ctx(), file_meta:uuid()) -> boolean().
is_traverse_finished_for_file(TraverseId, FileCtx, TraverseRootFileUuid) ->
    try
        is_traverse_finished_for_file_unsafe(TraverseId, FileCtx, TraverseRootFileUuid)
    catch throw:{error, ?MISSING_FILE_META(_)} ->
        % subtree of this file is disconnected from space root, no one should be checking it
        false
    end.


%% @private
-spec is_traverse_finished_for_file_unsafe(traverse:id(), file_ctx:ctx(), file_meta:uuid()) -> boolean().
is_traverse_finished_for_file_unsafe(TraverseId, FileCtx, TraverseRootFileUuid) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    TraverseRootFileCtx = file_ctx:new_by_uuid(TraverseRootFileUuid, SpaceId),
    {TraverseRootFileUuidPath, _} = file_ctx:get_uuid_based_path(TraverseRootFileCtx),
    {IsDir, FileCtx1} = file_ctx:is_dir(FileCtx),
    
    {ok, References} = file_ctx:list_references_const(FileCtx1),
    LogicalUuid = file_ctx:get_logical_uuid_const(FileCtx1),
    ReferencesFileCtx = lists:map(
        fun (FileUuid) when FileUuid == LogicalUuid -> FileCtx1;
            (FileUuid) -> file_ctx:new_by_uuid(FileUuid, SpaceId)
        end,
    References),
    ReferencesInTraverseSubtree = lists:filtermap(fun(InternalFileCtx) ->
        {FileUuidPath, InternalFileCtx1} = file_ctx:get_uuid_based_path(InternalFileCtx),
        case string:prefix(FileUuidPath, TraverseRootFileUuidPath) of
            nomatch -> false;
            _ -> {true, InternalFileCtx1}
        end
    end, ReferencesFileCtx),
    
    ReferencesInTraverseSubtree == [] orelse lists:any(fun(InternalFileCtx) ->
        is_traverse_finished_for_file_in_qos_subtree(TraverseId, InternalFileCtx, TraverseRootFileUuid, IsDir)
    end, ReferencesInTraverseSubtree).


%% @private
-spec is_traverse_finished_for_file_in_qos_subtree(traverse:id(), file_ctx:ctx(), file_meta:uuid(), boolean()) -> 
    boolean().
is_traverse_finished_for_file_in_qos_subtree(TraverseId, FileCtx, TraverseRootFileUuid, _IsDir = true) ->
    InodeUuid = file_ctx:get_referenced_uuid_const(FileCtx),
    % qos_status document exists only for directories, so it does not matter whether referenced or logical uuid is used
    case qos_status_model:get(TraverseId, InodeUuid) of
        {ok, _} ->
            false;
        ?ERROR_NOT_FOUND ->
            has_traverse_link(TraverseId, FileCtx) orelse is_parent_fulfilled(TraverseId, FileCtx, InodeUuid, TraverseRootFileUuid)
    end;
is_traverse_finished_for_file_in_qos_subtree(TraverseId, FileCtx, TraverseRootFileUuid, _IsDir = false) ->
    {ParentFileCtx, FileCtx1} = file_tree:get_parent(FileCtx, undefined),
    {FileName, FileCtx2} = file_ctx:get_aliased_name(FileCtx1, undefined),
    ParentUuid = file_ctx:get_logical_uuid_const(ParentFileCtx),
    LogicalUuid = file_ctx:get_logical_uuid_const(FileCtx),
    case qos_status_model:get(TraverseId, ParentUuid) of
        {ok, #document{
            value = #qos_status{
                previous_batch_last_filename = PreviousBatchLastFilename,
                current_batch_last_filename = LastFilename, files_list = FilesList
            }
        }} ->
            FileName =< PreviousBatchLastFilename orelse
                (not (FileName > LastFilename) and not lists:member(LogicalUuid, FilesList));
        ?ERROR_NOT_FOUND ->
            is_parent_fulfilled(TraverseId, FileCtx2, LogicalUuid, TraverseRootFileUuid)
    end.


%% @private
-spec is_parent_fulfilled(traverse:id(), file_ctx:ctx(), Uuid :: file_meta:uuid(),
    QosRootFileUuid :: file_meta:uuid()) -> boolean().
is_parent_fulfilled(_TraverseId, _FileCtx, Uuid, TraverseRootFileUuid) when Uuid == TraverseRootFileUuid ->
    false;
is_parent_fulfilled(TraverseId, FileCtx, _Uuid, TraverseRootFileUuid) ->
    {ParentFileCtx, _FileCtx1} = file_tree:get_parent(FileCtx, undefined),
    ParentUuid = file_ctx:get_logical_uuid_const(ParentFileCtx),
    has_traverse_link(TraverseId, ParentFileCtx)
        orelse (not has_qos_status_doc(TraverseId, ParentUuid)
        andalso is_parent_fulfilled(TraverseId, ParentFileCtx, ParentUuid, TraverseRootFileUuid)).


%% @private
-spec has_traverse_link(traverse:id(), file_ctx:ctx()) -> boolean().
has_traverse_link(TraverseId, FileCtx) ->
    {Path, _} = file_ctx:get_uuid_based_path(FileCtx),
    case qos_status_links:get_next_local_links(?TRAVERSE_LINKS_KEY(TraverseId), Path, 1) of
        {ok, [Path]} -> true;
        _ -> false
    end.


%% @private
-spec has_qos_status_doc(traverse:id(), file_meta:uuid()) -> boolean().
has_qos_status_doc(TraverseId, Uuid) ->
    case qos_status_model:get(TraverseId, Uuid) of
        {ok, _} -> true;
        ?ERROR_NOT_FOUND -> false
    end.

%%%===================================================================
%%% Internal higher level functions operating on qos_status doc
%%%===================================================================

%% @private
-spec update_status_doc_and_handle_finished(traverse:id(), file_ctx:ctx(), file_ctx:ctx() | undefined, 
    qos_status_model:diff()) -> ok | {error, term()}.
update_status_doc_and_handle_finished(TraverseId, FileCtx, OriginalRootParentCtx, UpdateFun) ->
    Uuid = file_ctx:get_logical_uuid_const(FileCtx),
    case qos_status_model:update(TraverseId, Uuid, UpdateFun) of
        {ok, #document{value = #qos_status{
            child_dirs_count = 0, files_list = [], is_last_batch = true, is_start_dir = true}}
        } ->
            handle_traverse_finished_for_dir(TraverseId, FileCtx, add_link);
        {ok, #document{value = #qos_status{
            child_dirs_count = 0, files_list = [], is_last_batch = true}}
        } ->
            handle_traverse_finished_for_dir(TraverseId, FileCtx, add_link),
            {ParentFileCtx, _} = file_tree:get_original_parent(FileCtx, OriginalRootParentCtx),
            ok = report_child_dir_traversed(TraverseId, ParentFileCtx, OriginalRootParentCtx);
        {ok, _} ->
            ok;
        {error, _} = Error -> Error
    end.


%% @private
-spec report_child_dir_traversed(traverse:id(), file_ctx:ctx(), file_ctx:ctx()) ->
    ok | {error, term()}.
report_child_dir_traversed(TraverseId, FileCtx, OriginalRootParentCtx) ->
    ?ok_if_not_found(update_status_doc_and_handle_finished(TraverseId, FileCtx, OriginalRootParentCtx,
        fun(#qos_status{child_dirs_count = ChildDirs} = Value) ->
            {ok, Value#qos_status{child_dirs_count = ChildDirs - 1}}
        end
    )).


%% @private
-spec handle_traverse_finished_for_dir(traverse:id(), file_ctx:ctx(), link_strategy()) -> ok.
handle_traverse_finished_for_dir(TraverseId, FileCtx, LinkStrategy) ->
    case get_uuid_based_path(FileCtx) of
        not_synced ->
            %% @TODO VFS-10747 When all ancestors are finally synced and QoS is inherited this will result in
            %% ?PENDING status for this directory until whole parent subtree is finished.
            ok;
        {Path, FileCtx1} ->
            Uuid = file_ctx:get_logical_uuid_const(FileCtx1),
            case LinkStrategy of
                add_link ->
                    ok = qos_status_links:add_link(?TRAVERSE_LINKS_KEY(TraverseId), {Path, Uuid});
                no_link ->
                    ok
            end,
            ok = qos_status_links:delete_all_local_links_with_prefix(
                ?TRAVERSE_LINKS_KEY(TraverseId), <<Path/binary, "/">>),
            ok = qos_status_model:delete(TraverseId, Uuid)
    end.


%% @private
-spec get_uuid_based_path(file_ctx:ctx()) -> {file_meta:uuid_based_path(), file_ctx:ctx()} | not_synced.
get_uuid_based_path(FileCtx) ->
    try
        file_ctx:get_uuid_based_path(FileCtx)
    catch throw:{error, ?MISSING_FILE_META(_)} ->
        not_synced
    end.
