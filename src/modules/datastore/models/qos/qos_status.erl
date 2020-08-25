%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for calculating QoS entry fulfillment status.
%%%
%%% QoS entry for given file/directory is fulfilled when:
%%%     - there is no information that qos_entry cannot be satisfied (see qos_entry.erl)
%%%     - traverse created as result of adding this QoS entry has already synchronized 
%%%       this file/all its files in subtree
%%%     - file is not being reconciled/no file is currently being reconciled in 
%%%       subtree of this directory
%%%
%%% In order to be able to check that given file/directory has been traversed additional 
%%% document(qos_status) is created for a directory when it was encountered during traverse. 
%%% This document contains information of traverse state in a directory subtree.
%%% 
%%% QoS traverse lists files in an ordered fashion (based on binary comparison) and splits them 
%%% into batches. After each batch have been evaluated it is reported to this module so qos_status 
%%% document could be appropriately updated. Next batch is started when previous one is finished.
%%% 
%%% qos_status document contains previous and current batch last filenames and list of not 
%%% finished files in current batch. It also has number of children directories for which 
%%% traverse have started. 
%%%
%%% When traverse of subtree of a directory is finished (i.e. all children files have been 
%%% synchronized and all children directories have traverse_links), traverse_link is created 
%%% for this directory and qos_status document is deleted. A traverse_link for a directory indicates 
%%% that traverse of subtree of this directory is finished. When traverse_link is created, links 
%%% for all children are deleted, so when traverse is finished only one traverse_link is left 
%%% for QoS entry root directory. This traverse_link is then deleted.
%%% 
%%% To check whether given file have been traversed(synchronized in a traverse): 
%%%     - qos_status document for parent exists: 
%%%         * given file's name is lower than last filename of previous batch -> traversed 
%%%         * given file's name is higher than last filename of current batch -> not traversed 
%%%         * file is on list of files not finished in current batch -> not traversed
%%%     - there is no qos_status document for parent directory: 
%%%         * any ancestor has traverse_link -> traversed
%%%         * otherwise -> not traversed
%%%
%%% To check whether given directory have been traversed: 
%%%     - qos_status document for directory exists -> not traversed
%%%     - there is no qos_status document for directory: 
%%%         * any ancestor has traverse_link -> traversed
%%%         * otherwise -> not traversed 
%%% 
%%% 
%%% In order to be able to check whether file is being reconciled, when file change was reported 
%%% reconcile_link is created. This link is file uuid_based_path (similar to canonical, but path 
%%% elements are uuids instead of filenames/dirnames). This link is deleted after reconcile job 
%%% is done. 
%%% 
%%% To check if there is any reconcile job in subtree of a directory simply check if there is any 
%%% reconcile_link with its prefix being this directory uuid based path.
%%% 
%%% @end
%%%-------------------------------------------------------------------
-module(qos_status).
-author("Michal Stanisz").

-include("modules/datastore/qos.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([check/2, aggregate/1]).
-export([report_traverse_start/2, report_traverse_finished/3,
    report_next_traverse_batch/6, report_traverse_finished_for_dir/3,
    report_traverse_finished_for_file/2]).
-export([report_reconciliation_started/3, report_reconciliation_finished/3]).
-export([report_file_transfer_failure/2]).
-export([report_file_deleted/2, report_entry_deleted/2]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0]).

-type path() :: file_meta:path().
-type id() :: datastore_doc:key().
-type record() :: #qos_status{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).
% Describes whether link should be added upon successful function execution
-type link_strategy() :: add_link | no_link.

-type summary() :: ?PENDING | ?FULFILLED | ?IMPOSSIBLE.
-export_type([summary/0]).

-define(CTX, #{
    model => ?MODULE,
    sync_enabled => true,
    remote_driver => datastore_remote_driver,
    mutator => oneprovider:get_id_or_undefined(),
    local_links_tree_id => oneprovider:get_id_or_undefined()
}).

-define(RECONCILE_LINK_NAME(Path, TraverseId), <<Path/binary, "###", TraverseId/binary>>).
-define(FAILED_TRANSFER_LINK_NAME(Path), <<Path/binary, "###failed_transfer">>).

-define(RECONCILE_LINKS_KEY(QosEntryId), <<"qos_status_reconcile", QosEntryId/binary>>).
-define(TRAVERSE_LINKS_KEY(TraverseId), <<"qos_status_traverse", TraverseId/binary>>).

-define(LIST_LINKS_BATCH_SIZE, 20).

%%%===================================================================
%%% API
%%%===================================================================

-spec check(file_ctx:ctx(), qos_entry:id()) -> summary().
check(FileCtx, QosEntryId) ->
    {ok, QosDoc} = qos_entry:get(QosEntryId),
    
    case qos_entry:is_possible(QosDoc) of
        false -> ?IMPOSSIBLE;
        true -> case check_possible_entry_status(FileCtx, QosDoc, QosEntryId) of
            true -> ?FULFILLED;
            false -> ?PENDING
        end
    end.


-spec aggregate([qos_status:summary()]) -> qos_status:summary().
aggregate(Statuses) ->
    lists:foldl(
        fun (_, ?IMPOSSIBLE) -> ?IMPOSSIBLE;
            (?IMPOSSIBLE, _) -> ?IMPOSSIBLE;
            (_, ?PENDING) -> ?PENDING;
            (Status, _Acc) -> Status
        end, ?FULFILLED, Statuses).


-spec report_traverse_start(traverse:id(), file_ctx:ctx()) -> {ok, file_ctx:ctx()}.
report_traverse_start(TraverseId, FileCtx) ->
    {ok, case file_ctx:is_dir(FileCtx) of
        {true, FileCtx1} ->
            {ok, _} = create(file_ctx:get_space_id_const(FileCtx), TraverseId, 
                file_ctx:get_uuid_const(FileCtx), start_dir),
            FileCtx1;
        {false, FileCtx1} -> 
            % No need to create qos_status doc for traverse of single file. Because there is no 
            % parent doc and traverse_link, status will be false until traverse finish.
            FileCtx1
    end}.


-spec report_traverse_finished(od_space:id(), traverse:id(), file_meta:uuid()) -> 
    ok | {error, term()}.
report_traverse_finished(SpaceId, TraverseId, Uuid) ->
    {Path, _} = file_ctx:get_uuid_based_path(file_ctx:new_by_guid(file_id:pack_guid(Uuid, SpaceId))),
    delete_synced_link(SpaceId, ?TRAVERSE_LINKS_KEY(TraverseId), oneprovider:get_id(), Path).


-spec report_next_traverse_batch(od_space:id(), traverse:id(), file_meta:uuid(),
    ChildrenDirs :: [file_meta:uuid()], ChildrenFiles :: [file_meta:uuid()], 
    BatchLastFilename :: file_meta:name()) -> ok.
report_next_traverse_batch(SpaceId, TraverseId, Uuid, ChildrenDirs, ChildrenFiles, BatchLastFilename) ->
    {ok, _} = update(TraverseId, Uuid,
        fun(#qos_status{
            child_dirs_count = ChildDirsCount, 
            current_batch_last_filename = LN
        } = Value) ->
            {ok, Value#qos_status{
                files_list = ChildrenFiles,
                previous_batch_last_filename = LN,
                current_batch_last_filename = BatchLastFilename,
                child_dirs_count = ChildDirsCount + length(ChildrenDirs)}
            }
        end),
    lists:foreach(fun(ChildDirUuid) ->
        create(SpaceId, TraverseId, ChildDirUuid, child_dir)
    end, ChildrenDirs).


-spec report_traverse_finished_for_dir(od_space:id(), traverse:id(), file_meta:uuid()) -> 
    ok | {error, term()}.
report_traverse_finished_for_dir(SpaceId, TraverseId, DirUuid) ->
    FileCtx = file_ctx:new_by_guid(file_id:pack_guid(DirUuid, SpaceId)),
    update_status_doc_and_handle_finished(TraverseId, FileCtx,
        fun(#qos_status{} = Value) ->
            {ok, Value#qos_status{is_last_batch = true}}
        end
    ).


-spec report_traverse_finished_for_file(traverse:id(), file_ctx:ctx()) -> 
    ok | {error, term()}.
report_traverse_finished_for_file(TraverseId, FileCtx) ->
    {ParentFileCtx, FileCtx1} = file_ctx:get_parent(FileCtx, undefined),
    FileUuid = file_ctx:get_uuid_const(FileCtx1),
    ?ok_if_not_found(update_status_doc_and_handle_finished(TraverseId, ParentFileCtx,
        fun(#qos_status{files_list = FilesList} = Value) ->
            case lists:member(FileUuid, FilesList) of
                true -> {ok, Value#qos_status{files_list = FilesList -- [FileUuid]}};
                false -> ?ERROR_NOT_FOUND % file was deleted during traverse
            end
        end
    )).


-spec report_reconciliation_started(traverse:id(), file_ctx:ctx(), [qos_entry:id()]) -> 
    ok | {error, term()}.
report_reconciliation_started(TraverseId, FileCtx, QosEntries) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    {UuidBasedPath, _} = file_ctx:get_uuid_based_path(FileCtx),
    Link = {?RECONCILE_LINK_NAME(UuidBasedPath, TraverseId), TraverseId},
    ProviderId = oneprovider:get_id(),
    lists:foreach(fun(QosEntryId) ->
        {ok, _} = add_synced_link(
            SpaceId, ?RECONCILE_LINKS_KEY(QosEntryId), ProviderId, Link),
        ok = delete_synced_link(SpaceId, ?RECONCILE_LINKS_KEY(QosEntryId), ProviderId,
            ?FAILED_TRANSFER_LINK_NAME(UuidBasedPath))
    end, QosEntries).


-spec report_reconciliation_finished(datastore_doc:scope(), traverse:id(), file_meta:uuid()) ->
    ok | {error, term()}.
report_reconciliation_finished(SpaceId, TraverseId, FileUuid) ->
    FileGuid = file_id:pack_guid(FileUuid, SpaceId),
    QosEntries = case file_qos:get_effective(FileUuid) of
        undefined -> [];
        {error, {file_meta_missing, FileUuid}} -> [];
        {error, _} = Error ->
            ?warning("Error after file ~p have been reconciled: ~p", [FileUuid, Error]),
            [];
        {ok, EffectiveFileQos} ->
            file_qos:get_qos_entries(EffectiveFileQos)
    end,
    {UuidBasedPath, _} = file_ctx:get_uuid_based_path(file_ctx:new_by_guid(FileGuid)),
    ProviderId = oneprovider:get_id(),
    lists:foreach(fun(QosEntryId) ->
        ok = delete_synced_link(SpaceId, ?RECONCILE_LINKS_KEY(QosEntryId), ProviderId,
            ?RECONCILE_LINK_NAME(UuidBasedPath, TraverseId))
    end, QosEntries).


-spec report_file_transfer_failure(file_ctx:ctx(), [qos_entry:id()]) ->
    ok | {error, term()}.
report_file_transfer_failure(FileCtx, QosEntries) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    ok = ?extract_ok(?ok_if_exists(
        qos_entry:add_to_failed_files_list(SpaceId, file_ctx:get_uuid_const(FileCtx)))),
    {UuidBasedPath, _} = file_ctx:get_uuid_based_path(FileCtx),
    Link = {?FAILED_TRANSFER_LINK_NAME(UuidBasedPath), <<"failed_transfer">>},
    ProviderId = oneprovider:get_id(),
    lists:foreach(fun(QosEntryId) ->
        ok = ?extract_ok(?ok_if_exists(
            add_synced_link(SpaceId, ?RECONCILE_LINKS_KEY(QosEntryId), ProviderId, Link)))
    end, QosEntries).


-spec report_file_deleted(file_ctx:ctx(), qos_entry:id()) -> ok.
report_file_deleted(FileCtx, QosEntryId) ->
    {ok, QosDoc} = qos_entry:get(QosEntryId),
    {ok, TraverseReqs} = qos_entry:get_traverse_reqs(QosDoc),
    {LocalTraverseIds, _} = qos_traverse_req:split_local_and_remote(TraverseReqs),
    {IsDir, FileCtx1} = file_ctx:is_dir(FileCtx),
    
    lists:foreach(fun(TraverseId) ->
        case IsDir of
            true ->
                {ParentFileCtx, _} = file_ctx:get_parent(FileCtx1, undefined),
                ok = report_child_dir_traversed(TraverseId, ParentFileCtx),
                ok = handle_traverse_finished_for_dir(TraverseId, FileCtx1, no_link);
            false ->
                ok = report_traverse_finished_for_file(TraverseId, FileCtx)
        end
    end, LocalTraverseIds),
    
    {ok, SpaceId} = qos_entry:get_space_id(QosDoc),
    {UuidBasedPath, _} = file_ctx:get_uuid_based_path(FileCtx1),
    % delete all reconcile links for given file
    delete_all_links_with_prefix(
        SpaceId, ?RECONCILE_LINKS_KEY(QosEntryId), ?RECONCILE_LINK_NAME(UuidBasedPath, <<"">>)).


-spec report_entry_deleted(od_space:id(), qos_entry:id()) -> ok.
report_entry_deleted(SpaceId, QosEntryId) ->
    % delete all reconcile links for given entry
    delete_all_links_with_prefix(SpaceId, ?RECONCILE_LINKS_KEY(QosEntryId), <<"">>).
    

%%%===================================================================
%%% Internal functions concerning QoS status check during traverse
%%%===================================================================

%% @private
-spec check_possible_entry_status(file_ctx:ctx(), qos_entry:doc(), qos_entry:id()) -> boolean().
check_possible_entry_status(FileCtx, QosDoc, QosEntryId) ->
    {FileDoc, FileCtx1} = file_ctx:get_file_doc(FileCtx),
    (not file_qos:is_effective_qos_of_file(FileDoc, QosEntryId)) orelse
        is_file_reconciled(FileCtx1, QosDoc) andalso
            are_traverses_finished_for_file(FileCtx1, QosDoc).


%% @private
-spec are_traverses_finished_for_file(file_ctx:ctx(), qos_entry:doc()) -> boolean().
are_traverses_finished_for_file(FileCtx, #document{key = QosEntryId} = QosDoc) ->
    {ok, AllTraverseReqs} = qos_entry:get_traverse_reqs(QosDoc),
    AllTraverseIds = qos_traverse_req:get_traverse_ids(AllTraverseReqs),
    {ok, QosRootFileUuid} = qos_entry:get_file_uuid(QosDoc),
    
    {IsDir, FileCtx1} = file_ctx:is_dir(FileCtx),
    NotFinishedTraverseIds = lists:filter(fun(TraverseId) ->
        not is_traverse_finished_for_file(TraverseId, FileCtx1, QosRootFileUuid, IsDir)
    end, AllTraverseIds),
    
    % fetch traverse list again to secure against possible race
    % between start of status check and traverse finish
    {ok, QosDoc1} = qos_entry:get(QosEntryId),
    {ok, TraverseReqsAfter} = qos_entry:get_traverse_reqs(QosDoc1),
    TraverseIdsAfter = qos_traverse_req:get_traverse_ids(TraverseReqsAfter),
    [] == lists_utils:intersect(TraverseIdsAfter, NotFinishedTraverseIds).


%% @private
-spec is_traverse_finished_for_file(traverse:id(), file_ctx:ctx(), 
    QosRootFileUuid :: file_meta:uuid(), IsDir :: boolean()) -> boolean().
is_traverse_finished_for_file(TraverseId, FileCtx, QosRootFileUuid, _IsDir = true) ->
    Uuid = file_ctx:get_uuid_const(FileCtx),
    case get(TraverseId, Uuid) of
        {ok, _} -> false;
        ?ERROR_NOT_FOUND -> has_traverse_link(TraverseId, FileCtx)
            orelse is_parent_fulfilled(TraverseId, FileCtx, Uuid, QosRootFileUuid);
        {error, _} = Error -> Error
    end;
is_traverse_finished_for_file(TraverseId, FileCtx, QosRootFileUuid, _IsDir = false) ->
    {FileName, _} = file_ctx:get_aliased_name(FileCtx, undefined),
    {ParentFileCtx, FileCtx1} = file_ctx:get_parent(FileCtx, undefined),
    Uuid = file_ctx:get_uuid_const(FileCtx1),
    ParentUuid = file_ctx:get_uuid_const(ParentFileCtx),
    case get(TraverseId, ParentUuid) of
        {ok, #document{
            value = #qos_status{
                previous_batch_last_filename = PreviousBatchLastFilename, 
                current_batch_last_filename = LastFilename, files_list = FilesList
            }
        }} ->
            FileName =< PreviousBatchLastFilename orelse 
                (not (FileName > LastFilename) and not lists:member(Uuid, FilesList));
        ?ERROR_NOT_FOUND -> 
            is_parent_fulfilled(TraverseId, FileCtx1, Uuid, QosRootFileUuid);
        {error, _} = Error -> 
            Error
    end.


%% @private
-spec is_parent_fulfilled(traverse:id(), file_ctx:ctx(), Uuid :: file_meta:uuid(), 
    QosRootFileUuid :: file_meta:uuid()) -> boolean().
is_parent_fulfilled(_TraverseId, _FileCtx, Uuid, QosRootFileUuid) when Uuid == QosRootFileUuid ->
    false;
is_parent_fulfilled(TraverseId, FileCtx, _Uuid, QosRootFileUuid) ->
    {ParentFileCtx, _FileCtx1} = file_ctx:get_parent(FileCtx, undefined),
    ParentUuid = file_ctx:get_uuid_const(ParentFileCtx),
    has_traverse_link(TraverseId, ParentFileCtx)
        orelse (not has_qos_status_doc(TraverseId, ParentUuid)
            andalso  is_parent_fulfilled(TraverseId, ParentFileCtx, ParentUuid, QosRootFileUuid)).


%%%===================================================================
%%% Internal functions regarding check of reconcile jobs
%%%===================================================================

%% @private
-spec is_file_reconciled(file_ctx:ctx(), qos_entry:doc()) -> boolean().
is_file_reconciled(FileCtx, #document{key = QosEntryId}) ->
    {UuidBasedPath, _} = file_ctx:get_uuid_based_path(FileCtx),
    case get_next_status_links(?RECONCILE_LINKS_KEY(QosEntryId), UuidBasedPath, 1) of
        {ok, []} -> true;
        {ok, [{Path, _}]} -> not str_utils:binary_starts_with(Path, UuidBasedPath)
    end.


%%%===================================================================
%%% Internal higher level functions operating on qos_status doc
%%%===================================================================

%% @private
-spec update_status_doc_and_handle_finished(traverse:id(), file_ctx:ctx(), diff()) ->
    ok | {error, term()}.
update_status_doc_and_handle_finished(TraverseId, FileCtx, UpdateFun) ->
    Uuid = file_ctx:get_uuid_const(FileCtx),
    case update(TraverseId, Uuid, UpdateFun) of
        {ok, #document{value = #qos_status{
            child_dirs_count = 0, files_list = [], is_last_batch = true, is_start_dir = true}}
        } -> 
            handle_traverse_finished_for_dir(TraverseId, FileCtx, add_link);
        {ok, #document{value = #qos_status{
            child_dirs_count = 0, files_list = [], is_last_batch = true}}
        } ->
            handle_traverse_finished_for_dir(TraverseId, FileCtx, add_link),
            {ParentFileCtx, _} = file_ctx:get_parent(FileCtx, undefined),
            ok = report_child_dir_traversed(TraverseId, ParentFileCtx);
        {ok, _} -> ok;
        {error, _} = Error -> Error
    end.


%% @private
-spec report_child_dir_traversed(traverse:id(), file_ctx:ctx()) ->
    ok | {error, term()}.
report_child_dir_traversed(TraverseId, FileCtx) ->
    ?ok_if_not_found(update_status_doc_and_handle_finished(TraverseId, FileCtx,
        fun(#qos_status{child_dirs_count = ChildDirs} = Value) ->
            {ok, Value#qos_status{child_dirs_count = ChildDirs - 1}}
        end
    )).


%% @private
-spec handle_traverse_finished_for_dir(traverse:id(), file_ctx:ctx(), link_strategy()) -> ok.
handle_traverse_finished_for_dir(TraverseId, FileCtx, LinkStrategy) ->
    {Path, FileCtx1} = file_ctx:get_uuid_based_path(FileCtx),
    Uuid = file_ctx:get_uuid_const(FileCtx1),
    SpaceId = file_ctx:get_space_id_const(FileCtx1),
    case LinkStrategy of
        add_link ->
            {ok, _} = add_synced_link(
                SpaceId, ?TRAVERSE_LINKS_KEY(TraverseId), 
                oneprovider:get_id(), {Path, Uuid});
        no_link -> ok
    end,
    ok = delete_all_links_with_prefix(
        SpaceId, ?TRAVERSE_LINKS_KEY(TraverseId), <<Path/binary, "/">>),
    ok = delete(TraverseId, Uuid).


%%%===================================================================
%%% Internal functions using datastore_model API
%%%===================================================================

%% @private
-spec create(od_space:id(), traverse:id(), file_meta:uuid(), DirType :: start_dir | child_dir) -> 
    {ok, doc()}.
create(SpaceId, TraverseId, DirUuid, DirType) ->
    Id = generate_status_doc_id(TraverseId, DirUuid),
    datastore_model:create(?CTX, #document{key = Id, scope = SpaceId,
        value = #qos_status{is_start_dir = DirType == start_dir}
    }).


%% @private
-spec update(traverse:id(), file_meta:uuid(), diff()) -> {ok, doc()} | {error, term()}.
update(TraverseId, Uuid, Diff) ->
    Id = generate_status_doc_id(TraverseId, Uuid),
    datastore_model:update(?CTX, Id, Diff).


%% @private
-spec get(traverse:id(), file_meta:uuid()) -> {ok, doc()} | {error, term()}.
get(TraverseId, Uuid) ->
    Id = generate_status_doc_id(TraverseId, Uuid),
    datastore_model:get(?CTX, Id).


%% @private
-spec delete(traverse:id(), file_meta:uuid()) -> ok | {error, term()}.
delete(TraverseId, Uuid)->
    Id = generate_status_doc_id(TraverseId, Uuid),
    ?ok_if_not_found(datastore_model:delete(?CTX, Id)).


%% @private
-spec add_synced_link(datastore_doc:scope(), datastore:key(), datastore:tree_id(),
    {datastore:link_name(), datastore:link_target()}) -> {ok, datastore:link()} | {error, term()}.
add_synced_link(SpaceId, Key, TreeId, Link) ->
    datastore_model:add_links(?CTX#{scope => SpaceId}, Key, TreeId, Link).


%% @private
-spec delete_synced_link(datastore_doc:scope(), datastore:key(), datastore:tree_id(),
    datastore:link_name() | {datastore:link_name(), datastore:link_rev()}) ->
    ok | {error, term()}.
delete_synced_link(SpaceId, Key, TreeId, Link) ->
    ok = datastore_model:delete_links(?CTX#{scope => SpaceId}, Key, TreeId, Link).


%% @private
-spec fold_links(id(), datastore_model:tree_ids(), datastore:fold_fun(datastore:link()),
    datastore:fold_acc(), datastore:fold_opts()) -> {ok, datastore:fold_acc()} |
    {{ok, datastore:fold_acc()}, datastore_links_iter:token()} | {error, term()}.
fold_links(Key, TreeIds, Fun, Acc, Opts) ->
    datastore_model:fold_links(?CTX, Key, TreeIds, Fun, Acc, Opts).

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.


-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    1.


-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {previous_batch_last_filename, binary},
        {current_batch_last_filename, binary},
        {files_list, [string]},
        {child_dirs_count, integer},
        {is_last_batch, boolean},
        {is_start_dir, boolean}
    ]}.

%%%===================================================================
%%% Internal functions operating on links and datastore document
%%%===================================================================

%% @private
-spec generate_status_doc_id(traverse:id(), file_meta:uuid()) -> id().
generate_status_doc_id(TraverseId, DirUuid) ->
    datastore_key:adjacent_from_digest([DirUuid, TraverseId], DirUuid).


%% @private
-spec has_traverse_link(traverse:id(), file_ctx:ctx()) -> boolean().
has_traverse_link(TraverseId, FileCtx) ->
    {Path, _} = file_ctx:get_uuid_based_path(FileCtx),
    case get_next_status_links(?TRAVERSE_LINKS_KEY(TraverseId), Path, 1) of
        {ok, [{Path, _}]} -> true;
        _ -> false
    end.


%% @private
-spec has_qos_status_doc(traverse:id(), file_meta:uuid()) -> boolean().
has_qos_status_doc(TraverseId, Uuid) ->
    case get(TraverseId, Uuid) of
        {ok, _} -> true;
        ?ERROR_NOT_FOUND -> false
    end.


%% @private
-spec delete_all_links_with_prefix(od_space:id(), datastore:key(), path()) -> ok.
delete_all_links_with_prefix(SpaceId, Key, Prefix) ->
    case get_next_status_links(Key, Prefix, ?LIST_LINKS_BATCH_SIZE) of
        {ok, []} -> ok;
        {ok, Links} ->
            case delete_links_with_prefix_in_batch(SpaceId, Key, Prefix, Links) of
                finished -> ok;
                false -> delete_all_links_with_prefix(SpaceId, Key, Prefix)
            end
    end.


%% @private
-spec delete_links_with_prefix_in_batch(od_space:id(), datastore:key(), path(), 
    [{datastore:link_name(), oneprovider:id()}]) -> finished | false.
delete_links_with_prefix_in_batch(SpaceId, Key, Prefix, Links) ->
    lists:foldl(fun 
        (_, finished) -> finished;
        ({LinkName, TreeId}, _) ->
            case str_utils:binary_starts_with(LinkName, Prefix) of
                true ->
                    ok = delete_synced_link(SpaceId, Key, TreeId, LinkName),
                    false;
                false -> 
                    finished
            end
        end, false, Links).


%% @private
-spec get_next_status_links(datastore:key(), path(), non_neg_integer()) -> 
    {ok, [{path(), datastore_links:tree_id()}]}.
get_next_status_links(Key, Path, BatchSize) ->
    fold_links(Key, all,
        fun(#link{name = Name, tree_id = TreeId}, Acc) -> {ok, [{Name, TreeId} | Acc]} end,
        [],
        #{prev_link_name => Path, size => BatchSize}
    ).
