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
%%%       this file/all its files in subtree (qos_uptree_status)
%%%     - file is not being reconciled/no file is currently being reconciled in 
%%%       subtree of this directory (qos_downtree_status)
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
%%% It may be impossible to generate such a path when not all docs are synced yet. In such a case
%%% traverse is added to entry traverse list and for each such traverse uuid based path is generated
%%% during status check on best effort manner.
%%%
%%% When file has many references (i.e some hardlinks were created), status links consisting of 
%%% uuid_based_path are added for each file reference. 
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
-export([report_traverse_start/2, report_traverse_finished/2,
    report_next_traverse_batch/5, report_traverse_finished_for_dir/2,
    report_traverse_finished_for_file/3]).
-export([report_reconciliation_started/3, report_reconciliation_finished/2]).
-export([report_file_transfer_failure/2]).
-export([report_file_deleted/2, report_file_deleted/3, report_entry_deleted/2]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0]).

-type path() :: file_meta:path().
-type summary() :: ?PENDING_QOS_STATUS | ?FULFILLED_QOS_STATUS | ?IMPOSSIBLE_QOS_STATUS.

-export_type([path/0, summary/0]).

-define(CTX, #{
    model => ?MODULE,
    sync_enabled => true,
    remote_driver => datastore_remote_driver,
    mutator => oneprovider:get_id_or_undefined(),
    local_links_tree_id => oneprovider:get_id_or_undefined()
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec check(file_ctx:ctx(), qos_entry:id()) -> summary().
check(FileCtx, QosEntryId) ->
    {ok, QosEntryDoc} = qos_entry:get(QosEntryId),
    
    case qos_entry:is_possible(QosEntryDoc) of
        false -> ?IMPOSSIBLE_QOS_STATUS;
        true -> case check_possible_entry_status(FileCtx, QosEntryDoc, QosEntryId) of
            true -> ?FULFILLED_QOS_STATUS;
            false -> ?PENDING_QOS_STATUS
        end
    end.


-spec aggregate([qos_status:summary()]) -> qos_status:summary().
aggregate(Statuses) ->
    lists:foldl(
        fun (_, ?IMPOSSIBLE_QOS_STATUS) -> ?IMPOSSIBLE_QOS_STATUS;
            (?IMPOSSIBLE_QOS_STATUS, _) -> ?IMPOSSIBLE_QOS_STATUS;
            (_, ?PENDING_QOS_STATUS) -> ?PENDING_QOS_STATUS;
            (Status, _Acc) -> Status
        end, ?FULFILLED_QOS_STATUS, Statuses).


-spec report_traverse_start(traverse:id(), file_ctx:ctx()) -> {ok, file_ctx:ctx()}.
report_traverse_start(TraverseId, FileCtx) ->
    qos_uptree_status:report_started(TraverseId, FileCtx).


-spec report_traverse_finished(traverse:id(), file_ctx:ctx()) -> ok | {error, term()}.
report_traverse_finished(TraverseId, FileCtx) ->
    qos_uptree_status:report_finished(TraverseId, FileCtx).


-spec report_next_traverse_batch(traverse:id(), file_ctx:ctx(),
    ChildrenDirs :: [file_meta:uuid()], ChildrenFiles :: [file_meta:uuid()], 
    BatchLastFilename :: file_meta:name()) -> ok.
report_next_traverse_batch(TraverseId, FileCtx, ChildrenDirs, ChildrenFiles, BatchLastFilename) ->
    qos_uptree_status:report_next_batch(TraverseId, FileCtx, ChildrenDirs, ChildrenFiles, BatchLastFilename).


-spec report_traverse_finished_for_dir(traverse:id(), file_ctx:ctx()) -> ok | {error, term()}.
report_traverse_finished_for_dir(TraverseId, FileCtx) ->
    qos_uptree_status:report_finished_for_dir(TraverseId, FileCtx).


-spec report_traverse_finished_for_file(traverse:id(), file_ctx:ctx(), file_ctx:ctx()) ->
    ok | {error, term()}.
report_traverse_finished_for_file(TraverseId, FileCtx, OriginalRootParentCtx) ->
    qos_uptree_status:report_finished_for_file(TraverseId, FileCtx, OriginalRootParentCtx).


-spec report_reconciliation_started(traverse:id(), file_ctx:ctx(), [qos_entry:id()]) -> 
    ok | {error, term()}.
report_reconciliation_started(TraverseId, FileCtx, QosEntries) ->
    qos_downtree_status:report_started(TraverseId, FileCtx, QosEntries).


-spec report_reconciliation_finished(traverse:id(), file_ctx:ctx()) -> ok | {error, term()}.
report_reconciliation_finished(TraverseId, FileCtx) ->
    qos_downtree_status:report_finished(TraverseId, FileCtx).


-spec report_file_transfer_failure(file_ctx:ctx(), [qos_entry:id()]) ->
    ok | {error, term()}.
report_file_transfer_failure(FileCtx, QosEntries) ->
    qos_downtree_status:report_file_transfer_failure(FileCtx, QosEntries).


-spec report_file_deleted(file_ctx:ctx(), qos_entry:id()) -> ok.
report_file_deleted(FileCtx, QosEntryId) ->
    report_file_deleted(FileCtx, QosEntryId, undefined).

-spec report_file_deleted(file_ctx:ctx(), qos_entry:id(), file_ctx:ctx() | undefined) -> ok.
report_file_deleted(FileCtx, QosEntryId, OriginalRootParentCtx) ->
    case qos_entry:get(QosEntryId) of
        {ok, QosEntryDoc} ->
            qos_uptree_status:report_file_deleted(FileCtx, QosEntryDoc, OriginalRootParentCtx),
            qos_downtree_status:report_file_deleted(FileCtx, QosEntryDoc, OriginalRootParentCtx);
        {error, not_found} ->
            ok
    end.


-spec report_entry_deleted(od_space:id(), qos_entry:id()) -> ok.
report_entry_deleted(SpaceId, QosEntryId) ->
    qos_downtree_status:report_entry_deleted(SpaceId, QosEntryId).
    

%%%===================================================================
%%% Internal functions concerning QoS status check
%%%===================================================================

%% @private
-spec check_possible_entry_status(file_ctx:ctx(), qos_entry:doc(), qos_entry:id()) -> boolean().
check_possible_entry_status(FileCtx, QosEntryDoc, QosEntryId) ->
    {FileDoc, FileCtx1} = file_ctx:get_file_doc_including_deleted(FileCtx),
    case file_meta_hardlinks:inspect_references(FileDoc) of    
        no_references_left -> throw(?ERROR_NOT_FOUND);
        has_at_least_one_reference -> ok
    end,
    (not file_qos:is_effective_qos_of_file(FileDoc, QosEntryId)) orelse
        qos_downtree_status:check(FileCtx1, QosEntryDoc) andalso
            qos_uptree_status:check(FileCtx1, QosEntryDoc).

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.


-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    qos_status_model:get_record_version().


-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(Version) ->
    qos_status_model:get_record_struct(Version).
