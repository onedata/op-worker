%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This is a helper model for tree_traverse mechanism.
%%% It holds information necessary to determine whether whole
%%% subtree of a directory was traversed.
%%% One `tree_traverse_progress` document is created per directory in
%%% given traverse task.
%%%
%%% Traverse lists children in batches and model holds information about
%%% number of remaining (i.e not yet traversed) already listed children,
%%% number of processed children and whether all batches have been listed.
%%% @end
%%%--------------------------------------------------------------------
-module(tree_traverse_progress).
-author("Michal Stanisz").
-author("Jakub Kudzia").

-include("tree_traverse.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").

%% API
-export([
    create/3,
    report_children_to_process/4,
    report_child_processed/2,
    report_last_batch/2,
    delete/2
]).

%% datastore_model callbacks
-export([
    get_ctx/0,
    get_record_struct/1,
    get_record_version/0,
    upgrade_record/2
]).

-type id() :: datastore_model:key().
-type record() :: #tree_traverse_progress{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).
-type status() :: ?SUBTREE_PROCESSED(
    NextSubtreeRoot :: file_meta:uuid(), 
    ProcessingStartTimestamp :: time:millis()
) | ?SUBTREE_NOT_PROCESSED.

-export_type([status/0]).

-define(CTX, #{
    model => ?MODULE
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec create(id(), file_meta:uuid(), file_meta:uuid()) -> ok | {error, term()}.
create(TaskId, FileUuid, ParentUuid) ->
    ?extract_ok(datastore_model:create(?CTX, #document{
        key = gen_id(TaskId, FileUuid),
        value = #tree_traverse_progress{
            next_subtree_root = ParentUuid, 
            processing_start_timestamp = global_clock:timestamp_millis()
        }
    })).


-spec report_children_to_process(id(), file_meta:uuid(), non_neg_integer(), boolean()) -> status().
report_children_to_process(TaskId, FileUuid, ChildrenCount, AllBatchesListed) ->
    update_and_check(TaskId, FileUuid, fun(#tree_traverse_progress{
        to_process = ToProcess,
        all_batches_listed = CurrentAllBatchesListed
    } = TTP) ->
        {ok, TTP#tree_traverse_progress{
            to_process = ToProcess + ChildrenCount,
            all_batches_listed = AllBatchesListed or CurrentAllBatchesListed
        }}
    end).


-spec report_child_processed(id(), file_meta:uuid()) -> status().
report_child_processed(TaskId, FileUuid) ->
    update_and_check(TaskId, FileUuid, fun(#tree_traverse_progress{processed = Processed} = TTP) ->
        {ok, TTP#tree_traverse_progress{processed = Processed + 1}}
    end).


-spec report_last_batch(id(), file_meta:uuid()) ->
    status() | {error, term()}.
report_last_batch(TaskId, FileUuid) ->
    update_and_check(TaskId, FileUuid, fun(TTP) ->
        {ok, TTP#tree_traverse_progress{all_batches_listed = true}}
    end).


-spec delete(id(), file_meta:uuid()) -> ok | {error, term()}.
delete(TaskId, FileUuid) ->
    ?extract_ok(datastore_model:delete(?CTX, gen_id(TaskId, FileUuid))).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec update(tree_traverse:id(), file_meta:uuid(), diff()) -> {ok, doc()} | {error, term()}.
update(TaskId, Uuid, UpdateFun) ->
    datastore_model:update(?CTX, gen_id(TaskId, Uuid), UpdateFun).

%% @private
-spec update_and_check(tree_traverse:id(), file_meta:uuid(), diff()) -> status() | {error, term()}.
update_and_check(TaskId, Uuid, UpdateFun) ->
    case update(TaskId, Uuid, UpdateFun) of
        {ok, #document{value = #tree_traverse_progress{
            to_process = ToProcess,
            processed = ToProcess,
            all_batches_listed = true
        } = Record}} ->
            #tree_traverse_progress{
                next_subtree_root = NextSubtreeRoot, 
                processing_start_timestamp = StartTimestamp
            } = Record,
            case NextSubtreeRoot of
                undefined ->
                    case file_meta:get_parent_uuid(Uuid) of
                        {ok, ParentUuid} -> ?SUBTREE_PROCESSED(ParentUuid, StartTimestamp);
                        {error, _} = Error -> Error
                    end;
                _ ->
                    ?SUBTREE_PROCESSED(NextSubtreeRoot, StartTimestamp)
            end;
        {ok, _} ->
            ?SUBTREE_NOT_PROCESSED;
        Error ->
            Error
    end.

%% @private
-spec gen_id(id(), file_meta:uuid()) -> id().
gen_id(TaskId, FileUuid) ->
    datastore_key:adjacent_from_digest([TaskId], FileUuid).


%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.


-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    3.


-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {to_process, integer},
        {processed, integer},
        {all_batches_listed, boolean}
    ]};
get_record_struct(2) ->
    {record, [
        {to_process, integer},
        {processed, integer},
        {all_batches_listed, boolean},
        {next_subtree_root, string} % new field
    ]};
get_record_struct(3) ->
    {record, [
        {to_process, integer},
        {processed, integer},
        {all_batches_listed, boolean},
        {next_subtree_root, string},
        {processing_start_timestamp, integer} % new field
    ]}.


-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, TreeTraverseProgress) ->
    {tree_traverse_progress,
        ToProcess,
        Processed,
        AllBatchesListed
    } = TreeTraverseProgress,
    
    {2, {tree_traverse_progress, 
        ToProcess,
        Processed,
        AllBatchesListed,
        undefined % new field `next_subtree_root`
    }};
upgrade_record(2, TreeTraverseProgress) ->
    {tree_traverse_progress,
        ToProcess,
        Processed,
        AllBatchesListed,
        NextSubtreeRoot
    } = TreeTraverseProgress,
    
    {3, #tree_traverse_progress{
        to_process = ToProcess,
        processed = Processed,
        all_batches_listed = AllBatchesListed,
        next_subtree_root = NextSubtreeRoot,
        processing_start_timestamp = global_clock:timestamp_millis()
    }}.
