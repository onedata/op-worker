%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% TODO UPDATE
%%% This model holds information necessary to tell whether whole subtree
%%% of a directory was traversed so this directory can be cleaned up.
%%% One `tree_traverse_status` document is created per directory.
%%%
%%% Traverse lists children in batches and model holds information about
%%% number of remaining (i.e not yet traversed) already listed children
%%% and whether all batches of have been listed.
%%% Based on this information it can be determined whether subtree of a
%%% directory was traversed (no children left and all batches have been
%%% evaluated).
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
    create/2,
    report_children_to_process/4,
    report_child_processed/2,
    report_last_batch/2,
    delete/2
]).

%% datastore_model callbacks
-export([
    get_ctx/0,
    get_record_struct/1,
    get_record_version/0
]).

-type id() :: datastore_model:key().
-type record() :: #tree_traverse_progress{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).
-type status() :: ?SUBTREE_PROCESSED | ?SUBTREE_NOT_PROCESSED.

-export_type([status/0]).

-define(CTX, #{
    model => ?MODULE
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec create(id(), file_meta:uuid()) -> ok | {error, term()}.
create(TaskId, FileUuid) ->
    ?extract_ok(datastore_model:create(?CTX, #document{
        key = gen_id(TaskId, FileUuid),
        value = #tree_traverse_progress{} % todo maybe set +1 here?
    })).


-spec report_children_to_process(id(), file_meta:uuid(), non_neg_integer(), boolean()) -> status().
report_children_to_process(TaskId, FileUUid, ChildrenCount, AllBatchesListed) ->
    update_and_check(TaskId, FileUUid, fun(#tree_traverse_progress{
        to_process = ToProcess,
        all_batches_listed = CurrentAllBatchesListed
    } = TTS) ->
        {ok, TTS#tree_traverse_progress{
            to_process = ToProcess + ChildrenCount,
            all_batches_listed = case AllBatchesListed of
                true -> true;
                false -> CurrentAllBatchesListed
            end
        }}
    end).


-spec report_child_processed(id(), file_meta:uuid()) -> status().
report_child_processed(TaskId, FileUuid) ->
    update_and_check(TaskId, FileUuid, fun(#tree_traverse_progress{processed = Processed} = TTS) ->
        {ok, TTS#tree_traverse_progress{processed = Processed + 1}}
    end).


-spec report_last_batch(id(), file_meta:uuid()) ->
    status() | {error, term()}.
report_last_batch(TaskId, FileUuid) ->
    update_and_check(TaskId, FileUuid, fun(TTS) ->
        {ok, TTS#tree_traverse_progress{all_batches_listed = true}}
    end).


-spec delete(id(), file_meta:uuid()) -> ok | {error, term()}.
delete(TaskId, FileUuid) ->
    ?extract_ok(datastore_model:delete(?CTX, gen_id(TaskId, FileUuid))).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec update(tree_traverse:task_id(), file_meta:uuid(), diff()) -> {ok, doc()} | {error, term()}.
update(TaskId, Uuid, UpdateFun) ->
    datastore_model:update(?CTX, gen_id(TaskId, Uuid), UpdateFun).

%% @private
-spec update_and_check(tree_traverse:task_id(), file_meta:uuid(), diff()) -> status() | {error, term()}.
update_and_check(TaskId, Uuid, UpdateFun) ->
    case update(TaskId, Uuid, UpdateFun) of
        {ok, #document{value = #tree_traverse_progress{
            to_process = ToProcess,
            processed = ToProcess,
            all_batches_listed = true
        }}} ->
            ?SUBTREE_PROCESSED;
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
    1.


-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {to_process, integer},
        {processed, integer},
        {all_batches_listed, boolean}
    ]}.