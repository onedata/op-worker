%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This model holds information necessary to tell whether whole subtree 
%%% of a directory was traversed so this directory can be cleaned up.
%%% One `cleanup_traverse_status` document is created per directory.
%%%
%%% Traverse lists children in batches and model holds information about 
%%% number of remaining (i.e not yet traversed) already listed children 
%%% and whether all batches of have been listed. 
%%% Based on this information it can be determined whether subtree of a 
%%% directory was traversed (no children left and all batches have been 
%%% evaluated).
%%% @end
%%%--------------------------------------------------------------------
-module(cleanup_traverse_status).
-author("Michal Stanisz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").

%% API
-export([
    create/2, 
    report_child_traversed/2, 
    report_children_listed/3, 
    report_last_batch/2
]).

%% datastore_model callbacks
-export([
    get_ctx/0, 
    get_record_struct/1, 
    get_record_version/0
]).

-type id() :: datastore_model:key().
-type record() :: #cleanup_traverse_status{}.
-type diff() :: datastore_doc:diff(record()).
-type status() :: traversed | not_traversed.

-export_type([status/0]).

-define(CTX, #{
    model => ?MODULE
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec create(unsupport_cleanup_traverse:id(), file_meta:uuid()) -> ok | {error, term()}.
create(TaskId, FileUuid) ->
    ?extract_ok(datastore_model:create(?CTX, 
        #document{key = gen_id(TaskId, FileUuid), value = #cleanup_traverse_status{}})).

-spec report_child_traversed(unsupport_cleanup_traverse:id(), file_meta:uuid()) -> status().
report_child_traversed(TaskId, FileUuid) ->
    add_children(TaskId, FileUuid, -1).

-spec report_children_listed(unsupport_cleanup_traverse:id(), file_meta:uuid(), non_neg_integer()) -> 
    ok.
report_children_listed(TaskId, FileId, ChildrenCount) ->
    add_children(TaskId, FileId, ChildrenCount),
    ok.

-spec report_last_batch(unsupport_cleanup_traverse:id(), file_meta:uuid()) ->
    status() | {error, term()}.
report_last_batch(TaskId, FileUuid) ->
    update_and_check(gen_id(TaskId, FileUuid), fun(#cleanup_traverse_status{} = Value) -> 
        {ok, Value#cleanup_traverse_status{last_batch = true}}
    end).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec add_children(unsupport_cleanup_traverse:id(), file_meta:uuid(), integer()) ->
    status() | {error, term()}.
add_children(TaskId, FileUuid, ChildrenCount) ->
    update_and_check(gen_id(TaskId, FileUuid), 
        fun(#cleanup_traverse_status{children_count = PrevCount} = Value) ->
            {ok, Value#cleanup_traverse_status{children_count = PrevCount + ChildrenCount}}
        end).

%% @private
-spec update_and_check(id(), diff()) -> status() | {error, term()}.
update_and_check(Id, UpdateFun) ->
    case datastore_model:update(?CTX, Id, UpdateFun) of
        {ok, #document{value = #cleanup_traverse_status{children_count = 0, last_batch = true}}} ->
            traversed;
        {ok, _} -> not_traversed;
        Error -> Error
    end.

%% @private
-spec gen_id(unsupport_cleanup_traverse:id(), file_meta:uuid()) -> id().
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
        {children_count, integer},
        {last_batch, boolean}
    ]}.
