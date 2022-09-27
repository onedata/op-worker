%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides `atm_store_container_iterator` functionality for
%%% `atm_tree_forest_store_container`. Each atm_data_type, that is to be
%%% allowed for iteration must implement behaviour provided by this module. 
%%% All modules implementing this behaviour must be registered in 
%%% `get_callback_module` function.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_tree_forest_store_container_iterator).
-author("Michal Stanisz").

-behaviour(atm_store_container_iterator).
-behaviour(persistent_record).

-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/errors.hrl"). 
-include_lib("ctool/include/logging.hrl").

%% API
-export([build/2]).

% atm_store_container_iterator callbacks
-export([get_next_batch/3]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).

-type tree_pagination_token() :: binary().

-record(atm_tree_forest_store_container_iterator, {
    callback_module :: module(),
    item_data_spec :: atm_data_spec:record(),
    current_tree_root :: undefined | atm_value:compressed(),
    tree_pagination_token :: undefined | tree_pagination_token(),
    roots_iterator :: atm_list_store_container_iterator:record()
}).
-type record() :: #atm_tree_forest_store_container_iterator{}.

-export_type([record/0]).


%%%===================================================================
%%% Callbacks
%%%===================================================================


-callback list_tree(
    atm_workflow_execution_auth:record(), 
    tree_pagination_token() | undefined, 
    atm_value:compressed(), 
    atm_store_container_iterator:batch_size()
) -> 
    {[atm_value:expanded()], tree_pagination_token() | undefined}.

%%%===================================================================
%%% API
%%%===================================================================

-spec build(atm_data_spec:record(), atm_list_store_container_iterator:record()) -> record().
build(ItemDataSpec, RootsIterator) ->
    Module = get_callback_module(atm_data_spec:get_type(ItemDataSpec)),
    #atm_tree_forest_store_container_iterator{
        callback_module = Module,
        item_data_spec = ItemDataSpec,
        roots_iterator = RootsIterator,
        tree_pagination_token = undefined
    }.


%%%===================================================================
%%% atm_store_container_iterator callbacks
%%%===================================================================

-spec get_next_batch(
    atm_workflow_execution_auth:record(),
    atm_store_container_iterator:batch_size(),
    record()
) ->
    {ok, [atm_value:expanded()], record()} | stop.
get_next_batch(AtmWorkflowExecutionAuth, BatchSize, Record) ->
    get_next_batch(AtmWorkflowExecutionAuth, BatchSize, Record, []).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec get_next_batch(
    atm_workflow_execution_auth:record(),
    atm_store_container_iterator:batch_size(),
    record(), 
    [atm_value:compressed()]
) ->
    {ok, [atm_value:compressed()], record()} | stop.
get_next_batch(_AtmWorkflowExecutionAuth, BatchSize, Record, ForestAcc) when BatchSize =< 0 ->
    {ok, ForestAcc, Record};

get_next_batch(
    AtmWorkflowExecutionAuth,
    BatchSize,
    Record = #atm_tree_forest_store_container_iterator{
        tree_pagination_token = undefined
    },
    ForestAcc
) ->
    #atm_tree_forest_store_container_iterator{
        roots_iterator = ListIterator, 
        item_data_spec = ItemDataSpec
    } = Record,
    case atm_list_store_container_iterator:get_next_batch(AtmWorkflowExecutionAuth, 1, ListIterator) of
        {ok, [CurrentTreeRootExpanded], NextRootsIterator} ->
            CurrentTreeRoot = atm_value:compress(CurrentTreeRootExpanded, ItemDataSpec),
            NextTreeRecord = Record#atm_tree_forest_store_container_iterator{
                current_tree_root = CurrentTreeRoot,
                roots_iterator = NextRootsIterator
            },
            {TreeAcc, UpdatedRecord} = list_tree(AtmWorkflowExecutionAuth, BatchSize, NextTreeRecord),
            get_next_batch(
                AtmWorkflowExecutionAuth, BatchSize - length(TreeAcc), UpdatedRecord, TreeAcc ++ ForestAcc);
        {ok, [], NextRootsIterator} ->
            UpdatedRecord = Record#atm_tree_forest_store_container_iterator{
                roots_iterator = NextRootsIterator,
                tree_pagination_token = undefined
            },
            get_next_batch(AtmWorkflowExecutionAuth, BatchSize, UpdatedRecord, ForestAcc);
        stop ->
            case length(ForestAcc) of
                0 -> stop;
                _ -> {ok, ForestAcc, Record}
            end
    end;

get_next_batch(AtmWorkflowExecutionAuth, BatchSize, Record, ForestAcc) ->
    {TreeAcc, UpdatedRecord} = list_tree(AtmWorkflowExecutionAuth, BatchSize, Record),
    get_next_batch(
        AtmWorkflowExecutionAuth, BatchSize - length(TreeAcc), UpdatedRecord, TreeAcc ++ ForestAcc).


%% @private
-spec list_tree(
    atm_workflow_execution_auth:record(),
    atm_store_container_iterator:batch_size(),
    record()
) ->
    {[atm_value:compressed()], record()}.
list_tree(
    AtmWorkflowExecutionAuth,
    BatchSize,
    #atm_tree_forest_store_container_iterator{
        callback_module = Module,
        tree_pagination_token = PaginationToken,
        current_tree_root = CurrentTreeRoot
    } = Record
) ->
    {TreeAcc, NextPaginationToken} = Module:list_tree(
        AtmWorkflowExecutionAuth, PaginationToken, CurrentTreeRoot, BatchSize),
    UpdatedRecord = Record#atm_tree_forest_store_container_iterator{
        tree_pagination_token = NextPaginationToken
    },
    {TreeAcc, UpdatedRecord}.


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================

-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#atm_tree_forest_store_container_iterator{
    callback_module = Module,
    item_data_spec = ItemDataSpec,
    current_tree_root = CurrentTraversableItem,
    tree_pagination_token = TreePaginationToken,
    roots_iterator = RootsIterator
}, NestedRecordEncoder) ->
    #{
        <<"typeSpecificModule">> => atom_to_binary(Module, utf8),
        <<"itemDataSpec">> => NestedRecordEncoder(ItemDataSpec, atm_data_spec),
        <<"currentTraversableItem">> => utils:undefined_to_null(CurrentTraversableItem),
        <<"treePaginationToken">> => utils:undefined_to_null(TreePaginationToken),
        <<"rootsIterator">> => NestedRecordEncoder(RootsIterator, atm_list_store_container_iterator)
    }.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{
    <<"typeSpecificModule">> := EncodedModule,
    <<"itemDataSpec">> := ItemDataSpecJson,
    <<"currentTraversableItem">> := CurrentTraversableItem,
    <<"treePaginationToken">> := TreePaginationToken,
    <<"rootsIterator">> := RootsIterator
}, NestedRecordDecoder) ->
    Module = binary_to_atom(EncodedModule, utf8),
    #atm_tree_forest_store_container_iterator{
        callback_module = Module,
        item_data_spec = NestedRecordDecoder(ItemDataSpecJson, atm_data_spec),
        current_tree_root = utils:null_to_undefined(CurrentTraversableItem),
        tree_pagination_token = utils:null_to_undefined(TreePaginationToken),
        roots_iterator = NestedRecordDecoder(RootsIterator, atm_list_store_container_iterator)
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec get_callback_module(atm_data_type:type()) -> module().
get_callback_module(atm_dataset_type) ->
    atm_dataset_value;
get_callback_module(atm_file_type) ->
    atm_file_value.
