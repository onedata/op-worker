%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements `atm_container` functionality for `tree forest`
%%% atm_store type. Uses `atm_list_container` for storing list of roots.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_tree_forest_container).
-author("Michal Stanisz").

-behaviour(atm_container).
-behaviour(persistent_record).

-include("modules/automation/atm_tmp.hrl").
-include_lib("ctool/include/errors.hrl").

%% atm_container callbacks
-export([create/3, get_data_spec/1, acquire_iterator/1, apply_operation/2, delete/1]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).

-type operation_options() :: #{binary() => boolean()}.
-type initial_value() :: [atm_api:item()] | undefined.
-record(atm_tree_forest_container, {
    roots_list :: atm_list_container:record()
}).
-type record() :: #atm_tree_forest_container{}.

-export_type([initial_value/0, operation_options/0, record/0]).


%%%===================================================================
%%% atm_container callbacks
%%%===================================================================

-spec create(atm_data_spec:record(), initial_value(), atm_workflow_execution_ctx:record()) -> record() | no_return().
create(AtmDataSpec, InitialValue, AtmWorkflowExecutionCtx) ->
    #atm_tree_forest_container{
        roots_list = atm_list_container:create(AtmDataSpec, InitialValue, AtmWorkflowExecutionCtx)
    }.


-spec get_data_spec(record()) -> atm_data_spec:record().
get_data_spec(#atm_tree_forest_container{roots_list = RootsList}) ->
    atm_list_container:get_data_spec(RootsList).


-spec acquire_iterator(record()) -> atm_tree_forest_container_iterator:record().
acquire_iterator(#atm_tree_forest_container{roots_list = RootsList}) ->
    DataSpec = atm_list_container:get_data_spec(RootsList),
    RootsIterator = atm_list_container:acquire_iterator(RootsList),
    atm_tree_forest_container_iterator:build(RootsIterator, DataSpec).


-spec apply_operation(record(), atm_container:operation()) ->
    record() | no_return().
apply_operation(#atm_tree_forest_container{roots_list = RootsList} = Record, Operation) ->
    Record#atm_tree_forest_container{
        roots_list = atm_list_container:apply_operation(RootsList, Operation)
    }.


-spec delete(record()) -> ok.
delete(#atm_tree_forest_container{roots_list = RootsList}) ->
    atm_list_container:delete(RootsList).


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================

-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#atm_tree_forest_container{roots_list = ListContainer}, NestedRecordEncoder) ->
    #{
        <<"rootsList">> => NestedRecordEncoder(ListContainer, atm_list_container)
    }.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{<<"rootsList">> := EncodedListContainer}, NestedRecordDecoder) ->
    #atm_tree_forest_container{
        roots_list = NestedRecordDecoder(EncodedListContainer, atm_list_container)
    }.
