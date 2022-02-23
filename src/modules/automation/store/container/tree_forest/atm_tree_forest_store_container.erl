%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements `atm_store_container` functionality for `tree forest`
%%% atm_store type. Uses `atm_list_store_container` for storing list of roots.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_tree_forest_store_container).
-author("Michal Stanisz").

-behaviour(atm_store_container).
-behaviour(persistent_record).

-include_lib("ctool/include/automation/automation.hrl").
-include_lib("ctool/include/errors.hrl").

%% atm_store_container callbacks
-export([
    create/3,
    get_config/1, get_iterated_item_data_spec/1,
    browse_content/3, acquire_iterator/1,
    apply_operation/2,
    delete/1
]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).

-type operation_options() :: #{}.  %% for now no options are supported
-type browse_options() :: atm_list_store_container:browse_options().
-type initial_content() :: [atm_value:expanded()] | undefined.

-record(atm_tree_forest_store_container, {
    config :: atm_tree_forest_store_config:record(),
    roots_list :: atm_list_store_container:record()
}).
-type record() :: #atm_tree_forest_store_container{}.

-export_type([initial_content/0, operation_options/0, browse_options/0, record/0]).


%%%===================================================================
%%% atm_store_container callbacks
%%%===================================================================


-spec create(
    atm_workflow_execution_auth:record(),
    atm_tree_forest_store_config:record(),
    initial_content()
) ->
    record() | no_return().
create(AtmWorkflowExecutionAuth, AtmStoreConfig, InitialContent) ->
    RootsListStoreConfig = #atm_list_store_config{
        item_data_spec = AtmStoreConfig#atm_tree_forest_store_config.item_data_spec
    },

    #atm_tree_forest_store_container{
        config = AtmStoreConfig,
        roots_list = atm_list_store_container:create(
            AtmWorkflowExecutionAuth, RootsListStoreConfig, InitialContent
        )
    }.


-spec get_config(record()) -> atm_tree_forest_store_config:record().
get_config(#atm_tree_forest_store_container{config = AtmStoreConfig}) ->
    AtmStoreConfig.


-spec get_iterated_item_data_spec(record()) -> atm_data_spec:record().
get_iterated_item_data_spec(#atm_tree_forest_store_container{
    config = #atm_tree_forest_store_config{item_data_spec = ItemDataSpec}
}) ->
    ItemDataSpec.


-spec browse_content(atm_workflow_execution_auth:record(), browse_options(), record()) ->
    atm_store_api:browse_result() | no_return().
browse_content(AtmWorkflowExecutionAuth, BrowseOpts, #atm_tree_forest_store_container{
    roots_list = RootsList
}) ->
    atm_list_store_container:browse_content(AtmWorkflowExecutionAuth, BrowseOpts, RootsList).


-spec acquire_iterator(record()) -> atm_tree_forest_store_container_iterator:record().
acquire_iterator(#atm_tree_forest_store_container{
    config = #atm_tree_forest_store_config{item_data_spec = ItemDataSpec},
    roots_list = RootsList
}) ->
    RootsIterator = atm_list_store_container:acquire_iterator(RootsList),
    atm_tree_forest_store_container_iterator:build(ItemDataSpec, RootsIterator).


-spec apply_operation(record(), atm_store_container:operation()) ->
    record() | no_return().
apply_operation(#atm_tree_forest_store_container{roots_list = RootsList} = Record, Operation) ->
    Record#atm_tree_forest_store_container{
        roots_list = atm_list_store_container:apply_operation(RootsList, Operation)
    }.


-spec delete(record()) -> ok.
delete(#atm_tree_forest_store_container{roots_list = RootsList}) ->
    atm_list_store_container:delete(RootsList).


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#atm_tree_forest_store_container{
    config = AtmStoreConfig,
    roots_list = ListContainer
}, NestedRecordEncoder) ->
    #{
        <<"config">> => NestedRecordEncoder(AtmStoreConfig, atm_tree_forest_store_config),
        <<"rootsList">> => NestedRecordEncoder(ListContainer, atm_list_store_container)
    }.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(
    #{<<"config">> := AtmStoreConfigJson, <<"rootsList">> := EncodedListContainer},
    NestedRecordDecoder
) ->
    #atm_tree_forest_store_container{
        config = NestedRecordDecoder(AtmStoreConfigJson, atm_tree_forest_store_config),
        roots_list = NestedRecordDecoder(EncodedListContainer, atm_list_store_container)
    }.
