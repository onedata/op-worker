%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module defines `atm_store_container` interface - an object which can be
%%% used for storing and retrieving data of specific type for given store type.
%%%
%%%                             !!! Caution !!!
%%% 1) This behaviour must be implemented by modules with records of the same name.
%%% 2) Modules implementing this behaviour must also implement `persistent_record`
%%%    behaviour.
%%% 3) Modules implementing this behaviour must be registered in
%%%    `atm_store_type_to_atm_store_container_type` and
%%%    `atm_store_container_type_to_atm_store_type` functions.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_store_container).
-author("Bartosz Walkowicz").

-behaviour(persistent_record).

-include("modules/automation/atm_execution.hrl").

%% API
-export([
    create/4,
    get_store_type/1, get_config/1, get_iterated_item_data_spec/1,
    acquire_iterator/1,
    browse_content/2,
    update_content/2,
    delete/1
]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-type type() ::
    atm_audit_log_store_container |
    atm_list_store_container |
    atm_range_store_container |
    atm_single_value_store_container |
    atm_tree_forest_store_container.

-type initial_content() ::
    atm_audit_log_store_container:initial_content() |
    atm_list_store_container:initial_content() |
    atm_range_store_container:initial_content() |
    atm_single_value_store_container:initial_content() |
    atm_tree_forest_store_container:initial_content().

-type record() ::
    atm_audit_log_store_container:record() |
    atm_list_store_container:record() |
    atm_range_store_container:record() |
    atm_single_value_store_container:record() |
    atm_tree_forest_store_container:record().

-type content_browse_req() ::
    atm_audit_log_store_container:content_browse_req() |
    atm_list_store_container:content_browse_req() |
    atm_range_store_container:content_browse_req() |
    atm_single_value_store_container:content_browse_req() |
    atm_tree_forest_store_container:content_browse_req().

-type content_update_req() ::
    atm_audit_log_store_container:content_update_req() |
    atm_list_store_container:content_update_req() |
    atm_range_store_container:content_update_req() |
    atm_single_value_store_container:content_update_req() |
    atm_tree_forest_store_container:content_update_req().

-export_type([type/0, initial_content/0, record/0]).
-export_type([content_browse_req/0, content_update_req/0]).


%%%===================================================================
%%% Callbacks
%%%===================================================================


-callback create(
    atm_workflow_execution_auth:record(),
    atm_store_config:record(),
    initial_content()
) ->
    record() | no_return().

-callback get_config(record()) -> atm_store_config:record().

-callback get_iterated_item_data_spec(record()) -> atm_data_spec:record().

-callback acquire_iterator(record()) -> atm_store_container_iterator:record().

-callback browse_content(record(), content_browse_req()) ->
    atm_store_api:browse_result() | no_return().  %% TODO browse result

-callback update_content(record(), content_update_req()) -> record() | no_return().

-callback delete(record()) -> ok | no_return().


%%%===================================================================
%%% API
%%%===================================================================


-spec create(
    automation:store_type(),
    atm_workflow_execution_auth:record(),
    atm_store_config:record(),
    initial_content()
) ->
    record().
create(AtmStoreType, AtmWorkflowExecutionAuth, AtmStoreConfig, InitialContent) ->
    RecordType = atm_store_type_to_atm_store_container_type(AtmStoreType),
    RecordType:create(AtmWorkflowExecutionAuth, AtmStoreConfig, InitialContent).


-spec get_store_type(record()) -> automation:store_type().
get_store_type(AtmStoreContainer) ->
    RecordType = utils:record_type(AtmStoreContainer),
    atm_store_container_type_to_atm_store_type(RecordType).


-spec get_config(record()) -> atm_store_config:record().
get_config(AtmStoreContainer) ->
    RecordType = utils:record_type(AtmStoreContainer),
    RecordType:get_config(AtmStoreContainer).


-spec get_iterated_item_data_spec(record()) -> atm_data_spec:record().
get_iterated_item_data_spec(AtmStoreContainer) ->
    RecordType = utils:record_type(AtmStoreContainer),
    RecordType:get_iterated_item_data_spec(AtmStoreContainer).


-spec acquire_iterator(record()) -> atm_store_container_iterator:record().
acquire_iterator(AtmStoreContainer) ->
    RecordType = utils:record_type(AtmStoreContainer),
    RecordType:acquire_iterator(AtmStoreContainer).


-spec browse_content(record(), content_browse_req()) ->
    atm_store_api:browse_result() | no_return().
browse_content(AtmStoreContainer, AtmStoreContentBrowseReq) ->
    RecordType = utils:record_type(AtmStoreContainer),
    RecordType:browse_content(AtmStoreContainer, AtmStoreContentBrowseReq).


-spec update_content(record(), content_update_req()) -> record() | no_return().
update_content(AtmStoreContainer, AtmStoreContentUpdateReq) ->
    RecordType = utils:record_type(AtmStoreContainer),
    RecordType:update_content(AtmStoreContainer, AtmStoreContentUpdateReq).


-spec delete(record()) -> ok | no_return().
delete(AtmStoreContainer) ->
    RecordType = utils:record_type(AtmStoreContainer),
    RecordType:delete(AtmStoreContainer).


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(AtmStoreContainer, NestedRecordEncoder) ->
    RecordType = utils:record_type(AtmStoreContainer),
    AtmStoreType = atm_store_container_type_to_atm_store_type(RecordType),

    maps:merge(
        #{<<"type">> => automation:store_type_to_json(AtmStoreType)},
        NestedRecordEncoder(AtmStoreContainer, RecordType)
    ).


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{<<"type">> := AtmStoreTypeJson} = AtmStoreContainerJson, NestedRecordDecoder) ->
    AtmStoreType = automation:store_type_from_json(AtmStoreTypeJson),
    RecordType = atm_store_type_to_atm_store_container_type(AtmStoreType),

    NestedRecordDecoder(AtmStoreContainerJson, RecordType).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec atm_store_type_to_atm_store_container_type(automation:store_type()) ->
    atm_store_container:type().
atm_store_type_to_atm_store_container_type(audit_log) -> atm_audit_log_store_container;
atm_store_type_to_atm_store_container_type(list) -> atm_list_store_container;
atm_store_type_to_atm_store_container_type(range) -> atm_range_store_container;
atm_store_type_to_atm_store_container_type(single_value) -> atm_single_value_store_container;
atm_store_type_to_atm_store_container_type(tree_forest) -> atm_tree_forest_store_container.


%% @private
-spec atm_store_container_type_to_atm_store_type(atm_store_container:type()) ->
    automation:store_type().
atm_store_container_type_to_atm_store_type(atm_audit_log_store_container) -> audit_log;
atm_store_container_type_to_atm_store_type(atm_list_store_container) -> list;
atm_store_container_type_to_atm_store_type(atm_range_store_container) -> range;
atm_store_container_type_to_atm_store_type(atm_single_value_store_container) -> single_value;
atm_store_container_type_to_atm_store_type(atm_tree_forest_store_container) -> tree_forest.
