%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements `atm_store_container` functionality for `single_value`
%%% atm_store type.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_single_value_store_container).
-author("Bartosz Walkowicz").

-behaviour(atm_store_container).
-behaviour(persistent_record).

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/errors.hrl").

%% atm_store_container callbacks
-export([
    create/1,
    copy/1,
    get_config/1,

    get_iterated_item_data_spec/1,
    acquire_iterator/1,

    browse_content/2,
    update_content/2,

    delete/1
]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-type initial_content() :: undefined | automation:item().

-type content_browse_req() :: #atm_store_content_browse_req{
    options :: atm_single_value_store_content_browse_options:record()
}.
-type content_update_req() :: #atm_store_content_update_req{
    options :: atm_single_value_store_content_update_options:record()
}.

-record(atm_single_value_store_container, {
    config :: atm_single_value_store_config:record(),
    compressed_item :: undefined | atm_store:item()
}).
-type record() :: #atm_single_value_store_container{}.

-export_type([
    initial_content/0, content_browse_req/0, content_update_req/0,
    record/0
]).


%%%===================================================================
%%% atm_store_container callbacks
%%%===================================================================


-spec create(atm_store_container:creation_args()) -> record() | no_return().
create(#atm_store_container_creation_args{
    store_config = AtmStoreConfig,
    initial_content = undefined
}) ->
    #atm_single_value_store_container{config = AtmStoreConfig};

create(#atm_store_container_creation_args{
    workflow_execution_auth = AtmWorkflowExecutionAuth,
    store_config = AtmStoreConfig,
    initial_content = InitialContent
}) ->
    ItemDataSpec = AtmStoreConfig#atm_single_value_store_config.item_data_spec,
    atm_value:validate_constraints(AtmWorkflowExecutionAuth, InitialContent, ItemDataSpec),

    #atm_single_value_store_container{
        config = AtmStoreConfig,
        compressed_item = atm_value:to_store_item(InitialContent, ItemDataSpec)
    }.


-spec copy(record()) -> no_return().
copy(_) ->
    throw(?ERROR_NOT_SUPPORTED).


-spec get_config(record()) -> atm_single_value_store_config:record().
get_config(#atm_single_value_store_container{config = AtmStoreConfig}) ->
    AtmStoreConfig.


-spec get_iterated_item_data_spec(record()) -> atm_data_spec:record().
get_iterated_item_data_spec(#atm_single_value_store_container{
    config = #atm_single_value_store_config{item_data_spec = ItemDataSpec}
}) ->
    ItemDataSpec.


-spec acquire_iterator(record()) -> atm_single_value_store_container_iterator:record().
acquire_iterator(#atm_single_value_store_container{
    config = #atm_single_value_store_config{item_data_spec = ItemDataSpec},
    compressed_item = CompressedItem
}) ->
    %% TODO VFS-9150 throw error if no content
    atm_single_value_store_container_iterator:build(CompressedItem, ItemDataSpec).


-spec browse_content(record(), content_browse_req()) ->
    atm_single_value_store_content_browse_result:record() | no_return().
browse_content(
    #atm_single_value_store_container{compressed_item = undefined},
    #atm_store_content_browse_req{
        store_schema_id = AtmStoreSchemaId,
        options = #atm_single_value_store_content_browse_options{}
    }
) ->
    throw(?ERROR_ATM_STORE_CONTENT_NOT_SET(AtmStoreSchemaId));

browse_content(
    #atm_single_value_store_container{
        config = #atm_single_value_store_config{item_data_spec = ItemDataSpec},
        compressed_item = CompressedItem
    },
    #atm_store_content_browse_req{
        workflow_execution_auth = AtmWorkflowExecutionAuth,
        options = #atm_single_value_store_content_browse_options{}
    }
) ->
    Item = case atm_value:describe_store_item(AtmWorkflowExecutionAuth, CompressedItem, ItemDataSpec) of
        {ok, _} = Result -> Result;
        {error, _} -> ?ERROR_FORBIDDEN
    end,
    #atm_single_value_store_content_browse_result{item = Item}.


-spec update_content(record(), content_update_req()) -> record() | no_return().
update_content(Record, #atm_store_content_update_req{
    workflow_execution_auth = AtmWorkflowExecutionAuth,
    argument = Item,
    options = #atm_single_value_store_content_update_options{}
}) ->
    ItemDataSpec =
        Record#atm_single_value_store_container
        .config#atm_single_value_store_config
        .item_data_spec,
    atm_value:validate_constraints(AtmWorkflowExecutionAuth, Item, ItemDataSpec),

    Record#atm_single_value_store_container{
        compressed_item = atm_value:to_store_item(Item, ItemDataSpec)
    }.


-spec delete(record()) -> ok.
delete(_Record) ->
    ok.


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#atm_single_value_store_container{
    config = AtmStoreConfig,
    compressed_item = Item
}, NestedRecordEncoder) ->
    maps_utils:put_if_defined(
        #{<<"config">> => NestedRecordEncoder(AtmStoreConfig, atm_single_value_store_config)},
        <<"compressedItem">>, Item
    ).


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{<<"config">> := AtmStoreConfigJson} = AtmStoreContainerJson, NestedRecordDecoder) ->
    #atm_single_value_store_container{
        config = NestedRecordDecoder(AtmStoreConfigJson, atm_single_value_store_config),
        compressed_item = maps:get(<<"compressedItem">>, AtmStoreContainerJson, undefined)
    }.
