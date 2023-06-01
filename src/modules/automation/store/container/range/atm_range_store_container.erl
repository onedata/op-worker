%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements `atm_store_container` functionality for `range`
%%% atm_store type.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_range_store_container).
-author("Bartosz Walkowicz").

-behaviour(atm_store_container).
-behaviour(persistent_record).

-include("modules/automation/atm_execution.hrl").

%% atm_store_container callbacks
-export([
    create/3,
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


-type initial_content() :: undefined | atm_range_value:range_json().

-type content_browse_req() :: #atm_store_content_browse_req{
    options :: atm_range_store_content_browse_options:record()
}.
-type content_update_req() :: #atm_store_content_update_req{
    options :: atm_range_store_content_update_options:record()
}.

-record(atm_range_store_container, {
    config :: atm_range_store_config:record(),
    range :: undefined | atm_range_value:range()
}).
-type record() :: #atm_range_store_container{}.

-export_type([
    initial_content/0, content_browse_req/0, content_update_req/0,
    record/0
]).


-define(RANGE_DATA_SPEC, #atm_range_data_spec{}).


%%%===================================================================
%%% atm_store_container callbacks
%%%===================================================================


-spec create(
    atm_workflow_execution_auth:record(),
    atm_range_store_config:record(),
    initial_content()
) ->
    record() | no_return().
create(_AtmWorkflowExecutionAuth, AtmStoreConfig, undefined) ->
    #atm_range_store_container{
        config = AtmStoreConfig,
        range = undefined
    };

create(AtmWorkflowExecutionAuth, AtmStoreConfig, InitialContent) ->
    atm_value:validate_constraints(AtmWorkflowExecutionAuth, InitialContent, ?RANGE_DATA_SPEC),

    #atm_range_store_container{
        config = AtmStoreConfig,
        range = atm_value:to_store_item(InitialContent, ?RANGE_DATA_SPEC)
    }.


-spec copy(record()) -> no_return().
copy(_) ->
    throw(?ERROR_NOT_SUPPORTED).


-spec get_config(record()) -> atm_range_store_config:record().
get_config(#atm_range_store_container{config = AtmStoreConfig}) ->
    AtmStoreConfig.


-spec get_iterated_item_data_spec(record()) -> atm_data_spec:record().
get_iterated_item_data_spec(_) ->
    #atm_number_data_spec{integers_only = true, allowed_values = undefined}.


-spec acquire_iterator(record()) -> atm_range_store_container_iterator:record().
acquire_iterator(#atm_range_store_container{range = undefined}) ->
    %% TODO VFS-9150 throw error if no content
    atm_range_store_container_iterator:build(0, 0, 1);

acquire_iterator(#atm_range_store_container{range = [Start, End, Step]}) ->
    atm_range_store_container_iterator:build(Start, End, Step).


-spec browse_content(record(), content_browse_req()) ->
    atm_range_store_content_browse_result:record() | no_return().
browse_content(
    #atm_range_store_container{range = undefined},
    #atm_store_content_browse_req{
        store_schema_id = AtmStoreSchemaId,
        options = #atm_range_store_content_browse_options{}
    }
) ->
    throw(?ERROR_ATM_STORE_CONTENT_NOT_SET(AtmStoreSchemaId));

browse_content(
    #atm_range_store_container{range = Range},
    #atm_store_content_browse_req{
        workflow_execution_auth = AtmWorkflowExecutionAuth,
        options = #atm_range_store_content_browse_options{}
    }
) ->
    {ok, RangeJson} = atm_value:describe_store_item(AtmWorkflowExecutionAuth, Range, ?RANGE_DATA_SPEC),
    #atm_range_store_content_browse_result{range = RangeJson}.


-spec update_content(record(), content_update_req()) -> record() | no_return().
update_content(Record, #atm_store_content_update_req{
    workflow_execution_auth = AtmWorkflowExecutionAuth,
    argument = Item,
    options = #atm_range_store_content_update_options{}
}) ->
    atm_value:validate_constraints(AtmWorkflowExecutionAuth, Item, ?RANGE_DATA_SPEC),
    Record#atm_range_store_container{range = atm_value:to_store_item(Item, ?RANGE_DATA_SPEC)}.


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
db_encode(
    #atm_range_store_container{config = AtmStoreConfig, range = Range},
    NestedRecordEncoder
) ->
    maps_utils:put_if_defined(
        #{<<"config">> => NestedRecordEncoder(AtmStoreConfig, atm_range_store_config)},
        <<"range">>,
        Range
    ).


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(
    AtmStoreContainerJson = #{<<"config">> := AtmStoreConfigJson},
    NestedRecordDecoder
) ->
    #atm_range_store_container{
        config = NestedRecordDecoder(AtmStoreConfigJson, atm_range_store_config),
        range = maps:get(<<"range">>, AtmStoreContainerJson, undefined)
    }.
