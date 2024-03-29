%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements `atm_store_container` functionality for `list`
%%% atm_store type.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_list_store_container).
-author("Michal Stanisz").

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


-type initial_content() :: [automation:item()] | undefined.

-type content_browse_req() :: #atm_store_content_browse_req{
    options :: atm_list_store_content_browse_options:record()
}.
-type content_update_req() :: #atm_store_content_update_req{
    options :: atm_list_store_content_update_options:record()
}.

-record(atm_list_store_container, {
    config :: atm_list_store_config:record(),
    backend_id :: atm_store_container_infinite_log_backend:id()
}).
-type record() :: #atm_list_store_container{}.

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
    create_container(AtmStoreConfig);

create(#atm_store_container_creation_args{
    workflow_execution_auth = AtmWorkflowExecutionAuth,
    store_config = AtmStoreConfig,
    initial_content = InitialItemsArray
}) ->
    atm_value:validate_constraints(
        AtmWorkflowExecutionAuth,
        InitialItemsArray,
        ?ATM_ARRAY_DATA_SPEC(AtmStoreConfig#atm_list_store_config.item_data_spec)
    ),

    Record = create_container(AtmStoreConfig),
    extend_insecure(InitialItemsArray, Record),
    Record.


-spec copy(record()) -> no_return().
copy(_) ->
    throw(?ERROR_NOT_SUPPORTED).


-spec get_config(record()) -> atm_list_store_config:record().
get_config(#atm_list_store_container{config = AtmStoreConfig}) ->
    AtmStoreConfig.


-spec get_iterated_item_data_spec(record()) -> atm_data_spec:record().
get_iterated_item_data_spec(Record) ->
    get_item_data_spec(Record).


-spec acquire_iterator(record()) -> atm_list_store_container_iterator:record().
acquire_iterator(#atm_list_store_container{
    config = #atm_list_store_config{item_data_spec = ItemDataSpec},
    backend_id = BackendId
}) ->
    atm_list_store_container_iterator:build(ItemDataSpec, BackendId).


-spec browse_content(record(), content_browse_req()) ->
    atm_list_store_content_browse_result:record() | no_return().
browse_content(Record, #atm_store_content_browse_req{
    workflow_execution_auth = AtmWorkflowExecutionAuth,
    options = #atm_list_store_content_browse_options{listing_opts = ListingOpts}
}) ->
    ItemDataSpec = get_item_data_spec(Record),
    ListingPostprocessor = fun({Index, {_Timestamp, CompressedItem}}) ->
        {Index, atm_value:describe_store_item(AtmWorkflowExecutionAuth, CompressedItem, ItemDataSpec)}
    end,
    {ok, {ProgressMarker, Entries}} = atm_store_container_infinite_log_backend:list_entries(
        Record#atm_list_store_container.backend_id,
        ListingOpts,
        ListingPostprocessor
    ),
    #atm_list_store_content_browse_result{
        items = Entries,
        is_last = ProgressMarker =:= done
    }.


-spec update_content(record(), content_update_req()) -> ok | no_return().
update_content(Record, #atm_store_content_update_req{
    workflow_execution_auth = AtmWorkflowExecutionAuth,
    argument = ItemsArray,
    options = #atm_list_store_content_update_options{function = extend}
}) ->
    atm_value:validate_constraints(
        AtmWorkflowExecutionAuth,
        ItemsArray,
        ?ATM_ARRAY_DATA_SPEC(get_item_data_spec(Record))
    ),
    extend_insecure(ItemsArray, Record);

update_content(Record, #atm_store_content_update_req{
    workflow_execution_auth = AtmWorkflowExecutionAuth,
    argument = Item,
    options = #atm_list_store_content_update_options{function = append}
}) ->
    atm_value:validate_constraints(AtmWorkflowExecutionAuth, Item, get_item_data_spec(Record)),
    append_insecure(Item, Record).


-spec delete(record()) -> ok.
delete(#atm_list_store_container{backend_id = BackendId}) ->
    atm_store_container_infinite_log_backend:delete(BackendId).


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#atm_list_store_container{
    config = AtmStoreConfig,
    backend_id = BackendId
}, NestedRecordEncoder) ->
    #{
        <<"config">> => NestedRecordEncoder(AtmStoreConfig, atm_list_store_config),
        <<"backendId">> => BackendId
    }.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(
    #{<<"config">> := AtmStoreConfigJson, <<"backendId">> := BackendId},
    NestedRecordDecoder
) ->
    #atm_list_store_container{
        config = NestedRecordDecoder(AtmStoreConfigJson, atm_list_store_config),
        backend_id = BackendId
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec create_container(atm_list_store_config:record()) -> record().
create_container(AtmStoreConfig) ->
    #atm_list_store_container{
        config = AtmStoreConfig,
        backend_id = atm_store_container_infinite_log_backend:create()
    }.


%% @private
-spec get_item_data_spec(record()) -> atm_data_spec:record().
get_item_data_spec(#atm_list_store_container{config = #atm_list_store_config{
    item_data_spec = ItemDataSpec
}}) ->
    ItemDataSpec.


%% @private
-spec extend_insecure([automation:item()], record()) -> ok.
extend_insecure(ItemsArray, #atm_list_store_container{
    config = #atm_list_store_config{item_data_spec = ItemDataSpec},
    backend_id = BackendId
}) ->
    %% TODO VFS-11091 infinite_log:extend
    lists:foreach(fun(Item) ->
        atm_store_container_infinite_log_backend:append(
            BackendId, atm_value:to_store_item(Item, ItemDataSpec)
        )
    end, ItemsArray).


%% @private
-spec append_insecure(automation:item(), record()) -> ok.
append_insecure(Item, #atm_list_store_container{
    config = #atm_list_store_config{item_data_spec = ItemDataSpec},
    backend_id = BackendId
}) ->
    ok = atm_store_container_infinite_log_backend:append(
        BackendId, atm_value:to_store_item(Item, ItemDataSpec)
    ).
