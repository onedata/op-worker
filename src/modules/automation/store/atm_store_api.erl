%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% API module for performing operations on automation stores.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_store_api).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_tmp.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([
    create/2,
    init_stream/2
]).

-type init_args() :: atm_container:init_args().

-export_type([init_args/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec create(atm_store_schema(), init_args()) -> {ok, atm_store:id()} | {error, term()}.
create(#atm_store_schema{
    name = Name,
    summary = Summary,
    description = Description,
    is_input_store = IsInputStore,
    store_type = StoreType,
    data_spec = AtmDataSpec
}, InitialArgs) ->
    ContainerModel = store_type_to_container_model(StoreType),

    {ok, _} = atm_store:create(#atm_store{
        name = Name,
        summary = Summary,
        description = Description,
        frozen = false,
        is_input_store = IsInputStore,
        type = StoreType,
        container = atm_container:create(ContainerModel, AtmDataSpec, InitialArgs)
    }).


-spec init_stream(atm_stream_schema(), atm_store:id() | atm_store:record()) ->
    atm_stream:stream().
init_stream(AtmStreamSchema, AtmStoreIdOrRecord) ->
    #atm_store{container = AtmContainer} = ensure_atm_store_record(AtmStoreIdOrRecord),
    atm_stream:init(AtmStreamSchema, AtmContainer).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec store_type_to_container_model(automation:store_type()) ->
    atm_container:model().
store_type_to_container_model(single_value) -> atm_single_value_container;
store_type_to_container_model(range) -> atm_range_container.


%% @private
-spec ensure_atm_store_record(atm_store:id() | atm_store:record()) ->
    atm_store:record().
ensure_atm_store_record(#atm_store{} = AtmStoreRecord) ->
    AtmStoreRecord;
ensure_atm_store_record(AtmStoreId) ->
    {ok, AtmStoreRecord} = atm_store:get(AtmStoreId),
    AtmStoreRecord.
