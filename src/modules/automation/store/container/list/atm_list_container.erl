%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements `atm_container` functionality for `list`
%%% atm_store type.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_list_container).
-author("Michal Stanisz").

-behaviour(atm_container).
-behaviour(persistent_record).

-include("modules/automation/atm_tmp.hrl").
-include_lib("ctool/include/errors.hrl").

%% atm_container callbacks
-export([create/2, get_data_spec/1, acquire_iterator/1, apply_operation/4, delete/1]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-type initial_value() :: [atm_api:item()] | undefined.
%% Full 'apply_operation_options' format can't be expressed directly in type spec due to
%% dialyzer limitations in specifying individual binaries. Instead it is
%% shown below:
%%
%% #{
%%      <<"isBatch">> := boolean()
%% }
-type apply_operation_options() :: #{binary() => boolean()}.

% id of underlying persistent record implemented by `atm_list_store_backend`
-type backend_id() :: binary().

-record(atm_list_container, {
    data_spec :: atm_data_spec:record(),
    backend_id :: backend_id()
}).
-type record() :: #atm_list_container{}.

-export_type([initial_value/0, apply_operation_options/0, backend_id/0, record/0]).


%%%===================================================================
%%% atm_container callbacks
%%%===================================================================

-spec create(atm_data_spec:record(), initial_value()) -> record() | no_return().
create(AtmDataSpec, undefined) ->
    create_container(AtmDataSpec);
create(AtmDataSpec, InitialValue) when is_list(InitialValue) ->
    validate_data_batch(AtmDataSpec, InitialValue),
    apply_operation(append, InitialValue, #{<<"isBatch">> => true}, create_container(AtmDataSpec));
create(_AtmDataSpec, _InitialValue) ->
    throw(?ERROR_ATM_BAD_DATA(<<"initialValue">>, <<"not a list">>)).


-spec get_data_spec(record()) -> atm_data_spec:record().
get_data_spec(#atm_list_container{data_spec = AtmDataSpec}) ->
    AtmDataSpec.


-spec acquire_iterator(record()) -> atm_list_container_iterator:record().
acquire_iterator(#atm_list_container{backend_id = BackendId}) ->
    atm_list_container_iterator:build(BackendId).


-spec apply_operation(atm_container:operation(), atm_api:item(), apply_operation_options(), record()) ->
    record() | no_return().
apply_operation(append, Batch, #{<<"isBatch">> := true}, #atm_list_container{} = Record) when is_list(Batch) ->
    #atm_list_container{data_spec = AtmDataSpec, backend_id = BackendId} = Record,
    validate_data_batch(AtmDataSpec, Batch),
    lists:foreach(fun(Item) ->
        ok = atm_list_store_backend:append(BackendId, json_utils:encode(Item))
    end, Batch),
    Record;
apply_operation(append, Item, Options, #atm_list_container{} = Record) ->
    apply_operation(append, [Item], Options#{<<"isBatch">> => true}, Record);
apply_operation(_Operation, _Item, _Options, _Record) ->
    throw(?ERROR_NOT_SUPPORTED).


-spec delete(record()) -> ok.
delete(#atm_list_container{backend_id = BackendId}) ->
    atm_list_store_backend:destroy(BackendId).


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================

-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#atm_list_container{
    data_spec = AtmDataSpec,
    backend_id = BackendId
}, NestedRecordEncoder) ->
        #{
            <<"dataSpec">> => NestedRecordEncoder(AtmDataSpec, atm_data_spec), 
            <<"backendId">> => BackendId
        }.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{<<"dataSpec">> := AtmDataSpecJson, <<"backendId">> := BackendId}, NestedRecordDecoder) ->
    #atm_list_container{
        data_spec = NestedRecordDecoder(AtmDataSpecJson, atm_data_spec),
        backend_id = BackendId
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec create_container(atm_data_spec:record()) -> record().
create_container(AtmDataSpec) ->
    {ok, Id} = atm_list_store_backend:create(#{}),
    #atm_list_container{
        data_spec = AtmDataSpec,
        backend_id = Id
    }.


-spec validate_data_batch(atm_data_spec:record(), [json_utils:json_term()]) -> ok | no_return().
validate_data_batch(AtmDataSpec, Batch) ->
    lists:foreach(fun(Item) ->
        atm_data_validator:validate(Item, AtmDataSpec)
    end, Batch).
