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

-include_lib("ctool/include/errors.hrl").

%% atm_container callbacks
-export([create/2, get_data_spec/1, acquire_iterator/1, update/4]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).

%% datastore API
-export([get_ctx/0]).


-type initial_value() :: [atm_api:item()].
%% Full 'initial_value' format can't be expressed directly in type spec due to
%% dialyzer limitations in specifying individual binaries. Instead it is
%% shown below:
%%
%% #{
%%      <<"isBatch">> := boolean()
%% }
-type update_options() :: #{binary() => boolean()}. 

-record(atm_list_container, {
    data_spec :: atm_data_spec:record(),
    backend_id :: binary()
}).
-type record() :: #atm_list_container{}.

-export_type([initial_value/0, update_options/0, record/0]).


-define(CTX, atm_store:get_ctx()).

%%%===================================================================
%%% atm_container callbacks
%%%===================================================================


-spec create(atm_data_spec:record(), initial_value()) -> record() | no_return().
create(AtmDataSpec, InitOpts) ->
    Key = datastore_key:new(),
    ok = datastore_infinite_log:create(?CTX, Key, InitOpts),

    #atm_list_container{
        data_spec = AtmDataSpec,
        backend_id = Key
    }.


-spec get_data_spec(record()) -> atm_data_spec:record().
get_data_spec(#atm_list_container{data_spec = AtmDataSpec}) ->
    AtmDataSpec.


-spec acquire_iterator(record()) -> atm_list_container_iterator:record().
acquire_iterator(#atm_list_container{backend_id = BackendId}) ->
    atm_list_container_iterator:create(BackendId).


-spec update(record(), atm_container:update_operation(), update_options(), atm_api:item()) ->
    record() | no_return().
update(#atm_list_container{} = Record, append, #{<<"isBatch">> := true}, Batch) ->
    #atm_list_container{data_spec = AtmDataSpec, backend_id = BackendId} = Record,
    lists:foreach(fun(Item) ->
        atm_data_validator:validate(Item, AtmDataSpec)
    end, Batch),
    lists:foreach(fun(Item) ->
        ok = datastore_infinite_log:append(?CTX, BackendId, json_utils:encode(Item))
    end, Batch),
    Record;
update(#atm_list_container{} = Record, append, Options, Item) ->
    update(Record, append, Options#{<<"isBatch">> => true}, [Item]);
update(_Record, _Operation, _Options, _Item) ->
    throw(?ERROR_NOT_SUPPORTED).


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
%%% datastore API 
%%%===================================================================

-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.