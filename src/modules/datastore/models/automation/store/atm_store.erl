%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model for storing information about automation store.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_store).
-author("Bartosz Walkowicz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").

%% API
-export([create/1, get/1, update/2, delete/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0]).
-export([encode_initial_content/1, decode_initial_content/1]).


-type id() :: binary().
-type record() :: #atm_store{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).

% automation:item converted (if needed) to a format for persisting in a store
-type item() :: json_utils:json_term().

-export_type([id/0, record/0, doc/0, diff/0]).
-export_type([item/0]).

-define(CTX, #{model => ?MODULE}).


%%%===================================================================
%%% API
%%%===================================================================


-spec create(record()) -> {ok, doc()} | {error, term()}.
create(AtmStoreRecord) ->
    datastore_model:create(?CTX, #document{value = AtmStoreRecord}).


-spec get(id()) -> {ok, doc()} | {error, term()}.
get(AtmStoreId) ->
    datastore_model:get(?CTX, AtmStoreId).


-spec update(id(), diff()) -> ok | {error, term()}.
update(AtmStoreId, UpdateFun) ->
    ?extract_ok(datastore_model:update(?CTX, AtmStoreId, UpdateFun)).


-spec delete(id()) -> ok | {error, term()}.
delete(AtmStoreId) ->
    datastore_model:delete(?CTX, AtmStoreId).


%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================


-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.


-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    1.


-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {workflow_execution_id, string},
        {schema_id, string},
        {initial_content, {custom, string, {?MODULE, encode_initial_content, decode_initial_content}}},
        {frozen, boolean},
        {container, {custom, string, {persistent_record, encode, decode, atm_store_container}}}
    ]}.


-spec encode_initial_content(undefined | json_utils:json_term()) -> binary().
encode_initial_content(Value) ->
    json_utils:encode(utils:undefined_to_null(Value)).


-spec decode_initial_content(binary()) -> undefined | json_utils:json_term().
decode_initial_content(Value) ->
    utils:null_to_undefined(json_utils:decode(Value)).
