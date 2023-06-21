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

-include("modules/automation/atm_execution.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").

%% API
-export([type_to_json/1, type_from_json/1]).
-export([config_to_json/1]).
-export([create/1, get/1, update/2, delete/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0]).
-export([encode_initial_content/1, decode_initial_content/1]).


-type id() :: binary().
-type record() :: #atm_store{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).

-type type() ::
    automation:store_type() |
    % types specific to op and as such not available in schema
    exception.

-type config() ::
    atm_store_config:record() |
    % configs for stores specific to op and not available in schema
    #atm_exception_store_config{}.

-type content_update_options() ::
    atm_store_content_update_options:record() |
    % options for stores specific to op and not available in schema
    #atm_exception_store_content_update_options{}.

% automation:item converted (if needed) to a format for persisting in a store
-type item() :: json_utils:json_term().

-export_type([id/0, record/0, doc/0, diff/0]).
-export_type([type/0, config/0, content_update_options/0, item/0]).

-define(CTX, #{model => ?MODULE}).


%%%===================================================================
%%% API
%%%===================================================================


-spec type_to_json(type()) -> json_utils:json_term().
type_to_json(exception) -> <<"exception">>;
type_to_json(AtmStoreType) -> automation:store_type_to_json(AtmStoreType).


-spec type_from_json(json_utils:json_term()) -> type().
type_from_json(<<"exception">>) -> exception;
type_from_json(AtmStoreTypeJson) -> automation:store_type_from_json(AtmStoreTypeJson).


-spec config_to_json(config()) -> json_utils:json_term().
config_to_json(Record = #atm_exception_store_config{}) ->
    RecordType = utils:record_type(Record),
    RecordType:to_json(Record).


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
