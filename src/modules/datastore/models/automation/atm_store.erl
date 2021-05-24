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
-include_lib("ctool/include/errors.hrl").

%% API
-export([
    create/1,
    get/1,
    delete/1
]).
%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0]).


-type id() :: binary().
-type record() :: #atm_store{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).

-type type() :: atom().
-type name() :: binary().
-type summary() :: binary().
-type description() :: binary().

-export_type([
    id/0, record/0, doc/0, diff/0,
    type/0, name/0, summary/0, description/0
]).

-type error() :: {error, term()}.


-define(CTX, #{model => ?MODULE}).


%%%===================================================================
%%% API
%%%===================================================================


-spec create(record()) -> {ok, id()} | error().
create(AtmStoreRecord) ->
    ?extract_key(datastore_model:create(?CTX, #document{
        value = AtmStoreRecord
    })).


-spec get(id()) -> {ok, record()} | ?ERROR_NOT_FOUND.
get(AtmStoreId) ->
    case datastore_model:get(?CTX, AtmStoreId) of
        {ok, #document{value = AtmStoreRecord}} ->
            {ok, AtmStoreRecord};
        ?ERROR_NOT_FOUND = Error ->
            Error
    end.


-spec delete(id()) -> ok | error().
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
        {type, atom},
        {name, string},
        {summary, string},
        {description, string},
        {frozen, boolean},
        {is_input_store, boolean},
        {container, {custom, string, {atm_container, encode, decode}}}
    ]}.
