%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module with the persistent list store backend implemented using infinite log.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_infinite_log_backend).
-author("Michal Stanisz").

-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([create/1, append/2, destroy/1, list/2, extract_listed_entry/1]).

%% datastore_model callbacks
-export([get_ctx/0]).

-type id() :: datastore_infinite_log:key().

-export_type([id/0]).


-define(CTX, #{model => ?MODULE}).


%%%===================================================================
%%% API
%%%===================================================================

-spec create(infinite_log:log_opts()) -> {ok, id()}.
create(Opts) ->
    Id = datastore_key:new(),
    ok = datastore_infinite_log:create(?CTX, Id, Opts),
    {ok, Id}.


-spec append(id(), atm_value:compressed()) -> ok.
append(Id, Item) ->
    datastore_infinite_log:append(?CTX, Id, json_utils:encode(Item)).


-spec destroy(id()) -> ok | {error, term()}.
destroy(Id) ->
    datastore_infinite_log:destroy(?CTX, Id).


-spec list(id(), infinite_log_browser:listing_opts()) ->
    {ok, {done | more, [{atm_store_api:index(), atm_value:compressed(), time:millis()}]}} | {error, term()}.
list(Id, Opts) ->
    case datastore_infinite_log:list(?CTX, Id, Opts) of
        {ok, {Marker, EntrySeries}} ->
            {ok, {Marker, lists:map(fun extract_listed_entry/1, EntrySeries)}};
        {error, _} = Error ->
            Error
    end.
    

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec extract_listed_entry({infinite_log:entry_index(), infinite_log:entry()}) ->
    {atm_store_api:index(), atm_value:compressed(), time:millis()}.
extract_listed_entry({EntryIndex, {Timestamp, Value}}) ->
    CompressedValue = json_utils:decode(Value),
    {integer_to_binary(EntryIndex), CompressedValue, Timestamp}.