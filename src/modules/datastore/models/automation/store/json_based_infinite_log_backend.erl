%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Database interface for manipulating infinite logs with JSON entries.
%%% @end
%%%-------------------------------------------------------------------
-module(json_based_infinite_log_backend).
-author("Michal Stanisz").

-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([create/1, create/2]).
-export([destroy/1]).
-export([append/2]).
-export([list/2]).

%% datastore_model callbacks
-export([get_ctx/0]).

-type id() :: datastore_infinite_log:key().
-type entry_value() :: json_utils:json_map().

-export_type([id/0, entry_value/0]).


-define(CTX, #{model => ?MODULE}).


%%%===================================================================
%%% API
%%%===================================================================

-spec create(infinite_log:log_opts()) -> {ok, id()} | {error, term()}.
create(Opts) ->
    Id = datastore_key:new(),
    case create(Id, Opts) of
        ok ->
            {ok, Id};
        {error, _} = Error ->
            Error
    end.


-spec create(id(), infinite_log:log_opts()) -> ok | {error, term()}.
create(Id, Opts) ->
    datastore_infinite_log:create(?CTX, Id, Opts).


-spec destroy(id()) -> ok | {error, term()}.
destroy(Id) ->
    datastore_infinite_log:destroy(?CTX, Id).


-spec append(id(), entry_value()) -> ok | {error, term()}.
append(Id, EntryValue) ->
    datastore_infinite_log:append(?CTX, Id, json_utils:encode(EntryValue)).


-spec list(id(), infinite_log_browser:listing_opts()) ->
    {ok, {done | more, [{infinite_log:entry_index(), infinite_log:timestamp(), entry_value()}]}} | {error, term()}.
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
    {infinite_log:entry_index(), infinite_log:timestamp(), entry_value()}.
extract_listed_entry({EntryIndex, {Timestamp, Value}}) ->
    DecodedValue = json_utils:decode(Value),
    {EntryIndex, Timestamp, DecodedValue}.
