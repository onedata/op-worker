%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility module for creation of ets based state of the process
%%% used by other cooperating processes.
%%% @end
%%%-------------------------------------------------------------------
-module(ets_state).
-author("Michal Wrzeszcz").

%% API
-export([init/1, terminate/1, save/4, get/3, delete/3]).
-export([init_collection/2, delete_collection/2, get_collection/3,
    add_to_collection/4, remove_from_collection/3,
    get_from_collection/3, get_from_collection/4]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Initializes state table.
%% @end
%%--------------------------------------------------------------------
-spec init(TableIdentifier :: atom()) -> ets:tid().
init(TableIdentifier) ->
    ets:new(TableIdentifier, [set, public, named_table]).

%%--------------------------------------------------------------------
%% @doc
%% Terminates state table.
%% @end
%%--------------------------------------------------------------------
-spec terminate(TableIdentifier :: atom()) -> ok.
terminate(TableIdentifier) ->
    true = ets:delete(TableIdentifier),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Saves value in state.
%% @end
%%--------------------------------------------------------------------
-spec save(TableIdentifier :: atom(), Owner :: pid(),
    Key :: term(), Value ::  term()) -> ok.
save(TableIdentifier, Owner, Key, Value) ->
    true = ets:insert(TableIdentifier, {{Owner, Key}, Value}),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Gets value from state.
%% @end
%%--------------------------------------------------------------------
-spec get(TableIdentifier :: atom(), Owner :: pid(), Key :: term()) ->
    {ok, Value :: term()} | error.
get(TableIdentifier, Owner, Key) ->
    case rpc:call(node(Owner), ets, lookup, [TableIdentifier, {Owner, Key}]) of
        [{_, Value}] -> {ok, Value};
        _ -> error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Deletes value from state.
%% @end
%%--------------------------------------------------------------------
-spec delete(TableIdentifier :: atom(), Owner :: pid(), Key :: term()) -> ok.
delete(TableIdentifier, Owner, Key) ->
    true = ets:delete(TableIdentifier, {Owner, Key}),
    ok.

%%%===================================================================
%%% Collection API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Initializes collection stored in state.
%% @end
%%--------------------------------------------------------------------
-spec init_collection(TableIdentifier :: atom(), Collection :: atom()) -> ok.
init_collection(TableIdentifier, Collection) ->
    ets_state:save(TableIdentifier, self(), Collection, #{}).

%%--------------------------------------------------------------------
%% @doc
%% Deletes collection stored in state.
%% @end
%%--------------------------------------------------------------------
-spec delete_collection(TableIdentifier :: atom(), Collection :: atom()) -> ok.
delete_collection(TableIdentifier, Collection) ->
    Owner = self(),
    {ok, CollectionMap} = get(TableIdentifier, Owner, Collection),
    maps:map(fun(Key, _) ->
        delete(TableIdentifier, Owner, {Collection, Key})
    end, CollectionMap),
    delete(TableIdentifier, Owner, Collection).

%%--------------------------------------------------------------------
%% @doc
%% Adds value to collection stored in state.
%% @end
%%--------------------------------------------------------------------
-spec add_to_collection(TableIdentifier :: atom(), Collection :: atom(),
    Key :: term(), Value ::  term()) -> ok.
add_to_collection(TableIdentifier, Collection, Key, Value) ->
    Owner = self(),
    {ok, CollectionMap} = get(TableIdentifier, Owner, Collection),
    save(TableIdentifier, Owner, {Collection, Key}, Value),
    save(TableIdentifier, Owner, Collection, maps:put(Key, Value, CollectionMap)).

%%--------------------------------------------------------------------
%% @doc
%% Removes value from collection stored in state.
%% @end
%%--------------------------------------------------------------------
-spec remove_from_collection(TableIdentifier :: atom(), Collection :: atom(),
    Key :: term()) -> ok.
remove_from_collection(TableIdentifier, Collection, Key) ->
    Owner = self(),
    {ok, CollectionMap} = get(TableIdentifier, Owner, Collection),
    delete(TableIdentifier, Owner, {Collection, Key}),
    save(TableIdentifier, Owner, Collection, maps:remove(Key, CollectionMap)).

%%--------------------------------------------------------------------
%% @doc
%% Gets collection stored in state.
%% @end
%%--------------------------------------------------------------------
-spec get_collection(TableIdentifier :: atom(), Owner :: pid(),
    Collection :: atom()) -> {ok, Collection :: map()} | error.
get_collection(TableIdentifier, Owner, Collection) ->
    get(TableIdentifier, Owner, Collection).

%%--------------------------------------------------------------------
%% @doc
%% Gets value from collection stored in state.
%% @end
%%--------------------------------------------------------------------
-spec get_from_collection(TableIdentifier :: atom(), Collection :: atom(),
    Key :: term()) -> {ok, Value :: term()} | error.
get_from_collection(TableIdentifier, Collection, Key) ->
    get(TableIdentifier, self(), {Collection, Key}).

%%--------------------------------------------------------------------
%% @doc
%% Gets value from collection stored in state.
%% @end
%%--------------------------------------------------------------------
-spec get_from_collection(TableIdentifier :: atom(), Owner :: pid(),
    Collection :: atom(), Key :: term()) -> {ok, Value :: term()} | error.
get_from_collection(TableIdentifier, Owner, Collection, Key) ->
    get(TableIdentifier, Owner, {Collection, Key}).