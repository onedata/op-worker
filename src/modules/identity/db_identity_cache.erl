%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C): 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module manages identity data in DHT.
%%% @end
%%%-------------------------------------------------------------------
-module(db_identity_cache).
-author("Michal Zmuda").

-behaviour(identity_cache_behaviour).

-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("public_key/include/public_key.hrl").

-export([put/2, get/1, invalidate/1]).

%%--------------------------------------------------------------------
%% @doc
%% Cached public key under given ID.
%% @end
%%--------------------------------------------------------------------
-spec put(identity:id(), identity:public_key()) -> ok.
put(ID, Key) ->
    cache(ID, Key).

%%--------------------------------------------------------------------
%% @doc
%% Determines cached public key for given ID.
%% @end
%%--------------------------------------------------------------------
-spec get(identity:id()) ->
    {ok, identity:public_key()} | {error, Reason :: term()}.
get(ID) ->
    case identity_cache:get(ID) of
        {ok, #document{value = #identity_cache{public_key = Key}}} ->
            {ok, Key};
        {error, {not_found, identity_cache}} ->
            {error, not_found};
        {error, Reason} ->
            {error, {db_error, Reason}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Ensures public key for given iD is not cached.
%% @end
%%--------------------------------------------------------------------
-spec invalidate(identity:id()) -> ok | {error, Reason :: term()}.
invalidate(ID) ->
    case identity_cache:delete(ID) of
        {error, Reason} -> {error, {db_error, Reason}};
        ok -> ok
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec cache(identity:id(), identity:public_key()) -> ok.
cache(ID, PublicKey) ->
    SystemTime = erlang:system_time(seconds),
    Result = identity_cache:create_or_update(#document{
        key = ID, value = #identity_cache{
            last_update_seconds = SystemTime,
            public_key = PublicKey,
            id = ID
        }}, fun(Current) ->
        {ok, Current#identity_cache{
            last_update_seconds = SystemTime,
            public_key = PublicKey
        }}
    end),

    case Result of
        {ok, _} -> ok;
        {error, Reason} -> ?warning("Unable to cache entry for ~p due to ~p", [ID, Reason])
    end.