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
-module(caching_oz_identity_repository).
-author("Michal Zmuda").

-behaviour(identity_repository_behaviour).

-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("public_key/include/public_key.hrl").

-export([publish/2, get/1]).

%%--------------------------------------------------------------------
%% @doc
%% Publishes public key under given ID.
%% @end
%%--------------------------------------------------------------------
-spec publish(identity:id(), identity:public_key()) ->
    ok | {error, Reason :: term()}.
publish(ID, PublicKey) ->
    case oz_identity_repository:publish(ID, PublicKey) of
        ok -> cache(ID, PublicKey), ok;
        _Error -> _Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Determines public key for given ID.
%% @end
%%--------------------------------------------------------------------
-spec get(identity:id()) ->
    {ok, identity:public_key()} | {error, Reason :: term()}.
get(ID) ->
    case identity_cache:get(ID) of
        {ok, #document{value = #identity_cache{public_key = PublicKey}}} ->
            {ok, PublicKey};
        {error, {not_found, _}} ->
            case oz_identity_repository:get(ID) of
                {ok, PublicKey} -> cache(ID, PublicKey), {ok, PublicKey};
                _Error -> _Error
            end;
        _Error -> _Error
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