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
-module(oz_identity_repository).
-author("Michal Zmuda").

-behaviour(identity_repository_behaviour).

-include_lib("ctool/include/logging.hrl").
-include_lib("public_key/include/public_key.hrl").

-define(DHT_DATA_KEY, <<"public_key">>).
-define(TYPE, identity).


-export([publish/2, get/1]).

%%--------------------------------------------------------------------
%% @doc
%% Publishes public key under given ID.
%% @end
%%--------------------------------------------------------------------
-spec publish(identity:id(), identity:public_key()) ->
    ok | {error, Reason :: term()}.
publish(ID, PublicKey) ->
    oz_identities:set_public_key(provider, ID, PublicKey).

%%--------------------------------------------------------------------
%% @doc
%% Determines public key for given ID.
%% @end
%%--------------------------------------------------------------------
-spec get(identity:id()) ->
    {ok, identity:public_key()} | {error, Reason :: term()}.
get(ID) ->
    oz_identities:get_public_key(provider, ID).