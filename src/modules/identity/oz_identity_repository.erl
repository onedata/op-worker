%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C): 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides access to identity repository through OZ api.
%%% The repository logic (like ownership protection) is implemented in OZ.
%%% @end
%%%-------------------------------------------------------------------
-module(oz_identity_repository).
-author("Michal Zmuda").

-behaviour(identity_repository_behaviour).

-include_lib("ctool/include/logging.hrl").
-include_lib("public_key/include/public_key.hrl").

-export([publish/2, get/1]).

-define(DHT_DATA_KEY, <<"public_key">>).
-define(TYPE, identity).

%%--------------------------------------------------------------------
%% @doc
%% {@link identity_repository_behaviour} callback publish/2.
%% @end
%%--------------------------------------------------------------------
-spec publish(identity:id(), identity:encoded_public_key()) ->
    ok | {error, Reason :: term()}.
publish(ID, PublicKey) ->
    oz_identities:set_public_key(provider, ID, PublicKey).

%%--------------------------------------------------------------------
%% @doc
%% {@link identity_repository_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(identity:id()) ->
    {ok, identity:encoded_public_key()} | {error, Reason :: term()}.
get(ID) ->
    oz_identities:get_public_key(provider, ID).