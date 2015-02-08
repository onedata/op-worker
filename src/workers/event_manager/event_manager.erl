%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% 
%%% @end
%%%-------------------------------------------------------------------
-module(event_manager).
-author("Krzysztof Trzepla").

-include("workers/event_manager/events.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([emit/1, subscribe/1]).

-define(SUBSCRIPTION_ID_LENGTH, 16).
-define(EVENT_MANAGER_WORKER, event_manager_worker).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Emits an event.
%% @end
%%--------------------------------------------------------------------
-spec emit(Event :: event()) -> ok.
emit(Event) ->
    worker_proxy:cast(?EVENT_MANAGER_WORKER, Event).

%%--------------------------------------------------------------------
%% @doc
%% Subscribes for events.
%% @end
%%--------------------------------------------------------------------
-spec subscribe(Subscription :: event_subscription()) ->
    {ok, SubscriptionId :: binary()} | {error, Reason :: term()}.
subscribe(Subscription) ->
    worker_proxy:multicall(?EVENT_MANAGER_WORKER, Subscription).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Generates random subscription ID.
%% @end
%%--------------------------------------------------------------------
-spec generate_subscription_id() -> SubscriptionId :: binary().
generate_subscription_id() ->
    base64:encode(crypto:rand_bytes(?SUBSCRIPTION_ID_LENGTH)).