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

%% API
-export([emit/1, subscribe/1]).

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
    worker_proxy:cast(?EVENT_MANAGER_WORKER, {event, Event}).

%%--------------------------------------------------------------------
%% @doc
%% Subscribes for events.
%% @end
%%--------------------------------------------------------------------
-spec subscribe(Subscription :: event_subscription()) ->
    ok | {error, Reason :: term()}.
subscribe(Subscription) ->
    worker_proxy:call(?EVENT_MANAGER_WORKER, {subscription, Subscription}).

%% handle(Message) ->
%%     case event_translator

%%%===================================================================
%%% Internal functions
%%%===================================================================