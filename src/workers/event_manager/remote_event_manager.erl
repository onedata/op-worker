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
-module(remote_event_manager).
-author("Krzysztof Trzepla").

-include("workers/event_manager/events.hrl").

%% API
-export([subscribe/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Subscribes for events by sending subscription request to given producers.
%% Returns unique subscription ID that can be used to cancel subscription.
%% @end
%%--------------------------------------------------------------------
-spec subscribe(Subscription :: event_subscription()) ->
    {ok, SubscriptionId :: binary()} | {error, Reason :: term()}.
subscribe(_Subscription) ->
    try
        %todo send subscription request
        {ok, undefined}
    catch
        error:{badmatch, Reason} -> Reason
    end.