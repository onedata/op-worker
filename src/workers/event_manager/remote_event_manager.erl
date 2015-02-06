
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

-include("registered_names.hrl").
-include("workers/event_manager/events.hrl").
-include("proto/oneclient/event_messages.hrl").

%% API
-export([subscribe/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Subscribes for events by sending subscription request to given producers.
%% Returns unique subscription ID that can be used to cancel subscription.
%% @end
%%--------------------------------------------------------------------
-spec subscribe(SubscriptionId :: binary(), Subscription :: event_subscription()) ->
    {ok, SubscriptionId :: binary()} | {error, Reason :: term()}.
subscribe(SubscriptionId, #read_event_subscription{} = Subscription) ->
    try
        {ok, _Request} = create_subscription_request(SubscriptionId, Subscription),
        {ok, SubscriptionId}
    catch
        error:{badmatch, Reason} -> Reason
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates subscription request.
%% @end
%%--------------------------------------------------------------------
-spec create_subscription_request(SubscriptionId :: binary(),
    Subscription :: event_subscription()) ->
    {ok, Request :: event_request()} | {error, Reason :: term()}.
create_subscription_request(SubscriptionId,
    #read_event_subscription{} = Subscription) ->
    {ok, #'ReadEventSubscription'{
        id = SubscriptionId,
        counter_threshold = set_parameter(
            Subscription#read_event_subscription.producer_counter_threshold,
            Subscription#read_event_subscription.subscriber_counter_threshold
        ),
        time_threshold = set_parameter(
            Subscription#read_event_subscription.producer_time_threshold,
            Subscription#read_event_subscription.subscriber_time_threshold
        ),
        size_threshold = set_parameter(
            Subscription#read_event_subscription.producer_size_threshold,
            Subscription#read_event_subscription.subscriber_size_threshold
        )
    }};

create_subscription_request(SubscriptionId,
    #write_event_subscription{} = Subscription) ->
    {ok, #'WriteEventSubscription'{
        id = SubscriptionId,
        counter_threshold = set_parameter(
            Subscription#write_event_subscription.producer_counter_threshold,
            Subscription#write_event_subscription.subscriber_counter_threshold
        ),
        time_threshold = set_parameter(
            Subscription#write_event_subscription.producer_time_threshold,
            Subscription#write_event_subscription.subscriber_time_threshold
        ),
        size_threshold = set_parameter(
            Subscription#write_event_subscription.producer_size_threshold,
            Subscription#write_event_subscription.subscriber_size_threshold
        )
    }};

create_subscription_request(_, Subscription) ->
    {error, {unknown_subscription, Subscription}}.

%%--------------------------------------------------------------------
%% @doc
%% Returns producer paramter if present or modified subscriber parameter.
%% If parameters are not provided returns 'undefined'.
%% @end
%%--------------------------------------------------------------------
-spec set_parameter(ProducerParameter, SubscriberParameter) -> Parameter when
    Parameter :: undefined | non_neg_integer(),
    ProducerParameter :: undefined | non_neg_integer(),
    SubscriberParameter :: undefined | non_neg_integer().
set_parameter(undefined, undefined) ->
    undefined;

set_parameter(undefined, SubscriberParameter) ->
    {ok, Ratio} = application:get_env(?APP_NAME, producer_threshold_ratio),
    SubscriberParameter / Ratio;

set_parameter(ProducerParameter, _) ->
    ProducerParameter.