%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(translator).
-author("Tomasz Lichon").

-include("proto/oneclient/messages.hrl").
-include("proto_internal/oneclient/common_messages.hrl").
-include("proto_internal/oneclient/handshake_messages.hrl").
-include("workers/event_manager/event_stream.hrl").
-include("workers/event_manager/events.hrl").
-include("workers/event_manager/read_event.hrl").
-include("workers/event_manager/write_event.hrl").
-include("registered_names.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([translate/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec translate(tuple()) -> tuple().
translate(#'FileBlock'{offset = Off, size = S}) ->
    #file_block{offset = Off, size = S};
translate(#file_block{offset = Off, size = S}) ->
    #'FileBlock'{offset = Off, size = S};

translate(#'EnvironmentVariable'{name = Name, value = Val}) ->
    #environment_variable{name = Name, value = Val};
translate(#environment_variable{name = Name, value = Val}) ->
    #'EnvironmentVariable'{name = Name, value = Val};

translate(#'Status'{code = Code, description = Desc}) ->
    #status{code = Code, description = Desc};
translate(#status{code = Code, description = Desc}) ->
    #'Status'{code = Code, description = Desc};

translate(#read_event{} = Record) ->
    #'ReadEvent'{
        counter = Record#read_event.counter,
        file_id = Record#read_event.file_id,
        size = Record#read_event.size,
        blocks = Record#read_event.blocks
    };
translate(#'ReadEvent'{} = Record) ->
    {ok, #read_event{
        counter = Record#'ReadEvent'.counter,
        file_id = Record#'ReadEvent'.file_id,
        size = Record#'ReadEvent'.size,
        blocks = Record#'ReadEvent'.blocks
    }};

translate(#read_event_subscription{} = Subscription) ->
    #'ReadEventSubscription'{
        id = Subscription#read_event_subscription.subscription_id,
        counter_threshold = set_event_threshold_parameter(
            Subscription#read_event_subscription.producer_counter_threshold,
            Subscription#read_event_subscription.subscriber_counter_threshold
        ),
        time_threshold = set_event_threshold_parameter(
            Subscription#read_event_subscription.producer_time_threshold,
            Subscription#read_event_subscription.subscriber_time_threshold
        ),
        size_threshold = set_event_threshold_parameter(
            Subscription#read_event_subscription.producer_size_threshold,
            Subscription#read_event_subscription.subscriber_size_threshold
        )
    };

translate(#write_event{} = Record) ->
    #'WriteEvent'{
        counter = Record#write_event.counter,
        file_id = Record#write_event.file_id,
        size = Record#write_event.size,
        blocks = Record#write_event.blocks
    };
translate(#'WriteEvent'{} = Record) ->
    #write_event{
        counter = Record#'WriteEvent'.counter,
        file_id = Record#'WriteEvent'.file_id,
        size = Record#'WriteEvent'.size,
        blocks = Record#'WriteEvent'.blocks
    };

translate(#write_event_subscription{} = Subscription) ->
    #'WriteEventSubscription'{
        counter_threshold = set_event_threshold_parameter(
            Subscription#write_event_subscription.producer_counter_threshold,
            Subscription#write_event_subscription.subscriber_counter_threshold
        ),
        time_threshold = set_event_threshold_parameter(
            Subscription#write_event_subscription.producer_time_threshold,
            Subscription#write_event_subscription.subscriber_time_threshold
        ),
        size_threshold = set_event_threshold_parameter(
            Subscription#write_event_subscription.producer_size_threshold,
            Subscription#write_event_subscription.subscriber_size_threshold
        )
    };

translate(#'HandshakeRequest'{environment_variables = Envs}) ->
    #handshake_request{environment_variables = Envs};
translate(#handshake_request{environment_variables = Envs}) ->
    #'HandshakeRequest'{environment_variables = Envs};

translate(#'HandshakeResponse'{fuse_id = Id}) ->
    #handshake_response{fuse_id = Id};
translate(#handshake_response{fuse_id = Id}) ->
    #'HandshakeResponse'{fuse_id = Id};

translate(#'HandshakeAcknowledgement'{fuse_id = Id}) ->
    #handshake_acknowledgement{fuse_id = Id};
translate(#handshake_acknowledgement{fuse_id = Id}) ->
    #'HandshakeAcknowledgement'{fuse_id = Id};

translate(Record) ->
    ?error("~p:~p - bad record ~p", [?MODULE, ?LINE, Record]),
    {error, {unknown_record, Record}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns producer paramter if present or modified subscriber parameter.
%% If parameters are not provided returns 'undefined'.
%% @end
%%--------------------------------------------------------------------
-spec set_event_threshold_parameter(ProducerParameter, SubscriberParameter) -> Parameter when
    Parameter :: undefined | non_neg_integer(),
    ProducerParameter :: undefined | non_neg_integer(),
    SubscriberParameter :: undefined | non_neg_integer().
set_event_threshold_parameter(undefined, undefined) ->
    undefined;

set_event_threshold_parameter(undefined, SubscriberParameter) ->
    {ok, Ratio} = application:get_env(?APP_NAME, producer_threshold_ratio),
    SubscriberParameter / Ratio;

set_event_threshold_parameter(ProducerParameter, _) ->
    ProducerParameter.
