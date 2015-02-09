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
-include("proto_internal/oneclient/read_event.hrl").
-include("proto_internal/oneclient/write_event.hrl").
-include("proto_internal/oneclient/event_messages.hrl").
-include("registered_names.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([translate_from_protobuf/1, translate_to_protobuf/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% traslate protobuf record to internal record
%% @end
%%--------------------------------------------------------------------
-spec translate_from_protobuf(tuple()) -> tuple().
translate_from_protobuf(#'FileBlock'{offset = Off, size = S}) ->
    #file_block{offset = Off, size = S};
translate_from_protobuf(#'EnvironmentVariable'{name = Name, value = Val}) ->
    #environment_variable{name = Name, value = Val};
translate_from_protobuf(#'Status'{code = Code, description = Desc}) ->
    #status{code = Code, description = Desc};
translate_from_protobuf(#'Event'{event = {_, Record}}) ->
    Record;
translate_from_protobuf(#'ReadEvent'{} = Record) ->
    {ok, #read_event{
        counter = Record#'ReadEvent'.counter,
        file_id = Record#'ReadEvent'.file_id,
        size = Record#'ReadEvent'.size,
        blocks = Record#'ReadEvent'.blocks
    }};
translate_from_protobuf(#'WriteEvent'{} = Record) ->
    #write_event{
        counter = Record#'WriteEvent'.counter,
        file_id = Record#'WriteEvent'.file_id,
        size = Record#'WriteEvent'.size,
        blocks = Record#'WriteEvent'.blocks
    };
translate_from_protobuf(#'HandshakeRequest'{environment_variables = Envs}) ->
    #handshake_request{environment_variables = Envs};
translate_from_protobuf(#'HandshakeResponse'{fuse_id = Id}) ->
    #handshake_response{fuse_id = Id};
translate_from_protobuf(#'HandshakeAcknowledgement'{fuse_id = Id}) ->
    #handshake_acknowledgement{fuse_id = Id};
translate_from_protobuf(Record) ->
    ?error("~p:~p - unknown record ~p", [?MODULE, ?LINE, Record]),
    throw({unknown_record, Record}).

%%--------------------------------------------------------------------
%% @doc
%% translate internal record to protobuf record
%% @end
%%--------------------------------------------------------------------
-spec translate_to_protobuf(tuple()) -> tuple().
translate_to_protobuf(#file_block{offset = Off, size = S}) ->
    #'FileBlock'{offset = Off, size = S};
translate_to_protobuf(#environment_variable{name = Name, value = Val}) ->
    #'EnvironmentVariable'{name = Name, value = Val};
translate_to_protobuf(#status{code = Code, description = Desc}) ->
    #'Status'{code = Code, description = Desc};
translate_to_protobuf(#read_event{} = Record) ->
    #'Event'{event =
    {read_event,
        #'ReadEvent'{
            counter = Record#read_event.counter,
            file_id = Record#read_event.file_id,
            size = Record#read_event.size,
            blocks = Record#read_event.blocks
        }}
    };
translate_to_protobuf(#write_event{} = Record) ->
    #'Event'{event =
    {write_event,
        #'WriteEvent'{
            counter = Record#write_event.counter,
            file_id = Record#write_event.file_id,
            size = Record#write_event.size,
            blocks = Record#write_event.blocks
        }}
    };
translate_to_protobuf(#event_subscription_cancellation{id = Id}) ->
    #'EventSubscription'{
        event_subscription = {event_subscription_cancellation,
            #'EventSubscriptionCancellation'{
                id = Id
            }
        }
    };
translate_to_protobuf(#read_event_subscription{} = Subscription) ->
    #'EventSubscription'{
        event_subscription = {read_event_subscription,
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
            }
        }
    };
translate_to_protobuf(#write_event_subscription{} = Subscription) ->
    #'EventSubscription'{
        event_subscription = {write_event_subscription,
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
            }
        }};
translate_to_protobuf(#handshake_request{environment_variables = Envs}) ->
    #'HandshakeRequest'{environment_variables = Envs};
translate_to_protobuf(#handshake_response{fuse_id = Id}) ->
    #'HandshakeResponse'{fuse_id = Id};
translate_to_protobuf(#handshake_acknowledgement{fuse_id = Id}) ->
    #'HandshakeAcknowledgement'{fuse_id = Id};
translate_to_protobuf(Record) ->
    ?error("~p:~p - unknown record ~p", [?MODULE, ?LINE, Record]),
    throw({unknown_record, Record}).

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
