%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains utility functions for events management.
%%% @end
%%%-------------------------------------------------------------------
-module(event_utils).
-author("Krzysztof Trzepla").

-include("modules/events/definitions.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([inject_event_stream_definition/1, send_events_handler/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Injects event stream definition based on the subscription type.
%% @end
%%--------------------------------------------------------------------
-spec inject_event_stream_definition(Sub :: event:subscription()) ->
    NewSub :: event:subscription().
inject_event_stream_definition(#subscription{object = #file_attr_subscription{
    counter_threshold = CtrThr, time_threshold = TimeThr}} = Sub) ->
    Sub#subscription{event_stream = ?FILE_ATTR_EVENT_STREAM#event_stream_definition{
        emission_rule = emission_rule_from_counter_threshold(CtrThr),
        emission_time = emission_time_from_time_threshold(TimeThr)
    }};

inject_event_stream_definition(#subscription{object = #file_location_subscription{
    counter_threshold = CtrThr, time_threshold = TimeThr}} = Sub) ->
    Sub#subscription{event_stream = ?FILE_LOCATION_EVENT_STREAM#event_stream_definition{
        emission_rule = emission_rule_from_counter_threshold(CtrThr),
        emission_time = emission_time_from_time_threshold(TimeThr)
    }};

inject_event_stream_definition(#subscription{object = #permission_changed_subscription{}} = Sub) ->
    Sub#subscription{event_stream = ?PERMISSION_CHANGED_EVENT_STREAM};

inject_event_stream_definition(#subscription{object = #file_removal_subscription{}} = Sub) ->
    Sub#subscription{event_stream = ?FILE_REMOVAL_EVENT_STREAM};

inject_event_stream_definition(#subscription{object = #quota_subscription{}} = Sub) ->
    Sub#subscription{event_stream = ?QUOTA_EXCEEDED_EVENT_STREAM#event_stream_definition{
        emission_time = 200
    }};

inject_event_stream_definition(#subscription{object = #file_renamed_subscription{}} = Sub) ->
    Sub#subscription{event_stream = ?FILE_RENAMED_EVENT_STREAM};

inject_event_stream_definition(#subscription{object = #file_accessed_subscription{}} = Sub) ->
    Sub#subscription{event_stream = ?FILE_ACCESSED_EVENT_STREAM}.

%%--------------------------------------------------------------------
%% @doc
%% Returns handler which sends events to the remote subscriber via sequencer stream.
%% @end
%%--------------------------------------------------------------------
-spec send_events_handler() -> Handler :: event_stream:event_handler().
send_events_handler() ->
    fun
        ([], _) -> ok;
        (Events, #{session_id := SessId}) ->
            communicator:send(#server_message{
                message_body = #events{events = Events}
            }, SessId);
        (_, _) -> ok
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns emission rule based on the counter threshold.
%% @end
%%--------------------------------------------------------------------
-spec emission_rule_from_counter_threshold(CtrThr :: undefined | non_neg_integer()) ->
    Rule :: event_stream:emission_rule().
emission_rule_from_counter_threshold(undefined) ->
    fun(_) -> false end;
emission_rule_from_counter_threshold(CtrThr) when is_integer(CtrThr) ->
    fun(Meta) -> Meta >= CtrThr end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns emission time based on the time threshold.
%% @end
%%--------------------------------------------------------------------
-spec emission_time_from_time_threshold(TimeThr :: undefined | non_neg_integer()) ->
    Time :: event_stream:emission_time().
emission_time_from_time_threshold(undefined) ->
    infinity;
emission_time_from_time_threshold(TimeThr) when is_integer(TimeThr) ->
    TimeThr.