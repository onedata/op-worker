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

%% API
-export([send_subscription_handler/0, send_subscription_cancellation_handler/0,
    inject_event_stream_definition/1]).

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
    file_uuid = FileUuid, counter_threshold = CtrThr,
    time_threshold = TimeThr}} = Sub) ->
    Sub#subscription{event_stream = ?FILE_ATTR_EVENT_STREAM#event_stream_definition{
        admission_rule = fun
            (#event{object = #update_event{object = #file_attr{uuid = Uuid}}})
                when Uuid =:= FileUuid -> true;
            (_) -> false
        end,
        emission_rule = emission_rule_from_counter_threshold(CtrThr),
        emission_time = emission_time_from_time_threshold(TimeThr),
        init_handler = open_sequencer_stream_handler(),
        terminate_handler = close_sequencer_stream_handler(),
        event_handler = send_events_handler()
    }};

inject_event_stream_definition(#subscription{object = #file_location_subscription{
    file_uuid = FileUuid, counter_threshold = CtrThr,
    time_threshold = TimeThr}} = Sub) ->
    Sub#subscription{event_stream = ?FILE_LOCATION_EVENT_STREAM#event_stream_definition{
        admission_rule = fun
            (#event{object = #update_event{object = #file_location{uuid = Uuid}}})
                when Uuid =:= FileUuid -> true;
            (_) -> false
        end,
        emission_rule = emission_rule_from_counter_threshold(CtrThr),
        emission_time = emission_time_from_time_threshold(TimeThr),
        init_handler = open_sequencer_stream_handler(),
        terminate_handler = close_sequencer_stream_handler(),
        event_handler = send_events_handler()
    }}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns handler which sends subscription to the remote client.
%% @end
%%--------------------------------------------------------------------
-spec send_subscription_handler() -> Handler :: event_stream:init_handler().
send_subscription_handler() ->
    fun(#subscription{id = SubId} = Sub, SessId) ->
        {ok, StmId} = sequencer:open_stream(SessId),
        sequencer:send_message(Sub, StmId, SessId),
        {SubId, StmId, SessId}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns handler which sends subscription cancellation to the remote client.
%% @end
%%--------------------------------------------------------------------
-spec send_subscription_cancellation_handler() ->
    Handler :: event_stream:terminate_handler().
send_subscription_cancellation_handler() ->
    fun({SubId, StmId, SessId}) ->
        sequencer:send_message(#subscription_cancellation{id = SubId}, StmId, SessId),
        sequencer:close_stream(StmId, SessId)
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns handler which opens sequencer stream.
%% @end
%%--------------------------------------------------------------------
-spec open_sequencer_stream_handler() -> Handler :: event_stream:init_handler().
open_sequencer_stream_handler() ->
    fun(_, SessId) ->
        {ok, StmId} = sequencer:open_stream(SessId),
        {StmId, SessId}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns handler which closes sequencer stream.
%% @end
%%--------------------------------------------------------------------
-spec close_sequencer_stream_handler() -> Handler :: event_stream:terminate_handler().
close_sequencer_stream_handler() ->
    fun({StmId, SessId}) ->
        sequencer:close_stream(StmId, SessId)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns handler which sends events to the remote subscriber via sequencer stream.
%% @end
%%--------------------------------------------------------------------
-spec send_events_handler() -> Handler :: event_stream:event_handler().
send_events_handler() ->
    fun(Evts, {StmId, SessId}) ->
        sequencer:send_message(#events{events = Evts}, StmId, SessId)
    end.

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