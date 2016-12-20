%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for creating default event stream definitions
%%% based on the subscription type.
%%% @end
%%%-------------------------------------------------------------------
-module(event_stream_factory).
-author("Krzysztof Trzepla").

-include("modules/events/definitions.hrl").
-include("proto/oneclient/server_messages.hrl").

%% API
-export([create/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates default event stream definition based on the provided subscription.
%% @end
%%--------------------------------------------------------------------
-spec create(Sub :: subscription:base() | subscription:type()) ->
    EvtStm :: event:stream().
create(#subscription{type = Type}) ->
    create(Type);

create(#file_read_subscription{time_threshold = TimeThr}) ->
    #event_stream{
        aggregation_rule = fun event_utils:aggregate_file_read_events/2,
        emission_time = make_emission_time(TimeThr),
        emission_rule = fun(_) -> false end,
        event_handler = fun fslogic_event:handle_file_read_events/2
    };

create(#file_written_subscription{time_threshold = TimeThr}) ->
    #event_stream{
        aggregation_rule = fun event_utils:aggregate_file_written_events/2,
        emission_time = make_emission_time(TimeThr),
        emission_rule = fun(_) -> false end,
        event_handler = fun fslogic_event:handle_file_written_events/2
    };

create(#file_attr_changed_subscription{} = Sub) ->
    #file_attr_changed_subscription{
        counter_threshold = CtrThr,
        time_threshold = TimeThr
    } = Sub,
    #event_stream{
        aggregation_rule = fun(OldEvent, NewEvent) ->
            OldAttr = OldEvent#file_attr_changed_event.file_attr,
            NewAttr = NewEvent#file_attr_changed_event.file_attr,
            OldEvent#file_attr_changed_event{
                file_attr = NewAttr#file_attr{
                    size = case NewAttr#file_attr.size of
                        undefined -> OldAttr#file_attr.size;
                        X -> X
                    end
                }
            }
        end,
        emission_rule = make_counter_emission_rule(CtrThr),
        emission_time = make_emission_time(TimeThr),
        event_handler = make_send_events_handler()
    };

create(#file_location_changed_subscription{} = Sub) ->
    #file_location_changed_subscription{
        counter_threshold = CtrThr,
        time_threshold = TimeThr
    } = Sub,
    #event_stream{
        emission_rule = make_counter_emission_rule(CtrThr),
        emission_time = make_emission_time(TimeThr),
        event_handler = make_send_events_handler()
    };

create(#file_perm_changed_subscription{}) ->
    #event_stream{
        event_handler = make_send_events_handler()
    };

create(#file_removed_subscription{}) ->
    #event_stream{
        event_handler = make_send_events_handler()
    };

create(#file_renamed_subscription{}) ->
    #event_stream{
        event_handler = make_send_events_handler()
    };

create(#quota_exceeded_subscription{}) ->
    #event_stream{
        event_handler = make_send_events_handler()
    };

create(#monitoring_subscription{time_threshold = TimeThr}) ->
    #event_stream{
        event_handler = fun monitoring_event:handle_monitoring_events/2,
        aggregation_rule = fun monitoring_event:aggregate_monitoring_events/2,
        emission_rule = fun(_) -> false end,
        emission_time = make_emission_time(TimeThr)
    }.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private @doc
%% Returns emission rule based on the counter threshold.
%% @end
%%--------------------------------------------------------------------
-spec make_counter_emission_rule(CtrThr :: undefined | non_neg_integer()) ->
    Rule :: event_stream:emission_rule().
make_counter_emission_rule(undefined) ->
    fun(_) -> false end;
make_counter_emission_rule(CtrThr) when is_integer(CtrThr) ->
    fun(Meta) -> Meta >= CtrThr end.

%%--------------------------------------------------------------------
%% @private @doc
%% Returns emission time based on the time threshold.
%% @end
%%--------------------------------------------------------------------
-spec make_emission_time(TimeThr :: undefined | non_neg_integer()) ->
    Time :: event_stream:emission_time().
make_emission_time(undefined) ->
    infinity;
make_emission_time(TimeThr) when is_integer(TimeThr) ->
    TimeThr.

%%--------------------------------------------------------------------
%% @private @doc
%% Returns handler which sends events to the remote subscriber via sequencer stream.
%% @end
%%--------------------------------------------------------------------
-spec make_send_events_handler() -> Handler :: event_stream:event_handler().
make_send_events_handler() ->
    fun
        ([], _) -> ok;
        (Evts, #{session_id := SessId}) ->
            communicator:send(#server_message{message_body = #events{
                events = [#event{type = Evt} || Evt <- Evts]
            }}, SessId)
    end.