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
    file_uuid = FileUuid, counter_threshold = CtrThr, time_threshold = TimeThr}} = Sub) ->
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
    file_uuid = FileUuid, counter_threshold = CtrThr, time_threshold = TimeThr}} = Sub) ->
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
    }};

inject_event_stream_definition(#subscription{
    object = #permission_changed_subscription{file_uuid = FileUuid}} = Sub) ->
    Sub#subscription{event_stream = ?PERMISSION_CHANGED_EVENT_STREAM#event_stream_definition{
        admission_rule = fun
            (#event{object = #permission_changed_event{file_uuid = Uuid}})
                when Uuid =:= FileUuid -> true;
            (_) -> false
        end,
        init_handler = fun(_, SessId, _) -> #{session_id => SessId} end,
        event_handler = fun(Events, #{session_id := SessId}) ->
            communicator:send(#server_message{
                message_body = #events{events = Events}
            }, SessId)
        end
    }};

inject_event_stream_definition(#subscription{
    object = #file_removal_subscription{file_uuid = FileUuid}} = Sub) ->
    Sub#subscription{event_stream = ?FILE_REMOVAL_EVENT_STREAM#event_stream_definition{
        admission_rule = fun
            (#event{object = #file_removal_event{file_uuid = Uuid}})
                when Uuid =:= FileUuid -> true;
            (_) -> false
        end,
        init_handler = fun(_, SessId, _) -> #{session_id => SessId} end,
        event_handler = fun(Events, #{session_id := SessId}) ->
            communicator:send(#server_message{
                message_body = #events{events = Events}
            }, SessId)
        end
    }};

inject_event_stream_definition(#subscription{
    object = #file_renamed_subscription{file_uuid = FileUuid}} = Sub) ->
    Sub#subscription{event_stream = ?FILE_RENAMED_EVENT_STREAM#event_stream_definition{
        admission_rule = fun
            (#event{object = #file_renamed_event{top_entry = #file_renamed_entry{old_uuid = Uuid}}})
                when Uuid =:= FileUuid -> true;
            (_) -> false
        end,
        init_handler = fun(_, SessId, _) -> #{session_id => SessId} end,
        event_handler = fun(Events, #{session_id := SessId}) ->
            communicator:send(#server_message{
                message_body = #events{events = Events}
            }, SessId)
        end
    }}.

%%--------------------------------------------------------------------
%% @doc
%% Returns handler which sends subscription to the remote client.
%% @end
%%--------------------------------------------------------------------
-spec send_subscription_handler() -> Handler :: event_stream:init_handler().
send_subscription_handler() ->
    fun
        (#subscription{id = SubId} = Sub, SessId, fuse) ->
            {ok, StmId} = sequencer:open_stream(SessId),
            sequencer:send_message(Sub, StmId, SessId),
            #{subscription_id => SubId, stream_id => StmId, session_id => SessId};
        (#subscription{id = SubId}, SessId, _) ->
            #{subscription_id => SubId, session_id => SessId}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns handler which sends subscription cancellation to the remote client.
%% @end
%%--------------------------------------------------------------------
-spec send_subscription_cancellation_handler() ->
    Handler :: event_stream:terminate_handler().
send_subscription_cancellation_handler() ->
    fun
        (#{subscription_id := SubId, stream_id := StmId, session_id := SessId}) ->
            sequencer:send_message(#subscription_cancellation{id = SubId}, StmId, SessId),
            sequencer:close_stream(StmId, SessId);
        (_) ->
            ok
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
    fun
        (_, SessId, fuse) ->
            {ok, StmId} = sequencer:open_stream(SessId),
            #{stream_id => StmId, session_id => SessId};
        (_, SessId, _) ->
            #{session_id => SessId}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns handler which closes sequencer stream.
%% @end
%%--------------------------------------------------------------------
-spec close_sequencer_stream_handler() -> Handler :: event_stream:terminate_handler().
close_sequencer_stream_handler() ->
    fun
        (#{stream_id := StmId, session_id := SessId}) ->
            sequencer:close_stream(StmId, SessId);
        (_) ->
            ok
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns handler which sends events to the remote subscriber via sequencer stream.
%% @end
%%--------------------------------------------------------------------
-spec send_events_handler() -> Handler :: event_stream:event_handler().
send_events_handler() ->
    fun
        ([], _) ->
            ok;
        (Evts, #{stream_id := StmId, session_id := SessId}) ->
            sequencer:send_message(#server_message{message_body = #events{events = Evts}, proxy_session_id = SessId}, StmId, SessId);
        (_, _) ->
            ok
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