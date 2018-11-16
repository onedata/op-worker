%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module decides where to send incoming event messages.
%%% @end
%%%-------------------------------------------------------------------
-module(event_router).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/event_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("cluster_worker/include/exometer_utils.hrl").

%% API
-export([route_proxy_message/2, route_and_ignore_answer/1,
    route_and_send_answer/1]).
-export([init_counters/0, init_report/0]).

-define(EXOMETER_NAME(Param), ?exometer_name(?MODULE, Param)).
-define(EXOMETER_COUNTERS, [events, event]).
-define(EXOMETER_HISTOGRAM_COUNTERS, [events_length]).
-define(EXOMETER_DEFAULT_DATA_POINTS_NUMBER, 10000).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Route messages that were send to remote-proxy session on different provider.
%% @end
%%--------------------------------------------------------------------
-spec route_proxy_message(Msg :: #client_message{}, TargetSessionId :: session:id()) -> ok.
route_proxy_message(#client_message{message_body = #events{events = Events}} = Msg, TargetSessionId) ->
    ?debug("route_proxy_message ~p ~p", [TargetSessionId, Msg]),
    ?update_counter(?EXOMETER_NAME(events)),
    ?update_counter(?EXOMETER_NAME(events_length), length(Events)),
    lists:foreach(fun(Evt) ->
        event:emit(Evt, TargetSessionId)
    end, Events);
route_proxy_message(#client_message{message_body = #event{} = Evt} = Msg, TargetSessionId) ->
    ?debug("route_proxy_message ~p ~p", [TargetSessionId, Msg]),
    ?update_counter(?EXOMETER_NAME(event)),
    event:emit(Evt, TargetSessionId),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Route message to adequate worker and return ok
%% @end
%%--------------------------------------------------------------------
-spec route_and_ignore_answer(#client_message{}) -> ok.
route_and_ignore_answer(#client_message{message_body = #event{} = Evt} = Msg) ->
    event:emit(Evt, router:effective_session_id(Msg)),
    ok;
route_and_ignore_answer(#client_message{message_body = #events{events = Evts}} = Msg) ->
    lists:foreach(fun(#event{} = Evt) ->
        event:emit(Evt, router:effective_session_id(Msg)) end, Evts),
    ok;
route_and_ignore_answer(#client_message{message_body = #subscription{} = Sub} = Msg) ->
    case session_manager:is_provider_session_id(router:effective_session_id(Msg)) of
        true ->
            ok; %% Do not route subscriptions from other providers (route only subscriptions from users)
        false ->
            event:subscribe(Sub, router:effective_session_id(Msg)),
            ok
    end;
route_and_ignore_answer(#client_message{message_body = #subscription_cancellation{} = SubCan} = Msg) ->
    case session_manager:is_provider_session_id(router:effective_session_id(Msg)) of
        true ->
            ok; %% Do not route subscription_cancellations from other providers
        false ->
            event:unsubscribe(SubCan, router:effective_session_id(Msg)),
            ok
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Route message to adequate worker, asynchronously wait for answer
%% repack it into server_message and send to the client
%% @end
%%--------------------------------------------------------------------
-spec route_and_send_answer(#client_message{}) -> ok.
route_and_send_answer(Msg = #client_message{
    session_id = OriginSessId,
    message_id = MsgId,
    message_body = FlushMsg = #flush_events{}
}) ->
    event:flush(FlushMsg#flush_events{notify =
    fun(Result) ->
        % Spawn because send can wait and block event_stream
        % Probably not needed after migration to asynchronous connections
        spawn(fun() ->
            communicator:send(Result#server_message{message_id = MsgId}, OriginSessId)
        end)
    end
    }, router:effective_session_id(Msg)),
    ok.

%%%===================================================================
%%% Exometer API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Initializes all counters.
%% @end
%%--------------------------------------------------------------------
-spec init_counters() -> ok.
init_counters() ->
    Size = application:get_env(?CLUSTER_WORKER_APP_NAME, 
        exometer_data_points_number, ?EXOMETER_DEFAULT_DATA_POINTS_NUMBER),
    Counters = lists:map(fun(Name) ->
        {?EXOMETER_NAME(Name), counter}
    end, ?EXOMETER_COUNTERS),
    Counters2 = lists:map(fun(Name) ->
        {?EXOMETER_NAME(Name), uniform, [{size, Size}]}
    end, ?EXOMETER_HISTOGRAM_COUNTERS),
    ?init_counters(Counters ++ Counters2).

%%--------------------------------------------------------------------
%% @doc
%% Subscribe for reports for all parameters.
%% @end
%%--------------------------------------------------------------------
-spec init_report() -> ok.
init_report() ->
    Reports = lists:map(fun(Name) ->
        {?EXOMETER_NAME(Name), [value]}
    end, ?EXOMETER_COUNTERS),
    Reports2 = lists:map(fun(Name) ->
        {?EXOMETER_NAME(Name), [min, max, median, mean, n]}
    end, ?EXOMETER_HISTOGRAM_COUNTERS),
    ?init_reports(Reports ++ Reports2).
