%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Subscription templates.
%%% @end
%%%--------------------------------------------------------------------
-module(event_subscriptions).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("modules/events/definitions.hrl").
-include_lib("ctool/include/global_definitions.hrl").

%% API
-export([read_subscription/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns default subscription for read event
%% @end
%%--------------------------------------------------------------------
-spec read_subscription(function()) -> #subscription{}.
read_subscription(Handler) ->
    {ok, ReadCounterThreshold} = application:get_env(?APP_NAME, default_read_event_counter_threshold),
    {ok, ReadTimeThreshold} = application:get_env(?APP_NAME, default_read_event_time_threshold_miliseconds),
    {ok, ReadSizeThreshold} = application:get_env(?APP_NAME, default_read_event_size_threshold),
    #subscription{
        object = #read_subscription{
            counter_threshold = ReadCounterThreshold,
            time_threshold = ReadTimeThreshold,
            size_threshold = ReadSizeThreshold
        },
        event_stream = ?READ_EVENT_STREAM#event_stream_definition{
            metadata = {0, 0}, %% {Counter, Size}
            emission_time = ReadTimeThreshold,
            emission_rule =
            fun({Counter, Size}) ->
                Counter > ReadCounterThreshold orelse Size > ReadSizeThreshold
            end,
            transition_rule =
            fun({Counter, Size}, #event{counter = C, object = #read_event{size = S}}) ->
                {Counter + C, Size + S}
            end,
            init_handler = event_utils:send_subscription_handler(),
            event_handler = Handler,
            terminate_handler = event_utils:send_subscription_cancellation_handler()
        }
    }.

%%%===================================================================
%%% Internal functions
%%%===================================================================