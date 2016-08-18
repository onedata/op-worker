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
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/global_definitions.hrl").

%% API
-export([read_subscription/1, write_subscription/1, file_accessed_subscription/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns default subscription for read events.
%% @end
%%--------------------------------------------------------------------
-spec read_subscription(fun()) -> #subscription{}.
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
            emission_rule = fun({Counter, Size}) ->
                Counter > ReadCounterThreshold orelse Size > ReadSizeThreshold
            end,
            transition_rule = fun({Counter, Size}, #event{counter = C, object = #read_event{size = S}}) ->
                {Counter + C, Size + S}
            end,
            event_handler = Handler
        }
    }.

%%--------------------------------------------------------------------
%% @doc
%% Returns default subscription for write events.
%% @end
%%--------------------------------------------------------------------
-spec write_subscription(fun()) -> #subscription{}.
write_subscription(Handler) ->
    {ok, WriteCounterThreshold} = application:get_env(?APP_NAME, default_write_event_counter_threshold),
    {ok, WriteTimeThreshold} = application:get_env(?APP_NAME, default_write_event_time_threshold_miliseconds),
    {ok, WriteSizeThreshold} = application:get_env(?APP_NAME, default_write_event_size_threshold),
    #subscription{
        id = ?FSLOGIC_SUB_ID,
        object = #write_subscription{
            counter_threshold = WriteCounterThreshold,
            time_threshold = WriteTimeThreshold,
            size_threshold = WriteSizeThreshold
        },
        event_stream = ?WRITE_EVENT_STREAM#event_stream_definition{
            metadata = {0, 0}, %% {Counter, Size}
            emission_time = WriteTimeThreshold,
            emission_rule = fun({Counter, Size}) ->
                Counter > WriteCounterThreshold orelse Size > WriteSizeThreshold
            end,
            transition_rule = fun({Counter, Size}, #event{counter = C, object = #write_event{size = S}}) ->
                {Counter + C, Size + S}
            end,
            event_handler = Handler
        }
    }.

%%--------------------------------------------------------------------
%% @doc
%% Returns default subscription for file access events.
%% @end
%%--------------------------------------------------------------------
-spec file_accessed_subscription(fun()) -> #subscription{}.
file_accessed_subscription(Handler) ->
    {ok, FileAccessedThreshold} = application:get_env(?APP_NAME,
        default_file_accessed_event_counter_threshold),
    {ok, FileAccessedTimeThreshold} = application:get_env(?APP_NAME,
        default_file_accessed_event_time_threshold_miliseconds),
    #subscription{
        object = #file_accessed_subscription{
            counter_threshold = FileAccessedThreshold,
            time_threshold = FileAccessedTimeThreshold
        },
        event_stream = ?FILE_ACCESSED_EVENT_STREAM#event_stream_definition{
            metadata = 0,
            emission_rule = fun(_) -> true end,
            event_handler = Handler
        }
    }.