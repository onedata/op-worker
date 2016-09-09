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
    {ok, ReadTimeThreshold} = application:get_env(?APP_NAME, default_read_event_time_threshold_miliseconds),
    #subscription{
        object = #read_subscription{time_threshold = ReadTimeThreshold},
        event_stream = ?READ_EVENT_STREAM#event_stream_definition{
            emission_time = ReadTimeThreshold,
            emission_rule = fun(_) -> false end,
            transition_rule = fun(_, _) -> ok end,
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
    {ok, WriteTimeThreshold} = application:get_env(?APP_NAME, default_write_event_time_threshold_miliseconds),
    #subscription{
        id = ?FSLOGIC_SUB_ID,
        object = #write_subscription{time_threshold = WriteTimeThreshold},
        event_stream = ?WRITE_EVENT_STREAM#event_stream_definition{
            emission_time = WriteTimeThreshold,
            emission_rule = fun(_) -> false end,
            transition_rule = fun(_, _) -> ok end,
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
    {ok, FileAccessedTimeThreshold} = application:get_env(?APP_NAME,
        default_file_accessed_event_time_threshold_miliseconds),
    #subscription{
        object = #file_accessed_subscription{
            time_threshold = FileAccessedTimeThreshold
        },
        event_stream = ?FILE_ACCESSED_EVENT_STREAM#event_stream_definition{
            emission_time = FileAccessedTimeThreshold,
            emission_rule = fun(_) -> false end,
            transition_rule = fun(_, _) -> ok end,
            event_handler = Handler
        }
    }.