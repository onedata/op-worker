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
-module(fslogic_subscriptions).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("modules/events/definitions.hrl").

%% API
-export([file_read_subscription/0, file_written_subscription/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns subscription for file read operation events.
%% @end
%%--------------------------------------------------------------------
-spec file_read_subscription() -> #subscription{}.
file_read_subscription() ->
    {ok, Threshold} = application:get_env(?APP_NAME,
        file_read_event_time_threshold_milliseconds),
    #subscription{
        id = ?FILE_READ_SUB_ID,
        type = #file_read_subscription{time_threshold = Threshold}
    }.

%%--------------------------------------------------------------------
%% @doc
%% Returns subscription for file written operation events.
%% @end
%%--------------------------------------------------------------------
-spec file_written_subscription() -> #subscription{}.
file_written_subscription() ->
    {ok, Threshold} = application:get_env(?APP_NAME,
        file_written_event_time_threshold_milliseconds),
    #subscription{
        id = ?FILE_WRITTEN_SUB_ID,
        type = #file_written_subscription{time_threshold = Threshold}
    }.
