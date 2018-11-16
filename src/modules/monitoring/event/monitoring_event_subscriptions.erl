%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% @end
%%%--------------------------------------------------------------------
-module(monitoring_event_subscriptions).

% MWevents - subscriber

-author("Michal Wrzeszcz").

-include("modules/events/subscriptions.hrl").
-include_lib("modules/monitoring/rrd_definitions.hrl").

%% API
-export([monitoring_subscription/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
%%-spec init(Args :: term()) -> Result when
%%    Result :: {ok, State :: worker_host:plugin_state()} | {error, Reason :: term()}.
monitoring_subscription() ->
    #subscription{
        id = ?MONITORING_SUB_ID,
        type = #monitoring_subscription{
            time_threshold = timer:seconds(?STEP_IN_SECONDS)
        }
    }.
