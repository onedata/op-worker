%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This file contains macros for connection modules usage.
%%% @end
%%%--------------------------------------------------------------------
-ifndef(CONNECTION_HRL).
-define(CONNECTION_HRL, 1).

-include("global_definitions.hrl").

% Heartbeat messages are sent to peer periodically to inform him that
% his requests are still carried on.
-define(WORKERS_STATUS_CHECK_INTERVAL, application:get_env(
    ?APP_NAME, router_processes_check_interval, timer:seconds(10)
)).

% Keepalive messages are sent to peer periodically to keep connection alive.
-define(KEEPALIVE_TIMEOUT, timer:seconds(60)).

-endif.
