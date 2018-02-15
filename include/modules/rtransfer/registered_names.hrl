%%%-------------------------------------------------------------------
%%% @author Konrad Zemek
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Registered names for gateway gen_servers and supervisors.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(GATEWAY_REGISTERED_NAMES_HRL).
-define(GATEWAY_REGISTERED_NAMES_HRL, true).

-define(RTRANSFER, rtransfer).
-define(GATEWAY, gateway).
-define(GATEWAY_SUPERVISOR, gateway_supervisor).
-define(GATEWAY_LISTENER, gateway_listener).
-define(GATEWAY_DISPATCHER, gateway_dispatcher).
-define(GATEWAY_DISPATCHER_SUPERVISOR, gateway_dispatcher_supervisor).
-define(GATEWAY_CONNECTION_MANAGER_SUPERVISOR, gateway_connection_manager_supervisor).
-define(GATEWAY_CONNECTION_SUPERVISOR, gateway_connection_supervisor).
-define(GATEWAY_INCOMING_QUEUE, gateway_queue).
-define(GATEWAY_NOTIFY_MAP, gateway_map).

-endif.
