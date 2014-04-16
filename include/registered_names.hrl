%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains definitions of names used to identify
%% different parts of application (or whole application).
%% @end
%% ===================================================================

-ifndef(REGISTERED_NAMES_HRL).
-define(REGISTERED_NAMES_HRL, 1).

%% Name of the application.
-define(APP_Name, veil_cluster_node).

%% Global name of gen_server that provides ccm functionality.
-define(CCM, central_cluster_manager).

%% Local name (name and node is used to identify it) of gen_server that 
%% coordinates node life cycle.
-define(Node_Manager_Name, node_manager).

%% Local name (name and node is used to identify it) of supervisor that 
%% coordinates application at each node (one supervisor per node).
-define(Supervisor_Name, sup_name).

%% Local name (name and node is used to identify it) of gen_server that
%% works as a dispatcher.
-define(Dispatcher_Name, request_dispatcher).

%% Name of Round Robin Database that collects node load statistics
-define(Node_Stats_RRD_Name, <<"node_stats.rrd">>).

%% Name of nodes monitoring process
-define(Monitoring_Proc, monitor_process).

%% Local name of the process waiting for dns udp messages
-define(DNS_UDP, dns_udp).

%% name of ETS which contains event types that should be produced by logical files manager
-define(LFM_EVENT_PRODUCTION_ENABLED_ETS, lfm_event_production_enabled_ets).

%% name of ETS which contains user dn which excceded quota (used by logical files manager)
-define(WRITE_DISABLED_USERS, write_disabled_users).

%% name of ETS which contains ack handlers
-define(ACK_HANDLERS, ack_handlers).

-endif.