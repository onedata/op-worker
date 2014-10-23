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
-define(APP_Name, oneprovider_node).

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

%% Local name (name and node is used to identify it) of gen_server that
%% coordinates access to Round Robin Database.
-define(RrdErlang_Name, rrderlang).

%% Name of Round Robin Database that collects, unifies and stores nodes statistical data
-define(Cluster_Stats_RRD_Name, <<"cluster_stats.rrd">>).

%% Name of Round Robin Database that collects node load statistics
-define(Node_Stats_RRD_Name, <<"node_stats.rrd">>).

%% Local name of the process waiting for dns udp messages
-define(DNS_UDP, dns_udp).

%% name of ETS which contains event types that should be produced by logical files manager
-define(LFM_EVENT_PRODUCTION_ENABLED_ETS, lfm_event_production_enabled_ets).

%% name of ETS which contains user dn which excceded quota (used by logical files manager)
-define(WRITE_DISABLED_USERS, write_disabled_users).

%% name of ETS which contains ack handlers
-define(ACK_HANDLERS, ack_handlers).

%% name of ETS which caches details about token-based user authentication.
-define(TOKEN_AUTHENTICATION_CACHE, ws_token_authentication).

%% oneproxy listeners
-define(ONEPROXY_DISPATCHER, oneproxy_dispatcher).
-define(ONEPROXY_REST, oneproxy_rest).

%% Storage UID and GID mappings caches
-define(STORAGE_USER_IDS_CACHE, storage_user_ids_cache).
-define(STORAGE_GROUP_IDS_CACHE, storage_group_ids_cache).

-endif.