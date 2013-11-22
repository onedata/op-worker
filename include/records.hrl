%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains definitions of records used by different
%% modules of application.
%% @end
%% ===================================================================

-ifndef(RECORDS_HRL).
-define(RECORDS_HRL, 1).

%% This record is used by node_manager (it contains its state). 
%% It describes node type (ccm or worker) and status of connection
%% with ccm (connected or not_connected).
-record(node_state, {node_type = worker, ccm_con_status = not_connected, state_num = 0, callbacks_num = 0, dispatcher_state = ok, callbacks_state = ok, memory_and_network_info = [], callbacks = []}).

%% This record is used by ccm (it contains its state). It describes 
%% nodes, dispatchers and workers in cluster. It also contains reference
%% to process used to monitor if nodes are alive.
-record(cm_state, {nodes = [], workers = [], state_num = 1, callbacks_num = 1, cluster_check_num = 0}).

%% This record is used by worker_host (it contains its state). It describes
%% plug_in that is used and state of this plug_in. It contains also
%% information about time of requests processing (used by ccm during
%% load balancing).
-record(host_state, {plug_in = non, plug_in_state = [], load_info = [], current_seq_job = none, seq_queue = []}).

%% This record is used by requests_dispatcher (it contains its state).
-record(dispatcher_state, {modules = [], state_num = 0, callbacks_num = 0, current_load = 0, avg_load = 0}).

-endif.
