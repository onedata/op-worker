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

-record(storage_stats, {read_bytes = 0, write_bytes = 0}).

%% This record is used by node_manager (it contains its state).
%% It describes node type (ccm or worker) and status of connection
%% with ccm (connected or not_connected).
-record(node_state, {node_type = worker, ccm_con_status = not_connected, state_num = 0, callbacks_num = 0, dispatcher_state = 0, callbacks_state = 0, callbacks = [], simple_caches = [], cpu_stats = [], network_stats = [], ports_stats = [], load_logging_fd = undefined}).

%% This record is used by ccm (it contains its state). It describes 
%% nodes, dispatchers and workers in cluster. It also contains reference
%% to process used to monitor if nodes are alive.
-record(cm_state, {nodes = [], workers = [], worker_lifecycle_listeners = [], dispatcher_maps = [], state_num = 1, callbacks_num = 1, cluster_check_num = 0, state_loaded = false, state_monitoring = on, storage_stats = #storage_stats{}, provider_id}).

%% This record is used by worker_host (it contains its state). It describes
%% plug_in that is used and state of this plug_in. It contains also
%% information about time of requests processing (used by ccm during
%% load balancing).
-record(host_state, {plug_in = non, request_map = non, dispatcher_request_map = non, dispatcher_request_map_ok = true, sub_proc_caches_ok = false, sub_procs = [], plug_in_state = [], load_info = [], current_seq_job = none, seq_queue = []}).
%% This method is used to init worker_host when it is using sub proccesses
-record(initial_host_description, {request_map = non, dispatcher_request_map = non, sub_procs = [], plug_in_state = []}).

%% This record is used by requests_dispatcher (it contains its state).
-record(dispatcher_state, {modules = [], modules_const_list = [], state_num = 0, callbacks_num = 0, current_load = 0, avg_load = 0, request_map = [], asnych_mode = false}).

-endif.
