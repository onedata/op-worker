-record(node_state, {node_type = worker, ccm_con_status = not_connected, dispatchers = [], workers = []}).
-record(cm_state, {nodes = [], dispatchers = [], workers = []}).
-record(host_state, {plug_in = non, plug_in_state = [], load_info = []}).