%% ===================================================================
%% @author Michal Sitko
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: Common defines for dao module
%% @end
%% ===================================================================

-ifndef(CLUSTER_RENGINE).
-define(CLUSTER_RENGINE, 1).


-record(event, {timestamp, multiplicity}).

%% processing_config stores information how events should be processed (aggregation, counting etc.)
-record(aggregator_config, {field_name, threshold, fun_field_name}).
-record(filter_config, {field_name, desired_value}).
-record(event_stream_config, {config, wrapped_config}).

%% Events definitions
%% field ans_pid is just for test purposes - in general events handlers should be fired just for side effects
-record(write_event, {ans_pid, fuse_id, user_id, user_dn, file_id, bytes, event = #event{}}).
-record(read_event, {ans_pid, user_id, file_id, bytes, event = #event{}}).
-record(mkdir_event, {ans_pid, fuse_id, user_id, user_dn, file_id, event = #event{}}).
-record(rm_event, {fuse_id, user_id, user_dn, file_id, event = #event{}}).
-record(open_event, {ans_pid, user_id, file_id, event = #event{}}).

-record(event_handler_item, {processing_method, tree_id, map_fun, disp_map_fun, handler_fun, config}).

%% Definitions
-define(EVENT_HANDLERS_CACHE, event_handlers_cache).
-define(EVENT_TREES_MAPPING, event_trees_cache).

-endif.