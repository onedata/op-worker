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


-record(event, {timestamp}).

%% processing_config stores information how events should be processed (aggregation, counting etc.)
-record(processing_config, {init_counter, aggregation_discriminator}).

%% Events definitions
%% field ans_pid is just for test purposes - in general events handlers should be fired just for side effects
-record(write_event, {ans_pid, user_id, file_id, bytes, event = #event{}}).
-record(mkdir_event, {ans_pid, user_id, event = #event{}}).
-record(event_handler_item, {processing_method, tree_id, map_fun, disp_map_fun, handler_fun, config = #processing_config{}}).

%% Definitions
-define(EVENT_HANDLERS_CACHE, event_handlers_cache).
-define(EVENT_TREES_MAPPING, event_trees_cache).

-endif.