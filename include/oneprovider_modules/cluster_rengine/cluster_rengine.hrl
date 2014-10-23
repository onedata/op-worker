%% ===================================================================
%% @author Michal Sitko
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: Common defines for cluster_rengine module
%% @end
%% ===================================================================

-ifndef(CLUSTER_RENGINE).
-define(CLUSTER_RENGINE, 1).

%% processing_config stores information how events should be processed (aggregation, counting etc.)
-record(aggregator_config, {field_name, threshold, fun_field_name}).
-record(filter_config, {field_name, desired_value}).
-record(event_stream_config, {config, wrapped_config}).

-record(event_handler_item, {processing_method, tree_id, map_fun, disp_map_fun, handler_fun, config}).

%% Definitions
-define(EVENT_HANDLERS_CACHE, event_handlers_cache).
-define(EVENT_TREES_MAPPING, event_trees_cache).

%% Types
-type event_handler_item() :: #event_handler_item{}.

-endif.