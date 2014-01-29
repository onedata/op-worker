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

%% Events definitions
-record(write_event, {user_id, file_id, bytes, event = #event{}}).
-record(event_handler_item, {tree_id, map_fun, disp_map_fun, handler_fun}).

%% Definitions
-define(EVENT_HANDLERS_CACHE, event_handlers_cache).
-define(EVENT_TREES_MAPPING, event_trees_cache).

-endif.