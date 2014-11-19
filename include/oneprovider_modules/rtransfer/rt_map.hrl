%% ===================================================================
%% @author Krzysztof Trzepla
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This file contains common macros and records for RTransfer
%% priority queue.
%% @end
%% ===================================================================

-ifndef(RT_PRIORITY_QUEUE_HRL).
-define(RT_PRIORITY_QUEUE_HRL, 1).

%% gen_server state
%% * container - pointer to container resource created as a call to rt_container:init_nif() function
-record(state, {container_ptr}).

-endif.