%% ===================================================================
%% @author Krzysztof Trzepla
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This file contains common macros and records for RTransfer
%% heap.
%% @end
%% ===================================================================

%% gen_server state
%% * heap - pointer to heap resource created as a call to rt_heap:init_nif() function
-record(state, {heap}).

%% RTransfer heap element
-record(rt_block, {file_id = "", offset = 0, size = 0, priority = 0, pids = []}).