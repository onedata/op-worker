%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% cluster_manager (ccm) definitions.
%%% @end
%%%-------------------------------------------------------------------
-ifndef(CLUSTER_MANAGER_HRL).
-define(CLUSTER_MANAGER_HRL, 1).

%% This record is used by ccm (it contains its state). It describes
%% nodes, dispatchers and workers in cluster. It also contains reference
%% to process used to monitor if nodes are alive.
-record(cm_state, {nodes = [], workers = [], dispatcher_maps = [], state_num = 1, cluster_check_num = 0, state_loaded = false, state_monitoring = on}).

-endif.