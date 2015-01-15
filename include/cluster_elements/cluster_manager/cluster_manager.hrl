%%%-------------------------------------------------------------------
%%% @author lichon
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 14. Jan 2015 13:28
%%%-------------------------------------------------------------------

%% This record is used by ccm (it contains its state). It describes
%% nodes, dispatchers and workers in cluster. It also contains reference
%% to process used to monitor if nodes are alive.
-record(cm_state, {nodes = [], workers = [], dispatcher_maps = [], state_num = 1, cluster_check_num = 0, state_loaded = false, state_monitoring = on}).

