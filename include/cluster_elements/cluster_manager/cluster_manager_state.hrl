%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% The state of cluster manager
%%% @end
%%%-------------------------------------------------------------------

-ifndef(CLUSTER_MANAGER_STATE_HRL).
-define(CLUSTER_MANAGER_STATE_HRL, 1).

%% This record is used by ccm (it contains its state). It describes
%% nodes, dispatchers and workers in cluster. It also contains reference
%% to process used to monitor if nodes are alive.
-record(cm_state, {
    nodes = [] :: [] | [Node :: node()],
    workers = [] ::[] | [{Node :: node(), Module :: module(), Args :: term()}],
    state_num = 1 :: integer()
}).

-endif.