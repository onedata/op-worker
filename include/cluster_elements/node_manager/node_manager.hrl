%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C): 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%% @doc
%%% Node manager definitions
%%% @end
%%%-------------------------------------------------------------------

%% This record is used by node_manager (it contains its state).
%% It describes node type (ccm or worker) and status of connection
%% with ccm (connected or not_connected).
-record(node_state, {node_type = worker, ccm_con_status = not_connected, state_num = 0, dispatcher_state = 0}).

