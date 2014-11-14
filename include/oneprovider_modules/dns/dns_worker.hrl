%% ===================================================================
%% @author Bartosz Polnik
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains definitions of state used by dns_worker module.
%% @end
%% ===================================================================

-ifndef(DNS_WORKER_HRL).
-define(DNS_WORKER_HRL, 1).

%% This record is used by dns_worker (it contains its state). The first element of
%% a tuple is a name of a module and the second is a list of ip addresses of nodes
%% sorted ascending by load.
%%
%% Example:
%%     Assuming that dns_worker plugin works on node1@127.0.0.1 and node2@192.168.0.1,
%%     (load on node1 < load on node2) and control_panel works on node3@127.0.0.1,
%%     dns_worker state will look like this:
%%     {dns_state, [{dns_worker, [{127,0,0,1}, {192,168,0,1}]}, {control_panel, [{127,0,0,1}]}]}
-record(dns_worker_state, {workers_list = [] :: [{atom(),  [{inet:ip4_address(), integer(), integer()}]}], nodes_list = [] :: [{inet:ip4_address(),  number()}], avg_load = 0 :: number()}).

-define(EXTERNALLY_VISIBLE_MODULES, [control_panel, fslogic, gateway, rtransfer, dns_worker]).

-endif.