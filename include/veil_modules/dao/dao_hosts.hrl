%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: dao_hosts header
%% @end
%% ===================================================================

-ifndef(DAO_HOSTS_HRL).
-define(DAO_HOSTS_HRL, 1).

%% Config
-define(DAO_DB_HOSTS_REFRESH_INTERVAL, 60 * 60 * 1000). % 1h
-define(RPC_MAX_RETRIES, 5). % how many times dao_hosts:call should attempt to rpc:call (with another host) before returning error
-define(DEFAULT_BAN_TIME, 5 * 60 * 1000). % how long host should have lowered priority after disconnection
-define(NODE_DOWN_TIMEOUT, 100). % used as node connection timeout

-endif.
