%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc dao_cluster header
%% @end
%% ===================================================================

-ifndef(DAO_CLUSTER).
-define(DAO_CLUSTER, 1).

%% This record contains environmental variables send by FUSE client
%% Variables are stored in 'env_vars' list. Entry format: {Name :: atom(), Value :: string()}
-record(fuse_session, {uid, hostname = "", env_vars = [], client_storage_info = [], valid_to = 0}).

%% This record represents single FUSE connection and its location.
-record(connection_info, {session_id, controlling_node, controlling_pid}).

-record(dbsync_state, {ets_list = []}).

-endif.