%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file defines modules used by system and their
%% initial arguments.
%% @end
%% ===================================================================

-ifndef(MODULES_AND_ARGS_HRL).
-define(MODULES_AND_ARGS_HRL, 1).

-define(MODULES, [central_logger, cluster_rengine, control_panel, dao_worker, fslogic, gateway, rtransfer, rule_manager, dns_worker, remote_files_manager, dbsync, gr_channel]).
-define(MODULES_WITH_ARGS, [{central_logger, []}, {cluster_rengine, []}, {control_panel, []}, {dao_worker, []}, {fslogic, []}, {gateway, []}, {rtransfer, []}, {rule_manager, []}, {dns_worker, []}, {remote_files_manager, []}, {dbsync, []}, {gr_channel, []}]).

%% Modules that are present on all workers.
-define(PERMANENT_MODULES, [gateway]).

%% Singleton modules are modules which are supposed to have only one instance.
-define(SINGLETON_MODULES, [control_panel, central_logger, rule_manager, rtransfer]).

-endif.