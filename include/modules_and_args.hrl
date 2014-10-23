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

-define(Modules, [central_logger, cluster_rengine, control_panel, dao_worker, fslogic, gateway, rtransfer, rule_manager, dns_worker, remote_files_manager]).
-define(Modules_With_Args, [{central_logger, []}, {cluster_rengine, []}, {control_panel, []}, {dao_worker, []}, {fslogic, []}, {gateway, []}, {rtransfer, []}, {rule_manager, []}, {dns_worker, []}, {remote_files_manager, []}]).

-endif.