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

-define(Modules, [central_logger, cluster_rengine, control_panel, dao, fslogic, gateway, rtransfer, rule_manager, dns_worker]).
-define(Modules_With_Args, [{central_logger, []}, {cluster_rengine, []}, {control_panel, []}, {dao, []}, {fslogic, []}, {gateway, []}, {rtransfer, []}, {rule_manager, []}, {dns_worker, []}]).
