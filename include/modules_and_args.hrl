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

-define(Modules, [cluster_rengine, control_panel, dao, fslogic, gateway, rtransfer, rule_manager]).
-define(Modules_With_Args, [{cluster_rengine, []}, {control_panel, []}, {dao, []}, {fslogic, []}, {gateway, []}, {rtransfer, []}, {rule_manager, []}]).
