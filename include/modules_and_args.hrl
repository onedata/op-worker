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

-define(MODULES, [http_worker, dns_worker]).
-define(MODULES_WITH_ARGS, [{http_worker, []}, {dns_worker, []}]).

%% Modules that are present on all workers.
-define(PERMANENT_MODULES, []).

%% Singleton modules are modules which are supposed to have only one instance.
-define(SINGLETON_MODULES, []).

-endif.