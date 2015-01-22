%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2013 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains definitions of macros that help during
%%% work with supervisor.
%%% @end
%%%-------------------------------------------------------------------
-ifndef(SUPERVISION_MACROS_HRL).
-define(SUPERVISION_MACROS_HRL, 1).

%% {RestartStrategy :: one_for_one | one_for_all | rest_for_one, MaxRestarts, MaxSecondsBetweenRestarts} -
%% RestartStrategy - http://www.erlang.org/doc/design_principles/sup_princ.html#strategy
%% MaxRestarts MaxSecondsBetweenRestarts - http://www.erlang.org/doc/design_principles/sup_princ.html#frequency
-define(SUP_FLAGS, {one_for_one, 5, 10}).

%% Macro that creates structure that identifies child of supervisor. It tells supervisor to start a child named 'Name',
%% using Module:Function(Args) start function, with given 'Restart' strategy. 5000 sets Allowed shutdown time for child,
%% the type of child is worker. The [Module] part is a name of callback module and in our case is the same as Module of startup function
%% (for more informations see http://www.erlang.org/doc/design_principles/sup_princ.html#id71970)
-define(SUP_CHILD(Name, Module, Restart, Args), {Name, {Module, start_link, Args}, Restart, 5000, worker, [Module]}).
-define(SUP_CHILD(Name, Module, Function, Restart, Args), {Name, {Module, Function, Args}, Restart, 5000, worker, [Module]}).

-endif.