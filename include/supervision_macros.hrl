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

-define(SUP_FLAGS, {one_for_one, 5, 10}).

%% Macro that creates structure that identifies child of supervisor.
-define(SUP_CHILD(Name, Module, Restart, Args), {Name, {Module, start_link, Args}, Restart, 5000, worker, [Module]}).
-define(SUP_CHILD(Name, Module, Function, Restart, Args), {Name, {Module, Function, Args}, Restart, 5000, worker, [Module]}).

-endif.