%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains definitions of macros that help during
%% work with supervisor.
%% @end
%% ===================================================================

%% Macro that creates structure that identifies child of supervisor.
-define(Sup_Child(Id, I, Type, Args), {Id, {I, start_link, Args}, Type, 5000, worker, [I]}).
-define(Sup_Child(Id, I, F, Type, Args), {Id, {I, F, Args}, Type, 5000, worker, [I]}).
-define(Sup_Child(Id, I, F, Type, Child_Type, Args), {Id, {I, F, Args}, Type, 5000, Child_Type, [I]}).