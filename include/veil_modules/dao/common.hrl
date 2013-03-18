%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: Common defines for dao module
%% @end
%% ===================================================================

%% Macros
-define(SEED, random:seed(erlang:now())).
-define(RND(N), random:uniform(N)).
-define(CHILD(I, Type), {I, {I, start_link, [[]]}, transient, 5000, Type, [I]}).