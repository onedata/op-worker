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

-define(SEED, random:seed(erlang:now())).
-define(RND(N), random:uniform(N)).

-define(CHILD(I, Type), {I, {I, start_link, [[]]}, transient, 5000, Type, [I]}).

-ifdef(TEST).

-define(LOAD_TEST_NODE(X), rpc:call(X, code, load_file, [?MODULE])).

-endif.