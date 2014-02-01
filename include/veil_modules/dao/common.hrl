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

-ifndef(COMMON_HRL).
-define(COMMON_HRL, 1).

-define(DAO_REQUEST_TIMEOUT, 5000).

%% View definitions location
-define(VIEW_DEF_LOCATION, "views/").
-define(MAP_DEF_SUFFIX, "_map.js").
-define(REDUCE_DEF_SUFFIX, "_reduce.js").

%% Macros

%% Seeds pseudo-random number generator with current time and hashed node name. <br/>
%% See {@link random:seed/3} and {@link erlang:now/0} for more details
-define(SEED, begin
                IsSeeded = get(proc_seeded),
                if
                    IsSeeded =/= true ->
                        put(proc_seeded, true),
                        {A_SEED, B_SEED, C_SEED} = now(),
                        L_SEED = atom_to_list(node()),
                        {_, Sum_SEED} =  lists:foldl(fun(Elem_SEED, {N_SEED, Acc_SEED}) ->
                            {N_SEED * 137, Acc_SEED + Elem_SEED * N_SEED} end, {1, 0}, L_SEED),
                        random:seed(Sum_SEED*10000 + A_SEED, B_SEED, C_SEED);
                    true -> already_seeded
                end
             end).

%% Returns random positive number from range 1 .. N. This macro is simply shortcut to random:uniform(N)
-define(RND(N), random:uniform(N)).

-endif.