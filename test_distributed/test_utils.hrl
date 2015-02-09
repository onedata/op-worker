%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2014 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This file contains ct tests helper macros and definitions.
%%% @end
%%%--------------------------------------------------------------------
-ifndef(TEST_UTILS_HRL).
-define(TEST_UTILS_HRL, 1).

-include_lib("ctool/include/test/assertions.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Catches suite init exceptions
-define(TRY_INIT(Config, EnvDescription),
    try test_node_starter:prepare_test_environment(Config, EnvDescription, ?MODULE)
    catch A:B -> ct:print("~p:~p~n~p", [A, B, erlang:get_stacktrace()]) end).

%% Returns absolute path to given file in the test data directory
-define(TEST_FILE(Config, X), filename:join(?config(data_dir, Config), X)).

%% Utility macros
-define(CURRENT_HOST, list_to_atom(lists:last(string:tokens(atom_to_list(node()), "@")))).
-define(NODE(NodeHost, NodeName), list_to_atom(atom_to_list(NodeName)++"@"++atom_to_list(NodeHost))).
-define(GET_NODE_NAME(FullName), list_to_atom(hd(string:tokens(atom_to_list(FullName), "@")))).
-define(GET_HOST(FullName), list_to_atom(lists:last(string:tokens(atom_to_list(FullName), "@")))).

-endif.