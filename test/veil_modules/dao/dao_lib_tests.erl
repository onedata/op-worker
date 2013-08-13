%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of dao_lib module.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================
-module(dao_lib_tests).

%% TODO przetestować metodę apply

-include_lib("eunit/include/eunit.hrl").
-include_lib("veil_modules/dao/dao.hrl").
-include("registered_names.hrl").
-include("modules_and_args.hrl").

wrap_record_test() ->
    ?assertMatch(#veil_document{record = #file{}}, dao_lib:wrap_record(#file{})).


strip_wrappers_test() ->
    ?assertMatch(#file{}, dao_lib:strip_wrappers(#veil_document{record = #file{}})),
    ?assertMatch({ok, #file{}}, dao_lib:strip_wrappers({ok, #veil_document{record = #file{}}})),
    ?assertMatch({ok, [#file{}, #file_descriptor{}]},
        dao_lib:strip_wrappers({ok, [#veil_document{record = #file{}}, #veil_document{record = #file_descriptor{}}]})).

apply_asynch_test() ->
  Module = dao,
  {MainAns, _} = dao_lib:apply(some_module, {asynch, some_method}, args, 1),
  ?assertEqual(MainAns, error),

  {ok, _} = request_dispatcher:start_link(),

  ?assertEqual(dao_lib:apply(some_module, {asynch, some_method}, args, 1), {error, worker_not_found}),

  worker_host:start_link(Module, [], 10),
  N1 = node(),
  WorkersList = [{N1, Module}],
  gen_server:cast(?Dispatcher_Name, {update_workers, WorkersList, 1, 1, 1, [Module | ?Modules]}),

  ?assertEqual(dao_lib:apply(some_module, {asynch, some_method}, args, 1), ok),

  worker_host:stop(Module),
  request_dispatcher:stop().

apply_synch_test() ->
  Module = dao,
  {MainAns, _} = dao_lib:apply(some_module, some_method, args, 1),
  ?assertEqual(MainAns, error),
  {ok, _} = request_dispatcher:start_link(),

  ?assertEqual(dao_lib:apply(some_module, some_method, args, 1), {error, worker_not_found}),

  worker_host:start_link(Module, [], 10),
  N1 = node(),
  WorkersList = [{N1, Module}],
  gen_server:cast(?Dispatcher_Name, {update_workers, WorkersList, 1, 1, 1, [Module | ?Modules]}),

  ?assertEqual(dao_lib:apply(some_module, some_method, args, 1), {error, wrong_args}),

  worker_host:stop(Module),
  request_dispatcher:stop().
