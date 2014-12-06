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

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include_lib("oneprovider_modules/dao/dao.hrl").
-include("registered_names.hrl").
-include("modules_and_args.hrl").
-endif.

-ifdef(TEST).

main_test_() ->
    {setup,
        fun setup/0,
        fun teardown/1,
        [
            {"record wrapping", fun wrap_record/0},
            {"stripping wrappers", fun strip_wrappers/0},
            {"apply asynchronous", fun apply_asynch/0},
            {"apply synchronous", fun apply_synch/0}
        ]
    }.

setup() ->
    meck:new([worker_host],[unstick, passthrough]),
    meck:expect(worker_host,register_simple_cache,
        fun (_,_,_,_,_) -> ok end
    ).

teardown(_) ->
    meck:unload(worker_host).

wrap_record() ->
    ?assertMatch(#db_document{record = #file{}}, dao_lib:wrap_record(#file{})).


strip_wrappers() ->
    ?assertMatch(#file{}, dao_lib:strip_wrappers(#db_document{record = #file{}})),
    ?assertMatch({ok, #file{}}, dao_lib:strip_wrappers({ok, #db_document{record = #file{}}})),
    ?assertMatch({ok, [#file{}, #file_descriptor{}]},
        dao_lib:strip_wrappers({ok, [#db_document{record = #file{}}, #db_document{record = #file_descriptor{}}]})).

apply_asynch() ->
  Module = dao_worker,
  {MainAns, _} = dao_lib:apply(some_module, {asynch, some_method}, args, 1),
  ?assertEqual(MainAns, error),

  {ok, _} = request_dispatcher:start_link(),

  ?assertEqual(dao_lib:apply(some_module, {asynch, some_method}, args, 1), {error, worker_not_found}),

  worker_host:start_link(Module, {[], {init_status, ets:info(db_host_store)}}, 10),
  N1 = node(),
  WorkersList = [{N1, Module}],
  gen_server:cast(?Dispatcher_Name, {update_workers, WorkersList, [], 1, 1, 1, [Module | ?MODULES]}),

  ?assertEqual(dao_lib:apply(some_module, {asynch, some_method}, args, 1), ok),

  worker_host:stop(Module),
  request_dispatcher:stop().

apply_synch() ->
  Module = dao_worker,
  {MainAns, _} = dao_lib:apply(some_module, some_method, args, 1),
  ?assertEqual(error, MainAns),
  {ok, _} = request_dispatcher:start_link(),

  ?assertEqual({error, worker_not_found}, dao_lib:apply(some_module, some_method, args, 1)),

  worker_host:start_link(Module, {[], {init_status, ets:info(db_host_store)}}, 10),
  N1 = node(),
  WorkersList = [{N1, Module}],
  gen_server:cast(?Dispatcher_Name, {update_workers, WorkersList, [], 1, 1, 1, [Module | ?MODULES]}),

  ?assertEqual({error, wrong_args}, dao_lib:apply(some_module, some_method, args, 1)),

  worker_host:stop(Module),
  request_dispatcher:stop().
-endif.






