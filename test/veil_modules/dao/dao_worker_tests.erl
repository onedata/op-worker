%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of dao_worker module.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================
-module(dao_worker_tests).

%% TODO nie przetestowane widoki (można to zrobić w teście ct) - ogólnie przydałby się jakiś integracyjny test dao w ct
%% TODO nie przetestowane listowanie rekordów (funkcja ogólna)

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include_lib("veil_modules/dao/dao.hrl").
-include_lib("dao/include/couch_db.hrl").
-include_lib("dao/include/common.hrl").
-endif.

-ifdef(TEST).

main_test_() ->
    {foreach, fun setup/0, fun teardown/1, [fun order_suite/1]}.

order_suite(_) ->
    {inorder, [fun init/0, fun handle/0, fun cleanup/0]}.

setup() ->
    meck:new([worker_host],[unstick, passthrough]),
    meck:new([dao_helper, dao_vfs]).

teardown(_) ->
    ok = meck:unload([dao_helper, dao_vfs, worker_host]).

init() ->
    meck:expect(worker_host,register_simple_cache,
        fun (_,_,_,_,_) -> ok end
    ),
    InitAns1 = dao_worker:init([]),
    ?assert(is_record(InitAns1, initial_host_description)),
    ?assertEqual(ok, InitAns1#initial_host_description.plug_in_state),
    worker_host:stop_all_sub_proc(InitAns1#initial_host_description.sub_procs),

    InitAns2 = dao_worker:init([]),
    ?assert(is_record(InitAns2, initial_host_description)),
    ?assertEqual(ok, InitAns2#initial_host_description.plug_in_state),
    meck:validate(worker_host),
    worker_host:stop_all_sub_proc(InitAns2#initial_host_description.sub_procs).

handle() ->
    ?assertNotEqual({error, wrong_args}, dao_worker:handle(1, {helper, test, []})),
    ?assertNotEqual({error, wrong_args}, dao_worker:handle(1, {hosts, test, []})),
    ?assertNotEqual({error, wrong_args}, dao_worker:handle(1, {test, []})),
    ?assertEqual({error, wrong_args}, dao_worker:handle(1, {"wrong", test, []})),
    ?assertEqual({error, undef}, dao_worker:handle(1, {wrong, test, []})),
    meck:expect(dao_vfs, list_dir, fun(_, _, _) -> ok end),
    ?assertEqual(ok, dao_worker:handle(1, {vfs, list_dir, [test, test, test]})),
    ?assert(meck:validate(dao_vfs)).

cleanup() ->
    ?assertEqual(ok, dao_worker:cleanup()).


-endif.
