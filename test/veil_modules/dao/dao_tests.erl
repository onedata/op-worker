%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of dao module.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================
-module(dao_tests).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include_lib("veil_modules/dao/dao.hrl").
-endif.

-ifdef(TEST).

main_test_() ->
    {inorder, [fun init/0, fun handle/0, fun cleanUp/0]}.

init() ->
    ?assert(dao:init([]) =:= ok).

handle() ->
    ?assertNot({error, wrong_args} =:= dao:handle(1, {helper, test, []})),
    ?assertNot({error, wrong_args} =:= dao:handle(1, {hosts, test, []})),
    ?assertNot({error, wrong_args} =:= dao:handle(1, {test, []})),
    ?assert({error, wrong_args} =:= dao:handle(1, {"wrong", test, []})),
    ?assert({error,undef} =:= dao:handle(1, {wrong, test, []})),
    meck:new(dao_vfs),
    meck:expect(dao_vfs, list_dir, fun(_, _) -> ok end),
    ok = dao:handle(1, {vfs, list_dir,  ["", test]}),
    meck:unload(dao_vfs).

cleanUp() ->
    ok = dao:cleanUp().

-endif.