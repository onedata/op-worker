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
-endif.

-ifdef(TEST).

main_test_() ->
    {inorder, [fun init/0, fun handle/0, fun cleanUp/0]}.

init() ->
    ?assert(dao:init([]) =:= ok).

handle() ->
    {error, _Resp} = dao:handle(1, {helper, test, []}),
    {error, _Resp} = dao:handle(1, {test, []}).

cleanUp() ->
    ok = dao:cleanUp().

-endif.