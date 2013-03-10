%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of dao_helper module.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================
-module(dao_helper_tests).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-ifdef(TEST).

name_test() ->
    <<"test">> = dao_helper:name("test"),
    <<"test">> = dao_helper:name(<<"test">>).

-endif.