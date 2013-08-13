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

wrap_record_test() ->
    ?assertMatch(#veil_document{record = #file{}}, dao_lib:wrap_record(#file{})).


strip_wrappers_test() ->
    ?assertMatch(#file{}, dao_lib:strip_wrappers(#veil_document{record = #file{}})),
    ?assertMatch({ok, #file{}}, dao_lib:strip_wrappers({ok, #veil_document{record = #file{}}})),
    ?assertMatch({ok, [#file{}, #file_descriptor{}]},
        dao_lib:strip_wrappers({ok, [#veil_document{record = #file{}}, #veil_document{record = #file_descriptor{}}]})).