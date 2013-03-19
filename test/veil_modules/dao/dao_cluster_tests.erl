%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of dao_cluster module.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================
-module(dao_cluster_tests).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include_lib("veil_modules/dao/dao.hrl").
-endif.

-ifdef(TEST).

save_record_test() ->
    ?assertException(throw, unsupported_record, dao_cluster:save_record(whatever, {a, b, c})),
    ?assertException(throw, invalid_record, dao_cluster:save_record(whatever, {some_record, a, c})).

get_record_test() ->
    ?assertException(throw, unsupported_record, dao_cluster:get_record(test, whatever)),
    ?assertException(throw, unsupported_record, dao_cluster:get_record(test, {whatever, a, c})).

-endif.