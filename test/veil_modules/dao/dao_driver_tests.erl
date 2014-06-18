%% ===================================================================
%% @author Tomasz Lichon
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of dao_driver module.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================
-module(dao_driver_tests).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include_lib("veil_modules/dao/dao.hrl").

is_valid_record_test() ->
    ?assert(dao_driver:is_valid_record(#some_record{})),
    ?assert(dao_driver:is_valid_record(some_record)),
    ?assert(dao_driver:is_valid_record("some_record")),
    ?assertNot(dao_driver:is_valid_record({some_record, field1})).

get_set_db_test() ->
    ?assertEqual(?DEFAULT_DB, dao_driver:get_db()),
    dao_driver:set_db("db"),
    ?assertEqual("db", dao_driver:get_db()).

-endif.
