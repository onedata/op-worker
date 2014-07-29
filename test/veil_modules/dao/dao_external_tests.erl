%% ===================================================================
%% @author Tomasz Lichon
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of dao_external module.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================
-module(dao_external_tests).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include_lib("veil_modules/dao/dao.hrl").

is_valid_record_test() ->
    ?assert(dao_external:is_valid_record(#some_record{})),
    ?assert(dao_external:is_valid_record(some_record)),
    ?assert(dao_external:is_valid_record("some_record")),
    ?assertNot(dao_external:is_valid_record({some_record, field1})).

get_set_db_test() ->
    ?assertEqual(?DEFAULT_DB, dao_external:get_db()),
    dao_external:set_db("db"),
    ?assertEqual("db", dao_external:get_db()).

-endif.
