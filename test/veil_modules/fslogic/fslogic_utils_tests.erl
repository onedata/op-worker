%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of fslogic_utils module.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================
-module(fslogic_utils_tests).

%% TODO dodać testy dla przypadków brzegowych (kiedy strip_path_leaf i basename
%% zwracają [?PATH_SEPARATOR]

%% TODO dodać test get_parent_and_name_from_path

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").
-include_lib("veil_modules/dao/dao.hrl").

setup() ->
  meck:new([dao_lib]).

teardown(_) ->
  ok = meck:unload([dao_lib]).

get_parent_and_name_from_path_test_() ->
  {foreach, fun setup/0, fun teardown/1, [fun get_parent_and_name_from_path/0]}.

strip_path_leaf_test() ->
    ?assertMatch("/some/path", fslogic_path:strip_path_leaf("/some/path/leaf")),
    ?assertMatch("/", fslogic_path:strip_path_leaf("/leaf")),
    ?assertMatch("/", fslogic_path:strip_path_leaf("/")),
    ?assertMatch("/", fslogic_path:strip_path_leaf("")),
    ?assertMatch("/base", fslogic_path:strip_path_leaf("/base/leaf/")).

basename_test() ->
    ?assertMatch("leaf", fslogic_path:basename("/root/dir/leaf")),
    ?assertMatch("/", fslogic_path:basename("/")),
    ?assertMatch("/", fslogic_path:basename("")),
    ?assertMatch("leaf", fslogic_path:basename("leaf")).

get_parent_and_name_from_path() ->
  Doc = #veil_document{uuid = "test_id"},
  meck:expect(dao_lib, apply, fun(_, _, ["/some/path"], _) -> {ok, Doc} end),
  ?assertEqual({ok, {"leaf", Doc}}, fslogic_path:get_parent_and_name_from_path("/some/path/leaf", 1)),

  meck:expect(dao_lib, apply, fun(_, _, ["/not_existing_dir"], _) -> {error, "my_error"} end),
  ?assertEqual({error, "Error: cannot find parent: my_error"}, fslogic_path:get_parent_and_name_from_path("/not_existing_dir/leaf", 1)),

  ?assert(meck:validate(dao_lib)).

-endif.