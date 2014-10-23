%% ===================================================================
%% @author Michał Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of storage_files_manager module.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================
-module(storage_files_manager_tests).

-include_lib("eunit/include/eunit.hrl").
-include("oneprovider_modules/fslogic/fslogic.hrl").
-include("oneprovider_modules/dao/dao.hrl").

-define(SH, "DirectIO").
-define(TEST_ROOT, "/tmp/onedata/"). %% Root of test filesystem
-define(SH2, "ClusterProxy").

%% perms_test_() ->
%%   {foreach, fun setup/0, fun teardown/1, [fun check_perms_user_file/0, fun check_perms_read_group_file/0, fun check_perms_group_perms_file/0, fun check_perms_group_write_file/0]}.

%% setup() ->
%%   meck:new([fslogic, helpers, fslogic_utils, fslogic_objects]).
%%
%% teardown(_) ->
%%   meck:unload().

%% This test checks if data from helpers is cached
file_name_cache_test() ->
  Flag = 2,
  %% Define mock for helpers
  meck:new(helpers),
  meck:expect(helpers, exec, fun
    (getattr, _, _) -> {0, #st_stat{}};
    (is_reg, _, _) -> true;
    (get_flag, _, _) -> Flag
  end),

  %% Get values first time - mock will be used
  TestFile = "file",
  TestFile2 = "file2",
  Ans = storage_files_manager:get_cached_value(TestFile, is_reg, shi),
  ?assertEqual({ok, true}, Ans),

  Ans2 = storage_files_manager:get_cached_value(TestFile, o_rdonly, shi),
  ?assertEqual({ok, Flag}, Ans2),

  %% Unload mock - helpers can not be used
  ?assert(meck:validate(helpers)),
  meck:unload(helpers),

  %% Get values second time - if cache works it is possible, if not method will fail (mock is unloaded)
  Ans3 = storage_files_manager:get_cached_value(TestFile, is_reg, shi),
  ?assertEqual({ok, true}, Ans3),

  Ans4 = storage_files_manager:get_cached_value(TestFile, o_rdonly, shi),
  ?assertEqual({ok, Flag}, Ans4),

  Ans6 = storage_files_manager:get_cached_value(TestFile2, o_rdonly, shi),
  ?assertEqual({ok, Flag}, Ans6),

  Ans7 = storage_files_manager:get_cached_value(TestFile, grp_wr, shi),
  ?assertEqual({ok, false}, Ans7),

  Ans5 = storage_files_manager:get_cached_value(TestFile, owner, shi),
  ?assertEqual({ok, "0"}, Ans5).

%% Tests if permissions in users dir are checked correctly
%% check_perms_user_file() ->
%%   SHInfo = #storage_helper_info{name = ?SH, init_args = [?TEST_ROOT]},
%%
%%   meck:expect(fslogic_objects, get_user, fun() -> {ok, #db_document{record = #user{login = "testuser"}}} end),
%%   meck:expect(fslogic_path, get_user_root, fun() -> {ok, "testuser"} end),
%%   ?assertEqual({ok, true}, storage_files_manager:check_perms("users/testuser/somefile", SHInfo)),
%%
%%   meck:expect(fslogic_path, get_user_root, fun() -> {ok, "testuser2"} end),
%%   ?assertEqual({ok, false}, storage_files_manager:check_perms("users/testuser/somefile", SHInfo)),
%%
%%   meck:expect(fslogic_path, get_user_root, fun() -> {error, error} end),
%%   storage_files_manager:check_perms("users/testuser/somefile", SHInfo),
%%
%%   ?assert(meck:validate(fslogic_path)).

%% Tests if read permissions in groups dir are checked correctly
%% check_perms_read_group_file() ->
%%   SHInfo = #storage_helper_info{name = ?SH, init_args = [?TEST_ROOT]},
%%
%%   meck:expect(fslogic_objects, get_user, fun() -> {ok, #db_document{record = #user{login = "testuser"}}} end),
%%   meck:expect(fslogic_utils, get_user_groups, fun(_, _) -> {ok, [xyz, abc, "testgroup", g123]} end),
%%   ?assertEqual({ok, true}, storage_files_manager:check_perms("groups/testgroup/somefile", SHInfo, read)),
%%
%%   meck:expect(fslogic_utils, get_user_groups, fun(_, _) -> {ok, [xyz, abc, "testgroup2", g123]} end),
%%   ?assertEqual({ok, false}, storage_files_manager:check_perms("groups/testgroup/somefile", SHInfo, read)),
%%
%%   meck:expect(fslogic_utils, get_user_groups, fun(_, _) -> {error, error} end),
%%   ?assertEqual({error, can_not_get_user_groups}, storage_files_manager:check_perms("groups/testgroup/somefile", SHInfo, read)),
%%
%%   ?assert(meck:validate(fslogic_objects)),
%%   ?assert(meck:validate(fslogic_utils)).

%% Tests if permissions to modify file's attributes in groups dir are checked correctly
%% check_perms_group_perms_file() ->
%%   SHInfo = #storage_helper_info{name = ?SH, init_args = [?TEST_ROOT]},
%%
%%   meck:expect(fslogic_objects, get_user, fun() -> {ok, #db_document{record = #user{login = "testuser"}}} end),
%%   meck:expect(fslogic_utils, get_user_groups, fun(_, _) -> {ok, [xyz, abc, "testgroup", g123]} end),
%%   meck:expect(helpers, exec, fun(getattr, _, _) -> {0, #st_stat{st_uid = 1000}} end),
%%   meck:expect(fslogic_utils, get_user_id_from_system, fun
%%     ("testuser") -> "1000\n";
%%     (_) -> "error\n"
%%   end),
%%   ?assertEqual({ok, true}, storage_files_manager:check_perms("groups/testgroup/somefile", SHInfo, perms)),
%%
%%   meck:expect(fslogic_objects, get_user, fun() -> {ok, #db_document{record = #user{login = "testuser2"}}} end),
%%   ?assertEqual({ok, false}, storage_files_manager:check_perms("groups/testgroup/somefile", SHInfo, perms)),
%%
%%   erlang:erase(),
%%   meck:expect(fslogic_objects, get_user, fun() -> {ok, #db_document{record = #user{login = "testuser"}}} end),
%%   meck:expect(helpers, exec, fun(getattr, _, _) -> {0, #st_stat{st_uid = 1001}} end),
%%   ?assertEqual({ok, false}, storage_files_manager:check_perms("groups/testgroup/somefile", SHInfo, perms)),
%%
%%   erlang:erase(),
%%   meck:expect(helpers, exec, fun(getattr, _, _) -> {2, #st_stat{st_uid = 1000}} end),
%%   ?assertEqual({error, can_not_check_file_owner}, storage_files_manager:check_perms("groups/testgroup/somefile", SHInfo, perms)),
%%
%%
%%   ?assert(meck:validate(fslogic_utils)),
%%   ?assert(meck:validate(fslogic_objects)),
%%   ?assert(meck:validate(helpers)).
%%
%% Tests if wrong format of path is found correctly
%% @todo: remove this test when storage_files_manager:check_perms gets permanently removed
%% wrong_path_format_test() ->
%%   SHInfo = #storage_helper_info{name = ?SH, init_args = [?TEST_ROOT]},
%%   ?assertEqual({error, wrong_path_format}, storage_files_manager:check_perms(?TEST_ROOT ++ "something/testuser/somefile", SHInfo)),
%%   ?assertEqual({error, too_short_path}, storage_files_manager:check_perms("something", SHInfo)).

%% Tests if write permissions in groups dir are checked correctly
%% check_perms_group_write_file() ->
%%   SHInfo = #storage_helper_info{name = ?SH, init_args = [?TEST_ROOT]},
%%
%%   meck:expect(fslogic_objects, get_user, fun() -> {ok, #db_document{record = #user{login = "testuser"}}} end),
%%   meck:expect(fslogic_utils, get_user_groups, fun(_, _) -> {ok, [xyz, abc, "testgroup", g123]} end),
%%   meck:expect(helpers, exec, fun(getattr, _, _) -> {0, #st_stat{st_uid = 1000, st_mode = 8#660}} end),
%%   ?assertEqual({ok, true}, storage_files_manager:check_perms("groups/testgroup/somefile", SHInfo)),
%%
%%   erlang:erase(),
%%   meck:expect(fslogic_utils, get_user_id_from_system, fun
%%     ("testuser") -> "1000\n";
%%     (_) -> "error\n"
%%   end),
%%   meck:expect(helpers, exec, fun(getattr, _, _) -> {0, #st_stat{st_uid = 1000, st_mode = 8#640}} end),
%%   ?assertEqual({ok, true}, storage_files_manager:check_perms("groups/testgroup/somefile", SHInfo)),
%%
%%   erlang:erase(),
%%   meck:expect(helpers, exec, fun(getattr, _, _) -> {0, #st_stat{st_uid = 1001, st_mode = 8#640}} end),
%%   ?assertEqual({ok, false}, storage_files_manager:check_perms("groups/testgroup/somefile", SHInfo)),
%%
%%   erlang:erase(),
%%   meck:expect(helpers, exec, fun(getattr, _, _) -> {2, #st_stat{st_uid = 1000, st_mode = 8#660}} end),
%%   ?assertEqual({error, can_not_check_grp_perms}, storage_files_manager:check_perms("groups/testgroup/somefile", SHInfo)),
%%
%%   ?assert(meck:validate(fslogic_utils)),
%%   ?assert(meck:validate(fslogic_objects)),
%%   ?assert(meck:validate(helpers)).
