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
-include("veil_modules/fslogic/fslogic.hrl").

%% This test checks if data from helpers is cached
file_name_cache_test() ->
  Flag = 2,
  %% Define mock for helpers
  meck:new(veilhelpers),
  meck:expect(veilhelpers, exec, fun
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
  ?assert(meck:validate(veilhelpers)),
  meck:unload(veilhelpers),

  %% Get values second time - if cache works it is possible, if not method will fail (mock is unloaded)
  Ans3 = storage_files_manager:get_cached_value(TestFile, is_reg, shi),
  ?assertEqual({ok, true}, Ans3),

  Ans4 = storage_files_manager:get_cached_value(TestFile, o_rdonly, shi),
  ?assertEqual({ok, Flag}, Ans4),

  Ans5 = storage_files_manager:get_cached_value(TestFile2, o_rdonly, shi),
  ?assertEqual({ok, Flag}, Ans5).