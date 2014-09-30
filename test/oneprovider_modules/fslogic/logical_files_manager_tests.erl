%% ===================================================================
%% @author Michał Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of logical_files_manager module.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================
-module(logical_files_manager_tests).

-include_lib("eunit/include/eunit.hrl").

cache_size_test() ->
  TestFile = "file",
  TestFile2 = "file2",
  S1 = logical_files_manager:cache_size(TestFile, 10),
  ?assertEqual(0, S1),
  S2 = logical_files_manager:cache_size(TestFile, 15),
  ?assertEqual(10, S2),
  S3 = logical_files_manager:cache_size(TestFile2, 13),
  ?assertEqual(0, S3),
  S4 = logical_files_manager:cache_size(TestFile, 2),
  ?assertEqual(25, S4).
