%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of remote_files_manager.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================

-module(remote_files_manager_tests).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-ifdef(TEST).

-include("veil_modules/fslogic/fslogic.hrl").
-include("veil_modules/dao/dao_vfs.hrl").

-define(SH, "DirectIO").
-define(TEST_ROOT, ["/tmp/veilfs"]). %% Root of test filesystem

get_helper_and_id_test() ->
  Storage = 1,
  Id = "id1",
  Ans1 = remote_files_manager:get_storage_and_id(integer_to_list(Storage) ++ ?REMOTE_HELPER_SEPARATOR ++ Id),
  ?assertEqual({Storage, Id}, Ans1).

get_helper_and_id_wrong_args_test() ->
  Storage = 1,
  Id = "id1",
  Ans1 = remote_files_manager:get_storage_and_id(integer_to_list(Storage) ++ Id),
  ?assertEqual(error, Ans1),

  Storage2 = "qqq",
  Ans2 = remote_files_manager:get_storage_and_id(Storage2 ++ ?REMOTE_HELPER_SEPARATOR ++ Id),
  ?assertEqual(error, Ans2).

-endif.
