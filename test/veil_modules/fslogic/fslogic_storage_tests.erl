%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of fslogic_storage module.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================
-module(fslogic_storage_tests).

-include_lib("eunit/include/eunit.hrl").
-include("registered_names.hrl").
-include("veil_modules/dao/dao.hrl").
-include("files_common.hrl").

get_sh_for_fuse_test() ->
    SH_default = #storage_helper_info{name = "default_sh"},
    SH_1       = #storage_helper_info{name = "sh1"},
    SH_2       = #storage_helper_info{name = "sh2"},
    StorageInfo =
        #storage_info{name = "storage1", default_storage_helper = SH_default,
                  fuse_groups = [#fuse_group_info{name = "group1", storage_helper = SH_1},
                                 #fuse_group_info{name = "group2", storage_helper = SH_2}]},
    ?assertMatch(SH_1, fslogic_storage:get_sh_for_fuse("group1", StorageInfo)),
    ?assertMatch(SH_2, fslogic_storage:get_sh_for_fuse("group2", StorageInfo)),
    ?assertMatch(SH_default, fslogic_storage:get_sh_for_fuse("group3", StorageInfo)),
    ok.
