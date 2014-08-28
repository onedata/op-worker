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

-define(TEST_VFS_STORAGE_INFO, "/tmp/vfs_storage.info").
-define(TEST_PROC_MOUNTS, "/tmp/sample_proc_mounts").

get_sh_for_fuse_test() ->
  meck:new([dao_lib]),
  meck:expect(dao_lib, apply, fun
    (_, _, ["fuse1"], _) ->
      {ok, #veil_document{record = #fuse_session{env_vars = [{testvar1, "testvalue1"}, {group_id, "group1"}]}}};
    (_, _, ["fuse2"], _) ->
      {ok, #veil_document{record = #fuse_session{env_vars = [{testvar1, "testvalue1"}, {group_id, "group2"}]}}};
    (_, _, [_], _) -> {error, some_error}
  end),

  SH_default = #storage_helper_info{name = "default_sh"},
  SH_1 = #storage_helper_info{name = "sh1"},
  SH_2 = #storage_helper_info{name = "sh2"},
  StorageInfo =
    #storage_info{name = "storage1", default_storage_helper = SH_default,
      fuse_groups = [#fuse_group_info{name = "group1", storage_helper = SH_1},
        #fuse_group_info{name = "group2", storage_helper = SH_2}]},
  ?assertMatch(SH_1, fslogic_storage:get_sh_for_fuse("fuse1", StorageInfo)),
  ?assertMatch(SH_2, fslogic_storage:get_sh_for_fuse("fuse2", StorageInfo)),
  ?assertMatch(SH_default, fslogic_storage:get_sh_for_fuse("fuse3", StorageInfo)),

  ?assert(meck:validate(dao_lib)),
  meck:unload([dao_lib]).

exist_storage_info_in_config_test() ->
  SampleFile = create_sample_vfs_storage_info_file(),
  ?assertNotEqual(error, SampleFile),
  {ok, File, ExistingStgInfos, NonExistingStgInfos} = SampleFile,
  lists:foreach(fun(ExistingStgInfo) ->
    ?assert(fslogic_storage:exist_storage_info_in_config({path, File}, ExistingStgInfo))
  end, ExistingStgInfos),
  lists:foreach(fun(NonExistingStgInfo) ->
    ?assertNot(fslogic_storage:exist_storage_info_in_config({path, File}, NonExistingStgInfo))
  end, NonExistingStgInfos),
  file:delete(File).

get_mount_points_test() ->
  SampleFile = create_sample_proc_mounts_file(),
  ?assertNotEqual(error, SampleFile),
  {ok, Fd, ExpectedMountPoints} = SampleFile,
  meck:new(file, [unstick, passthrough]),
  meck:expect(file, open, fun("/proc/mounts", [read]) -> {ok, Fd} end),
  ActualMountPoints = fslogic_storage:get_mount_points(),
  ?assert(meck:validate(file)),
  ?assertEqual(ok, meck:unload(file)),
  ?assertEqual(ExpectedMountPoints, ActualMountPoints),
  file:delete(?TEST_PROC_MOUNTS).

get_relative_path_test() ->
  ?assertEqual({ok, "path"}, fslogic_storage:get_relative_path("/base", "/base/path")),
  ?assertEqual({ok, "."}, fslogic_storage:get_relative_path("/base/path", "/base/path")),
  ?assertEqual(error, fslogic_storage:get_relative_path("/base", "/path")).

%% ====================================================================
%% Helper functions
%% ====================================================================

create_sample_vfs_storage_info_file() ->
  ExistingStgInfos = ["{1, /mnt/vfs1}\n", "{2, /mnt/vfs2}\n", "{3, /mnt/vfs3}\n"],
  NonExistingStgInfos = ["{4, /mnt/vfs4}\n", "{5, /mnt/vfs5}\n", "{6, /mnt/vfs6}\n"],
  try
    {ok, Fd} = file:open(?TEST_VFS_STORAGE_INFO, [write]),
    lists:foreach(fun(ExistingStgInfo) -> ok = file:write(Fd, ExistingStgInfo) end, ExistingStgInfos),
    ok = file:close(Fd),
    {ok, ?TEST_VFS_STORAGE_INFO, ExistingStgInfos, NonExistingStgInfos}
  catch
    _:_ -> error
  end.

create_sample_proc_mounts_file() ->
  try
    {ok, WriteFd} = file:open(?TEST_PROC_MOUNTS, [write]),
    ok = file:write(WriteFd, "/mnt/vfs1 /mnt/vfs1 ...\n"),
    ok = file:write(WriteFd, "/mnt/vfs2 /mnt/vfs2 ...\n"),
    ok = file:write(WriteFd, "/mnt/vfs3 /mnt/vfs3 ...\n"),
    ok = file:close(WriteFd),
    {ok, ReadFd} = file:open(?TEST_PROC_MOUNTS, [read]),
    {ok, ReadFd, {ok, ["/mnt/vfs3", "/mnt/vfs2", "/mnt/vfs1"]}}
  catch
    _:_ -> error
  end.
