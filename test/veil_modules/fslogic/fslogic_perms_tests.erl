%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013, ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of fslogic_perms.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================
-module(fslogic_perms_tests).
-author("Rafal Slota").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include("veil_modules/dao/dao.hrl").
-include("veil_modules/fslogic/fslogic.hrl").

check_file_perms_test() ->
    meck:new([storage_files_manager,fslogic_context]),
    UserDoc = #veil_document{record = #user{}, uuid = "321"},
    OwnerUserDoc = #veil_document{record = #user{}, uuid = "123"},
    RootUserDoc = #veil_document{record = #user{}, uuid = ?CLUSTER_USER_ID},
    FileDoc = #veil_document{record = #file{uid = "123", perms = ?WR_ALL_PERM}},
    NonWriteableFileDoc = #veil_document{record = #file{uid = "123", perms = ?WR_USR_PERM}},
    meck:expect(storage_files_manager,setup_ctx,fun (_) -> ok end),
    meck:expect(fslogic_context,get_fs_group_ctx,fun () -> ["some_group"] end),

    %% Non group access
    ?assertMatch(ok, fslogic_perms:check_file_perms("/dir", OwnerUserDoc, FileDoc, owner)),
    ?assertMatch(ok, fslogic_perms:check_file_perms("/", OwnerUserDoc, FileDoc, owner)),
    ?assertMatch(ok, fslogic_perms:check_file_perms("/dir/file", OwnerUserDoc, FileDoc, owner)),

    GroupPath = "/" ++ ?GROUPS_BASE_DIR_NAME ++ "/some/path",
    ?assertMatch(ok, fslogic_perms:check_file_perms(GroupPath, OwnerUserDoc, FileDoc,owner)),
    ?assertMatch({error, {permission_denied, _}}, fslogic_perms:check_file_perms(GroupPath, UserDoc, FileDoc, owner)),
    ?assertMatch(ok, fslogic_perms:check_file_perms(GroupPath, UserDoc, FileDoc, write)),
    ?assertMatch({error, {permission_denied, _}}, fslogic_perms:check_file_perms(GroupPath, UserDoc, NonWriteableFileDoc, write)),
    ?assertMatch(ok, fslogic_perms:check_file_perms(GroupPath, OwnerUserDoc, NonWriteableFileDoc, write)),
    ?assertMatch(ok, fslogic_perms:check_file_perms(GroupPath, RootUserDoc, NonWriteableFileDoc, write)),
    meck:unload([storage_files_manager,fslogic_context]).

-endif.