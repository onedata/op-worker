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
-include("oneprovider_modules/dao/dao.hrl").
-include("oneprovider_modules/fslogic/fslogic.hrl").

check_file_perms_test() ->
    UserDoc = #db_document{record = #user{}, uuid = "321"},
    OwnerUserDoc = #db_document{record = #user{}, uuid = "123"},
    RootUserDoc = #db_document{record = #user{}, uuid = ?CLUSTER_USER_ID},
    FileDoc = #db_document{record = #file{uid = "123", perms = ?WR_ALL_PERM}},
    NonWriteableFileDoc = #db_document{record = #file{uid = "123", perms = ?WR_USR_PERM}},

    meck:new(fslogic_file),
    meck:expect(fslogic_file, get_file_local_location, fun(_) -> #file_location{} end),

    GroupPath = "/" ++ ?SPACES_BASE_DIR_NAME ++ "/some/path",
    ?assertMatch(ok, fslogic_perms:check_file_perms(GroupPath, OwnerUserDoc, FileDoc, owner)),
    ?assertMatch({error, {permission_denied, _}}, fslogic_perms:check_file_perms(GroupPath, UserDoc, FileDoc, owner)),
    ?assertMatch(ok, fslogic_perms:check_file_perms(GroupPath, UserDoc, FileDoc, write)),
    ?assertMatch({error, {permission_denied, _}}, fslogic_perms:check_file_perms(GroupPath, UserDoc, NonWriteableFileDoc, write)),
    ?assertMatch(ok, fslogic_perms:check_file_perms(GroupPath, OwnerUserDoc, NonWriteableFileDoc, write)),
    ?assertMatch(ok, fslogic_perms:check_file_perms(GroupPath, RootUserDoc, NonWriteableFileDoc, write)),

    ok = meck:unload(fslogic_file).

-endif.
