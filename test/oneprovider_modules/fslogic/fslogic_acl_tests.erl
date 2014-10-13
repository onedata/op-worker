%% ===================================================================
%% @author Tomasz Lichon
%% @copyright (C): 2014, ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This module tests the functionality of fslogic_acl.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================
-module(fslogic_acl_tests).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include("oneprovider_modules/fslogic/fslogic_acl.hrl").
-include("fuse_messages_pb.hrl").
-include("oneprovider_modules/fslogic/fslogic.hrl").
-include("oneprovider_modules/dao/dao_users.hrl").

check_permission_test() ->
    Id1 = <<"id1">>,
    User1 = #user{global_id = Id1},
    Ace1 = #accesscontrolentity{acetype = ?allow_mask, aceflags = ?no_flags_mask, identifier = Id1, acemask = ?read_mask},
    Ace2 = #accesscontrolentity{acetype = ?allow_mask, aceflags = ?no_flags_mask, identifier = Id1, acemask = ?write_mask},
    % read permission
    ?assertEqual(ok, fslogic_acl:check_permission([Ace1, Ace2], User1, ?read_mask)),
    % rdwr permission on different ACEs
    ?assertEqual(ok, fslogic_acl:check_permission([Ace1, Ace2], User1, ?read_mask bor ?write_mask)),
    % rwx permission, not allowed
    ?assertEqual(?VEPERM, catch fslogic_acl:check_permission([Ace1, Ace2], User1, ?read_mask bor ?write_mask bor ?execute_mask)),
    % x permission, not allowed
    ?assertEqual(?VEPERM, catch fslogic_acl:check_permission([Ace1, Ace2], User1, ?execute_mask)),

    Id2 = <<"id2">>,
    User2 = #user{global_id = Id2},
    Ace3 = #accesscontrolentity{acetype = ?deny_mask, aceflags = ?no_flags_mask, identifier = Id2, acemask = ?read_mask},
    Ace4 = #accesscontrolentity{acetype = ?deny_mask, aceflags = ?no_flags_mask, identifier = Id1, acemask = ?read_mask},
    % read permission, with denying someone's else read
    ?assertEqual(ok, fslogic_acl:check_permission([Ace3, Ace1, Ace2], User1, ?read_mask)),
    % read permission, with denying read
    ?assertEqual(?VEPERM, catch fslogic_acl:check_permission([Ace2, Ace4, Ace1], User2, ?read_mask bor ?write_mask)).

check_group_permission_test() ->
    Id1 = <<"id1">>,
    GId1 = <<"gid1">>,
    GId2 = <<"gid2">>,
    GId3 = <<"gid3">>,
    Groups1 = [#group_details{id = GId1}, #group_details{id=GId2}],
    User1 = #user{global_id = Id1, groups = Groups1},

    Ace1 = #accesscontrolentity{acetype = ?allow_mask, aceflags = ?identifier_group_mask, identifier = GId1, acemask = ?read_mask},
    Ace2 = #accesscontrolentity{acetype = ?allow_mask, aceflags = ?identifier_group_mask, identifier = GId2, acemask = ?write_mask},
    Ace3 = #accesscontrolentity{acetype = ?allow_mask, aceflags = ?identifier_group_mask, identifier = GId3, acemask = ?execute_mask},

    % read permission
    ?assertEqual(ok, fslogic_acl:check_permission([Ace1, Ace2, Ace3], User1, ?read_mask)),
    % rdwr permission on different ACEs
    ?assertEqual(ok, fslogic_acl:check_permission([Ace1, Ace2, Ace3], User1, ?read_mask bor ?write_mask)),
    % rwx permission, not allowed
    ?assertEqual(?VEPERM, catch fslogic_acl:check_permission([Ace1, Ace2, Ace3], User1, ?read_mask bor ?write_mask bor ?execute_mask)),
    % x permission, not allowed
    ?assertEqual(?VEPERM, catch fslogic_acl:check_permission([Ace1, Ace2, Ace3], User1, ?execute_mask)),
    % write, not allowed
    ?assertEqual(?VEPERM, catch fslogic_acl:check_permission([Ace1], User1, ?write_mask)),

    Id2 = <<"id2">>,
    User2 = #user{global_id = Id2},
    Ace4 = #accesscontrolentity{acetype = ?deny_mask, aceflags = ?no_flags_mask, identifier = GId1, acemask = ?read_mask},

    % user allow, group deny
    ?assertEqual(?VEPERM, catch fslogic_acl:check_permission([Ace2, Ace4], User1, ?read_mask bor ?write_mask)),
    % read & write allow from user and group ace
    ?assertEqual(ok, fslogic_acl:check_permission([Ace1, Ace4], User1, ?read_mask bor ?read_mask)),

    ?assertEqual(?VEPERM, catch fslogic_acl:check_permission([Ace1, Ace2], User2, ?read_mask bor ?write_mask)).

-endif.