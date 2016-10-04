%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Unit tests for fslogic_acl module.
%%% @end
%%%--------------------------------------------------------------------
-module(fslogic_acl_tests).

-ifdef(TEST).

-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/posix/errors.hrl").

acl_conversion_test() ->
    UserId = <<"UserId">>,
    UserName = <<"UserName">>,
    GroupId = <<"GroupId">>,
    GroupName = <<"GroupName">>,
    meck:new(od_user),
    meck:expect(od_user, get,
        fun(Id) when Id =:= UserId ->
            {ok, #document{value = #od_user{name = UserName}}}
        end
    ),
    meck:new(od_group),
    meck:expect(od_group, get,
        fun(Id) when Id =:= GroupId ->
            {ok, #document{value = #od_group{name = GroupName}}}
        end
    ),

    % when
    AceName1 = fslogic_acl:uid_to_ace_name(UserId),
    AceName2 = fslogic_acl:gid_to_ace_name(GroupId),

    % then
    ?assert(is_binary(AceName1)),
    ?assert(is_binary(AceName2)),

    % when
    Acl = fslogic_acl:from_json_format_to_acl(
        [
            #{
                <<"acetype">> => fslogic_acl:bitmask_to_binary(?allow_mask),
                <<"identifier">> => AceName1,
                <<"aceflags">> => fslogic_acl:bitmask_to_binary(?no_flags_mask),
                <<"acemask">> => fslogic_acl:bitmask_to_binary(?read_mask bor ?write_mask)
            },
            #{
                <<"acetype">> => fslogic_acl:bitmask_to_binary(?deny_mask),
                <<"identifier">> => AceName2,
                <<"aceflags">> => fslogic_acl:bitmask_to_binary(?identifier_group_mask),
                <<"acemask">> => fslogic_acl:bitmask_to_binary(?write_mask)
            }
        ]
    ),

    % then
    ?assertEqual(Acl, [
        #accesscontrolentity{acetype = ?allow_mask, identifier = UserId, aceflags = ?no_flags_mask, acemask = ?read_mask bor ?write_mask},
        #accesscontrolentity{acetype = ?deny_mask, identifier = GroupId, aceflags = ?identifier_group_mask, acemask = ?write_mask}
    ]),
    meck:validate(od_user),
    meck:validate(od_group),
    meck:unload().

check_permission_test() ->
    Id1 = <<"id1">>,
    User1 = #document{key = Id1, value = #od_user{}},
    Ace1 = #accesscontrolentity{acetype = ?allow_mask, aceflags = ?no_flags_mask, identifier = Id1, acemask = ?read_mask},
    Ace2 = #accesscontrolentity{acetype = ?allow_mask, aceflags = ?no_flags_mask, identifier = Id1, acemask = ?write_mask},
    % read permission
    ?assertEqual(ok, fslogic_acl:check_permission([Ace1, Ace2], User1, ?read_mask)),
    % rdwr permission on different ACEs
    ?assertEqual(ok, fslogic_acl:check_permission([Ace1, Ace2], User1, ?read_mask bor ?write_mask)),
    % rwx permission, not allowed
    ?assertEqual(?EACCES, catch fslogic_acl:check_permission([Ace1, Ace2], User1, ?read_mask bor ?write_mask bor ?execute_mask)),
    % x permission, not allowed
    ?assertEqual(?EACCES, catch fslogic_acl:check_permission([Ace1, Ace2], User1, ?execute_mask)),

    Id2 = <<"id2">>,
    User2 = #document{key = Id2, value = #od_user{}},
    Ace3 = #accesscontrolentity{acetype = ?deny_mask, aceflags = ?no_flags_mask, identifier = Id2, acemask = ?read_mask},
    Ace4 = #accesscontrolentity{acetype = ?deny_mask, aceflags = ?no_flags_mask, identifier = Id1, acemask = ?read_mask},
    % read permission, with denying someone's else read
    ?assertEqual(ok, fslogic_acl:check_permission([Ace3, Ace1, Ace2], User1, ?read_mask)),
    % read permission, with denying read
    ?assertEqual(?EACCES, catch fslogic_acl:check_permission([Ace2, Ace4, Ace1], User2, ?read_mask bor ?write_mask)).

check_group_permission_test() ->
    Id1 = <<"id1">>,
    GId1 = <<"gid1">>,
    GId2 = <<"gid2">>,
    GId3 = <<"gid3">>,
    Groups1 = [GId1, GId2],
    User1 = #document{key = Id1, value = #od_user{group_ids = Groups1}},

    Ace1 = #accesscontrolentity{acetype = ?allow_mask, aceflags = ?identifier_group_mask, identifier = GId1, acemask = ?read_mask},
    Ace2 = #accesscontrolentity{acetype = ?allow_mask, aceflags = ?identifier_group_mask, identifier = GId2, acemask = ?write_mask},
    Ace3 = #accesscontrolentity{acetype = ?allow_mask, aceflags = ?identifier_group_mask, identifier = GId3, acemask = ?execute_mask},

    % read permission
    ?assertEqual(ok, fslogic_acl:check_permission([Ace1, Ace2, Ace3], User1, ?read_mask)),
    % rdwr permission on different ACEs
    ?assertEqual(ok, fslogic_acl:check_permission([Ace1, Ace2, Ace3], User1, ?read_mask bor ?write_mask)),
    % rwx permission, not allowed
    ?assertEqual(?EACCES, catch fslogic_acl:check_permission([Ace1, Ace2, Ace3], User1, ?read_mask bor ?write_mask bor ?execute_mask)),
    % x permission, not allowed
    ?assertEqual(?EACCES, catch fslogic_acl:check_permission([Ace1, Ace2, Ace3], User1, ?execute_mask)),
    % write, not allowed
    ?assertEqual(?EACCES, catch fslogic_acl:check_permission([Ace1], User1, ?write_mask)),

    Id2 = <<"id2">>,
    User2 = #document{key = Id2, value = #od_user{}},
    Ace4 = #accesscontrolentity{acetype = ?deny_mask, aceflags = ?no_flags_mask, identifier = GId1, acemask = ?read_mask},

    % user allow, group deny
    ?assertEqual(?EACCES, catch fslogic_acl:check_permission([Ace2, Ace4], User1, ?read_mask bor ?write_mask)),
    % read & write allow from od_user and group ace
    ?assertEqual(ok, fslogic_acl:check_permission([Ace1, Ace4], User1, ?read_mask bor ?read_mask)),

    ?assertEqual(?EACCES, catch fslogic_acl:check_permission([Ace1, Ace2], User2, ?read_mask bor ?write_mask)).

-endif.