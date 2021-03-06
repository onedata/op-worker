%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Unit tests for acl module.
%%% @end
%%%--------------------------------------------------------------------
-module(acl_tests).

-ifdef(TEST).

-include("modules/fslogic/data_access_control.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("ctool/include/errors.hrl").


bitmask_acl_conversion_test() ->
    UserId = <<"UserId">>,
    UserName = <<"UserName">>,
    GroupId = <<"GroupId">>,
    GroupName = <<"GroupName">>,

    lists:foreach(fun(Perms) ->
        Ace1 = #access_control_entity{
            acetype = ?allow_mask,
            identifier = UserId,
            name = UserName,
            aceflags = ?no_flags_mask,
            acemask = Perms
        },
        Ace2 = #access_control_entity{
            acetype = ?deny_mask,
            identifier = GroupId,
            name = GroupName,
            aceflags = ?identifier_group_mask,
            acemask = ?complement_flags(Perms)
        },

        % when
        Acl = acl:from_json([ace:to_json(Ace1, cdmi), ace:to_json(Ace2, cdmi)], cdmi),

        % then
        ?assertEqual(Acl, [Ace1, Ace2])

    end, [
        ?read_object_mask,
        ?list_container_mask,
        ?write_object_mask,
        ?add_object_mask,
        ?add_subcontainer_mask,
        ?read_metadata_mask,
        ?write_metadata_mask,
        ?traverse_container_mask,
        ?delete_child_mask,
        ?read_attributes_mask,
        ?write_attributes_mask,
        ?delete_mask,
        ?read_acl_mask,
        ?write_acl_mask
    ]).


binary_acl_conversion_test() ->
    UserId = <<"UserId">>,
    UserName = <<"UserName">>,
    GroupId = <<"GroupId">>,
    GroupName = <<"GroupName">>,

    lists:foreach(fun({PermBin, PermBitmask}) ->
        % when
        Acl = acl:from_json(
            [
                #{
                    <<"acetype">> => <<"ALLOW">>,
                    <<"identifier">> => <<UserName/binary, "#", UserId/binary>>,
                    <<"aceflags">> => <<"NO_FLAGS">>,
                    <<"acemask">> => <<"READ_OBJECT, WRITE_OBJECT">>
                },
                #{
                    <<"acetype">> => <<"DENY">>,
                    <<"identifier">> => <<GroupName/binary, "#", GroupId/binary>>,
                    <<"aceflags">> => <<"IDENTIFIER_GROUP">>,
                    <<"acemask">> => PermBin
                }
            ],
            cdmi
        ),

        % then
        ?assertEqual(Acl, [
            #access_control_entity{
                acetype = ?allow_mask,
                identifier = UserId,
                name = UserName,
                aceflags = ?no_flags_mask,
                acemask = ?read_object_mask bor ?write_object_mask
            },
            #access_control_entity{
                acetype = ?deny_mask,
                identifier = GroupId,
                name = GroupName,
                aceflags = ?identifier_group_mask,
                acemask = PermBitmask
            }
        ])

    end, [
        {?read_object, ?read_object_mask},
        {?list_container, ?list_container_mask},
        {?write_object, ?write_object_mask},
        {?add_object, ?add_object_mask},
        {?add_subcontainer, ?add_subcontainer_mask},
        {?read_metadata, ?read_metadata_mask},
        {?write_metadata, ?write_metadata_mask},
        {?traverse_container, ?traverse_container_mask},
        {?read_attributes, ?read_attributes_mask},
        {?write_attributes, ?write_attributes_mask},
        {?delete, ?delete_mask},
        {?read_acl, ?read_acl_mask},
        {?write_acl, ?write_acl_mask}
    ]).


check_normal_user_permission_test_() ->
    UserId1 = <<"id1">>,
    User1 = #document{key = UserId1, value = #od_user{}},
    UserId2 = <<"id2">>,
    User2 = #document{key = UserId2, value = #od_user{}},

    FileGuid = file_id:pack_guid(<<"file_guid">>, <<"space_id">>),
    FileCtx = file_ctx:new_by_guid(FileGuid),

    Ace1 = #access_control_entity{
        acetype = ?allow_mask,
        identifier = UserId1,
        aceflags = ?no_flags_mask,
        acemask = ?read_mask
    },
    Ace2 = #access_control_entity{
        acetype = ?allow_mask,
        identifier = UserId1,
        aceflags = ?no_flags_mask,
        acemask = ?write_mask
    },
    Ace3 = #access_control_entity{
        acetype = ?deny_mask,
        identifier = UserId2,
        aceflags = ?no_flags_mask,
        acemask = ?read_mask
    },
    Ace4 = #access_control_entity{
        acetype = ?deny_mask,
        identifier = UserId1,
        aceflags = ?no_flags_mask,
        acemask = ?read_mask
    },

    F = fun(Acl, User, Perms) ->
        acl:check_acl(Acl, User, FileCtx, Perms, #user_access_check_progress{
            finished_step = ?ACL_CHECK(0),
            allowed = ?no_flags_mask,
            denied = ?no_flags_mask
        })
    end,

    [
        ?_assertMatch(
            {allowed, _, #user_access_check_progress{
                finished_step = ?ACL_CHECK(1),
                allowed = ?read_mask,
                denied = ?no_flags_mask
            }},
            F([Ace1, Ace2], User1, ?read_mask)
        ),
        ?_assertMatch(
            {allowed, _, #user_access_check_progress{
                finished_step = ?ACL_CHECK(2),
                allowed = ?read_mask bor ?write_mask,
                denied = ?no_flags_mask
            }},
            F([Ace1, Ace2], User1, ?read_mask bor ?write_mask)
        ),
        ?_assertMatch(
            {denied, _, #user_access_check_progress{
                finished_step = ?ACL_CHECK(3),
                allowed = ?read_mask bor ?write_mask,
                denied = bnot (?read_mask bor ?write_mask)
            }},
            F([Ace1, Ace2], User1, ?read_mask bor ?write_mask bor ?traverse_container_mask)
        ),
        ?_assertMatch(
            {denied, _, #user_access_check_progress{
                finished_step = ?ACL_CHECK(3),
                allowed = ?read_mask bor ?write_mask,
                denied = bnot (?read_mask bor ?write_mask)
            }},
            F([Ace1, Ace2], User1, ?traverse_container_mask)
        ),
        ?_assertMatch(
            {allowed, _, #user_access_check_progress{
                finished_step = ?ACL_CHECK(2),
                allowed = ?read_mask,
                denied = 0
            }},
            F([Ace3, Ace1, Ace2], User1, ?read_mask)
        ),
        ?_assertMatch(
            {denied, _, #user_access_check_progress{
                finished_step = ?ACL_CHECK(4),
                allowed = ?no_flags_mask,
                denied = bnot ?no_flags_mask
            }},
            F([Ace2, Ace4, Ace1], User2, ?read_mask bor ?write_mask)
        )
    ].


check_normal_group_permission_test_() ->
    FileGuid = file_id:pack_guid(<<"file_guid">>, <<"space_id">>),
    FileCtx = file_ctx:new_by_guid(FileGuid),

    GroupId1 = <<"gid1">>,
    GroupId2 = <<"gid2">>,
    GroupId3 = <<"gid3">>,

    UserId1 = <<"id1">>,
    User1 = #document{key = UserId1, value = #od_user{eff_groups = [GroupId1, GroupId2]}},
    UserId2 = <<"id2">>,
    User2 = #document{key = UserId2, value = #od_user{}},

    Ace1 = #access_control_entity{
        acetype = ?allow_mask,
        identifier = GroupId1,
        aceflags = ?identifier_group_mask,
        acemask = ?read_mask
    },
    Ace2 = #access_control_entity{
        acetype = ?allow_mask,
        identifier = GroupId2,
        aceflags = ?identifier_group_mask,
        acemask = ?write_mask
    },
    Ace3 = #access_control_entity{
        acetype = ?allow_mask,
        identifier = GroupId3,
        aceflags = ?identifier_group_mask,
        acemask = ?traverse_container_mask
    },
    Ace4 = #access_control_entity{
        acetype = ?deny_mask,
        identifier = GroupId1,
        aceflags = ?identifier_group_mask,
        acemask = ?read_mask
    },

    F = fun(Acl, User, Perms) ->
        acl:check_acl(Acl, User, FileCtx, Perms, #user_access_check_progress{
            finished_step = ?ACL_CHECK(0),
            allowed = ?no_flags_mask,
            denied = ?no_flags_mask
        })
    end,

    [
        ?_assertMatch(
            {allowed, _, #user_access_check_progress{
                finished_step = ?ACL_CHECK(1),
                allowed = ?read_mask,
                denied = ?no_flags_mask
            }},
            F([Ace1, Ace3, Ace2], User1, ?read_mask)
        ),
        ?_assertMatch(
            {allowed, _, #user_access_check_progress{
                finished_step = ?ACL_CHECK(3),
                allowed = ?read_mask bor ?write_mask,
                denied = ?no_flags_mask
            }},
            F([Ace1, Ace3, Ace2], User1, ?read_mask bor ?write_mask)
        ),
        ?_assertMatch(
            {denied, _, #user_access_check_progress{
                finished_step = ?ACL_CHECK(4),
                allowed = ?read_mask bor ?write_mask,
                denied = bnot (?read_mask bor ?write_mask)
            }},
            F([Ace1, Ace2, Ace3], User1, ?read_mask bor ?write_mask bor ?traverse_container_mask)
        ),
        ?_assertMatch(
            {denied, _, #user_access_check_progress{
                finished_step = ?ACL_CHECK(4),
                allowed = ?read_mask bor ?write_mask,
                denied = bnot (?read_mask bor ?write_mask)
            }},
            F([Ace1, Ace2, Ace3], User1, ?traverse_container_mask)
        ),
        ?_assertMatch(
            {denied, _, #user_access_check_progress{
                finished_step = ?ACL_CHECK(2),
                allowed = ?read_mask,
                denied = bnot ?read_mask
            }},
            F([Ace1], User1, ?write_mask)
        ),
        ?_assertMatch(
            {denied, _, #user_access_check_progress{
                finished_step = ?ACL_CHECK(2),
                allowed = ?write_mask,
                denied = ?read_mask
            }},
            F([Ace2, Ace4], User1, ?read_mask bor ?write_mask)
        ),
        ?_assertMatch(
            {allowed, _, #user_access_check_progress{
                finished_step = ?ACL_CHECK(3),
                allowed = ?write_mask,
                denied = ?read_mask
            }},
            F([Ace4, Ace1, Ace2], User1, ?write_mask)
        ),
        ?_assertMatch(
            {denied, _, #user_access_check_progress{
                finished_step = ?ACL_CHECK(3),
                allowed = ?no_flags_mask,
                denied = bnot ?no_flags_mask
            }},
            F([Ace1, Ace2], User2, ?read_mask bor ?write_mask)
        )
    ].


check_owner_principal_permission_test_() ->
    UserId = <<"UserId">>,
    User = #document{key = UserId, value = #od_user{}},

    FileGuid = file_id:pack_guid(<<"file_guid">>, <<"space_id">>),
    FileMeta = #file_meta{owner = UserId},
    FileDoc = #document{key = FileGuid, value = FileMeta},
    FileCtx = file_ctx:new_by_doc(FileDoc, <<"SpaceId">>),

    Ace1 = #access_control_entity{
        acetype = ?allow_mask,
        aceflags = ?no_flags_mask,
        identifier = ?owner,
        acemask = ?read_mask
    },
    Ace2 = #access_control_entity{
        acetype = ?allow_mask,
        aceflags = ?no_flags_mask,
        identifier = ?owner,
        acemask = ?write_mask
    },

    F = fun(Acl, User, Perms) ->
        acl:check_acl(Acl, User, FileCtx, Perms, #user_access_check_progress{
            finished_step = ?ACL_CHECK(0),
            allowed = ?no_flags_mask,
            denied = ?no_flags_mask
        })
    end,

    [
        ?_assertMatch(
            {allowed, _, #user_access_check_progress{
                finished_step = ?ACL_CHECK(1),
                allowed = ?read_mask,
                denied = ?no_flags_mask
            }},
            F([Ace1, Ace2], User, ?read_mask)
        ),
        ?_assertMatch(
            {allowed, _, #user_access_check_progress{
                finished_step = ?ACL_CHECK(2),
                allowed = ?read_mask bor ?write_mask,
                denied = ?no_flags_mask
            }},
            F([Ace1, Ace2], User, ?read_mask bor ?write_mask)
        ),
        ?_assertMatch(
            {denied, _, #user_access_check_progress{
                finished_step = ?ACL_CHECK(3),
                allowed = ?read_mask bor ?write_mask,
                denied = bnot (?read_mask bor ?write_mask)
            }},
            F([Ace1, Ace2], User, ?read_mask bor ?write_mask bor ?traverse_container_mask)
        ),
        ?_assertMatch(
            {denied, _, #user_access_check_progress{
                finished_step = ?ACL_CHECK(3),
                allowed = ?read_mask bor ?write_mask,
                denied = bnot (?read_mask bor ?write_mask)
            }},
            F([Ace1, Ace2], User, ?traverse_container_mask)
        )
    ].


check_group_principal_permission_test_() ->
    SpaceId = <<"space">>,
    FileGuid = file_id:pack_guid(<<"file_guid">>, <<"space_id">>),
    FileDoc = #document{key = FileGuid, value = #file_meta{}},
    FileCtx = file_ctx:new_by_doc(FileDoc, SpaceId),

    UserId = <<"id1">>,
    User = #document{key = UserId, value = #od_user{eff_spaces = [SpaceId]}},

    Ace1 = #access_control_entity{
        acetype = ?allow_mask,
        aceflags = ?no_flags_mask,
        identifier = ?group,
        acemask = ?read_mask
    },
    Ace2 = #access_control_entity{
        acetype = ?deny_mask,
        aceflags = ?no_flags_mask,
        identifier = ?group,
        acemask = ?write_mask
    },

    F = fun(Acl, User, Perms) ->
        acl:check_acl(Acl, User, FileCtx, Perms, #user_access_check_progress{
            finished_step = ?ACL_CHECK(0),
            allowed = ?no_flags_mask,
            denied = ?no_flags_mask
        })
    end,

    [
        ?_assertMatch(
            {allowed, _, #user_access_check_progress{
                finished_step = ?ACL_CHECK(1),
                allowed = ?read_mask,
                denied = ?no_flags_mask
            }},
            F([Ace1, Ace2], User, ?read_mask)
        ),
        ?_assertMatch(
            {denied, _, #user_access_check_progress{
                finished_step = ?ACL_CHECK(2),
                allowed = ?read_mask,
                denied = ?write_mask
            }},
            F([Ace1, Ace2], User, ?read_mask bor ?write_mask)
        ),
        ?_assertMatch(
            {denied, _, #user_access_check_progress{
                finished_step = ?ACL_CHECK(2),
                allowed = ?read_mask,
                denied = ?write_mask
            }},
            F([Ace1, Ace2], User, ?read_mask bor ?write_mask bor ?traverse_container_mask)
        ),
        ?_assertMatch(
            {denied, _, #user_access_check_progress{
                finished_step = ?ACL_CHECK(3),
                allowed = ?read_mask,
                denied = bnot ?read_mask
            }},
            F([Ace1, Ace2], User, ?traverse_container_mask)
        )
    ].


check_everyone_principal_permission_test_() ->
    OwnerId = <<"id1">>,
    FileGuid = file_id:pack_guid(<<"file_guid">>, <<"space_id">>),
    FileMeta = #file_meta{owner = OwnerId},
    FileDoc = #document{key = FileGuid, value = FileMeta},
    FileCtx = file_ctx:new_by_doc(FileDoc, <<"space">>),

    UserId = <<"id2">>,
    User = #document{key = UserId, value = #od_user{}},

    Ace1 = #access_control_entity{
        acetype = ?allow_mask,
        aceflags = ?no_flags_mask,
        identifier = ?everyone,
        acemask = ?read_mask
    },
    Ace2 = #access_control_entity{
        acetype = ?deny_mask,
        aceflags = ?no_flags_mask,
        identifier = ?everyone,
        acemask = ?write_mask
    },

    F = fun(Acl, User, Perms) ->
        acl:check_acl(Acl, User, FileCtx, Perms, #user_access_check_progress{
            finished_step = ?ACL_CHECK(0),
            allowed = ?no_flags_mask,
            denied = ?no_flags_mask
        })
    end,

    [
        ?_assertMatch(
            {allowed, _, #user_access_check_progress{
                finished_step = ?ACL_CHECK(1),
                allowed = ?read_mask,
                denied = ?no_flags_mask
            }},
            F([Ace1, Ace2], User, ?read_mask)
        ),
        ?_assertMatch(
            {denied, _, #user_access_check_progress{
                finished_step = ?ACL_CHECK(2),
                allowed = ?read_mask,
                denied = ?write_mask
            }},
            F([Ace1, Ace2], User, ?read_mask bor ?write_mask)
        ),
        ?_assertMatch(
            {denied, _, #user_access_check_progress{
                finished_step = ?ACL_CHECK(2),
                allowed = ?read_mask,
                denied = ?write_mask
            }},
            F([Ace1, Ace2], User, ?read_mask bor ?write_mask bor ?traverse_container_mask)
        ),
        ?_assertMatch(
            {denied, _, #user_access_check_progress{
                finished_step = ?ACL_CHECK(3),
                allowed = ?read_mask,
                denied = bnot ?read_mask
            }},
            F([Ace1, Ace2], User, ?traverse_container_mask)
        )
    ].


check_anonymous_principal_permission_test_() ->
    FileGuid = file_id:pack_guid(<<"file_guid">>, <<"space_id">>),
    FileCtx = file_ctx:new_by_guid(FileGuid),

    UserId = <<"id2">>,
    User = #document{key = UserId, value = #od_user{}},
    Guest = #document{key = ?GUEST_USER_ID, value = #od_user{}},

    Ace1 = #access_control_entity{
        acetype = ?allow_mask,
        aceflags = ?no_flags_mask,
        identifier = ?anonymous,
        acemask = ?read_mask
    },
    Ace2 = #access_control_entity{
        acetype = ?deny_mask,
        aceflags = ?no_flags_mask,
        identifier = ?anonymous,
        acemask = ?write_mask
    },

    F = fun(Acl, User, Perms) ->
        acl:check_acl(Acl, User, FileCtx, Perms, #user_access_check_progress{
            finished_step = ?ACL_CHECK(0),
            allowed = ?no_flags_mask,
            denied = ?no_flags_mask
        })
    end,

    [
        ?_assertMatch(
            {allowed, _, #user_access_check_progress{
                finished_step = ?ACL_CHECK(1),
                allowed = ?read_mask,
                denied = ?no_flags_mask
            }},
            F([Ace1, Ace2], Guest, ?read_mask)
        ),
        ?_assertMatch(
            {denied, _, #user_access_check_progress{
                finished_step = ?ACL_CHECK(3),
                allowed = ?no_flags_mask,
                denied = bnot ?no_flags_mask
            }},
            F([Ace1, Ace2], User, ?read_mask)
        ),
        ?_assertMatch(
            {denied, _, #user_access_check_progress{
                finished_step = ?ACL_CHECK(2),
                allowed = ?read_mask,
                denied = ?write_mask
            }},
            F([Ace1, Ace2], Guest, ?read_mask bor ?write_mask)
        ),
        ?_assertMatch(
            {denied, _, #user_access_check_progress{
                finished_step = ?ACL_CHECK(2),
                allowed = ?read_mask,
                denied = ?write_mask
            }},
            F([Ace1, Ace2], Guest, ?read_mask bor ?write_mask bor ?traverse_container_mask)
        ),
        ?_assertMatch(
            {denied, _, #user_access_check_progress{
                finished_step = ?ACL_CHECK(3),
                allowed = ?read_mask,
                denied = bnot ?read_mask
            }},
            F([Ace1, Ace2], Guest, ?traverse_container_mask)
        )
    ].


-endif.
