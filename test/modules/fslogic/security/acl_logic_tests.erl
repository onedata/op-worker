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
-module(acl_logic_tests).

-ifdef(TEST).

-include("modules/datastore/datastore_models.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/errors.hrl").


bitmask_acl_conversion_test() ->
    UserId = <<"UserId">>,
    UserName = <<"UserName">>,
    GroupId = <<"GroupId">>,
    GroupName = <<"GroupName">>,

    Ace1 = #access_control_entity{
        acetype = ?allow_mask,
        identifier = UserId,
        name = UserName,
        aceflags = ?no_flags_mask,
        acemask = ?read_mask bor ?write_mask
    },
    Ace2 = #access_control_entity{
        acetype = ?deny_mask,
        identifier = GroupId,
        name = GroupName,
        aceflags = ?identifier_group_mask,
        acemask = ?write_mask
    },

    % when
    Acl = acl:from_json([ace:to_json(Ace1, cdmi), ace:to_json(Ace2, cdmi)], cdmi),

    % then
    ?assertEqual(Acl, [Ace1, Ace2]).


binary_acl_conversion_test() ->
    UserId = <<"UserId">>,
    UserName = <<"UserName">>,
    GroupId = <<"GroupId">>,
    GroupName = <<"GroupName">>,

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
                <<"acemask">> => <<"WRITE_OBJECT">>
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
            acemask = ?write_object_mask
        }
    ]).


check_permission_test() ->
    Id1 = <<"id1">>,
    FileGuid = <<"file_guid">>,
    FileCtx = file_ctx:new_by_guid(FileGuid),
    User1 = #document{key = Id1, value = #od_user{}},
    Ace1 = #access_control_entity{acetype = ?allow_mask, aceflags = ?no_flags_mask, identifier = Id1, acemask = ?read_mask},
    Ace2 = #access_control_entity{acetype = ?allow_mask, aceflags = ?no_flags_mask, identifier = Id1, acemask = ?write_mask},
    % read permission
    ?assertMatch({ok, _}, acl:assert_permitted([Ace1, Ace2], User1, ?read_mask, FileCtx)),
    % rdwr permission on different ACEs
    ?assertMatch({ok, _}, acl:assert_permitted([Ace1, Ace2], User1, ?read_mask bor ?write_mask, FileCtx)),
    % rwx permission, not allowed
    ?assertEqual(?EACCES, catch acl:assert_permitted([Ace1, Ace2], User1, ?read_mask bor ?write_mask bor ?traverse_container_mask, FileCtx)),
    % x permission, not allowed
    ?assertEqual(?EACCES, catch acl:assert_permitted([Ace1, Ace2], User1, ?traverse_container_mask, FileCtx)),

    Id2 = <<"id2">>,
    User2 = #document{key = Id2, value = #od_user{}},
    Ace3 = #access_control_entity{acetype = ?deny_mask, aceflags = ?no_flags_mask, identifier = Id2, acemask = ?read_mask},
    Ace4 = #access_control_entity{acetype = ?deny_mask, aceflags = ?no_flags_mask, identifier = Id1, acemask = ?read_mask},
    % read permission, with denying someone's else read
    ?assertMatch({ok, _}, acl:assert_permitted([Ace3, Ace1, Ace2], User1, ?read_mask, FileCtx)),
    % read permission, with denying read
    ?assertEqual(?EACCES, catch acl:assert_permitted([Ace2, Ace4, Ace1], User2, ?read_mask bor ?write_mask, FileCtx)).


check_group_permission_test() ->
    Id1 = <<"id1">>,
    FileGuid = <<"file_guid">>,
    FileCtx = file_ctx:new_by_guid(FileGuid),
    GId1 = <<"gid1">>,
    GId2 = <<"gid2">>,
    GId3 = <<"gid3">>,
    Groups1 = [GId1, GId2],
    User1 = #document{key = Id1, value = #od_user{eff_groups = Groups1}},

    Ace1 = #access_control_entity{acetype = ?allow_mask, aceflags = ?identifier_group_mask, identifier = GId1, acemask = ?read_mask},
    Ace2 = #access_control_entity{acetype = ?allow_mask, aceflags = ?identifier_group_mask, identifier = GId2, acemask = ?write_mask},
    Ace3 = #access_control_entity{acetype = ?allow_mask, aceflags = ?identifier_group_mask, identifier = GId3, acemask = ?traverse_container_mask},

    % read permission
    ?assertMatch({ok, _}, acl:assert_permitted([Ace1, Ace2, Ace3], User1, ?read_mask, FileCtx)),
    % rdwr permission on different ACEs
    ?assertMatch({ok, _}, acl:assert_permitted([Ace1, Ace2, Ace3], User1, ?read_mask bor ?write_mask, FileCtx)),
    % rwx permission, not allowed
    ?assertEqual(?EACCES, catch acl:assert_permitted([Ace1, Ace2, Ace3], User1, ?read_mask bor ?write_mask bor ?traverse_container_mask, FileCtx)),
    % x permission, not allowed
    ?assertEqual(?EACCES, catch acl:assert_permitted([Ace1, Ace2, Ace3], User1, ?traverse_container_mask, FileCtx)),
    % write, not allowed
    ?assertEqual(?EACCES, catch acl:assert_permitted([Ace1], User1, ?write_mask, FileCtx)),

    Id2 = <<"id2">>,
    User2 = #document{key = Id2, value = #od_user{}},
    Ace4 = #access_control_entity{acetype = ?deny_mask, aceflags = ?no_flags_mask, identifier = GId1, acemask = ?read_mask},

    % user allow, group deny
    ?assertEqual(?EACCES, catch acl:assert_permitted([Ace2, Ace4], User1, ?read_mask bor ?write_mask, FileCtx)),
    % read & write allow from od_user and group ace
    ?assertMatch({ok, _}, acl:assert_permitted([Ace1, Ace4], User1, ?read_mask bor ?read_mask, FileCtx)),

    ?assertEqual(?EACCES, catch acl:assert_permitted([Ace1, Ace2], User2, ?read_mask bor ?write_mask, FileCtx)).


check_owner_principal_permission_test() ->
    Id1 = <<"id1">>,
    Principal = <<"OWNER@">>,
    FileGuid = <<"file_guid">>,
    FileMeta = #file_meta{owner = Id1},
    FileDoc = #document{key = FileGuid, value = FileMeta},
    FileCtx = file_ctx:new_by_doc(FileDoc, <<"space">>, undefined),
    meck:new(file_ctx, [passthrough]),
    meck:expect(file_ctx, get_file_doc, fun(Ctx) -> {FileDoc, Ctx} end),
    User1 = #document{key = Id1, value = #od_user{}},
    Ace5 = #access_control_entity{acetype = ?allow_mask, aceflags = ?no_flags_mask, identifier = Principal, acemask = ?read_mask},
    Ace6 = #access_control_entity{acetype = ?allow_mask, aceflags = ?no_flags_mask, identifier = Principal, acemask = ?write_mask},
    % read permission
    ?assertMatch({ok, _}, acl:assert_permitted([Ace5, Ace6], User1, ?read_mask, FileCtx)),
    % rdwr permission on different ACEs
    ?assertMatch({ok, _}, acl:assert_permitted([Ace5, Ace6], User1, ?read_mask bor ?write_mask, FileCtx)),
    % rwx permission, not allowed
    ?assertEqual(?EACCES, catch acl:assert_permitted([Ace5, Ace6], User1, ?read_mask bor ?write_mask bor ?traverse_container_mask, FileCtx)),
    % x permission, not allowed
    ?assertEqual(?EACCES, catch acl:assert_permitted([Ace5, Ace6], User1, ?traverse_container_mask, FileCtx)),
    meck:validate(file_ctx),
    meck:unload().


check_group_principal_permission_test() ->
    Id1 = <<"id1">>,
    Gid1 = <<"gid1">>,
    Principal = <<"GROUP@">>,
    FileGuid = <<"file_guid">>,
    FileDoc = #document{key = FileGuid, value = #file_meta{}},
    FileCtx = file_ctx:new_by_doc(FileDoc, <<"space">>, undefined),
    meck:new(file_ctx, [passthrough]),
    meck:expect(file_ctx, get_file_doc, fun(Ctx) -> {FileDoc, Ctx} end),
    User1 = #document{key = Id1, value = #od_user{eff_groups = [Gid1]}},
    Ace5 = #access_control_entity{acetype = ?allow_mask, aceflags = ?no_flags_mask, identifier = Principal, acemask = ?read_mask},
    Ace6 = #access_control_entity{acetype = ?deny_mask, aceflags = ?no_flags_mask, identifier = Principal, acemask = ?write_mask},
    % read permission
    ?assertMatch({ok, _}, acl:assert_permitted([Ace5, Ace6], User1, ?read_mask, FileCtx)),
    % rdwr permission on different ACEs, not allowed
    ?assertEqual(?EACCES, catch acl:assert_permitted([Ace5, Ace6], User1, ?read_mask bor ?write_mask, FileCtx)),
    % rwx permission, not allowed
    ?assertEqual(?EACCES, catch acl:assert_permitted([Ace5, Ace6], User1, ?read_mask bor ?write_mask bor ?traverse_container_mask, FileCtx)),
    % x permission, not allowed
    ?assertEqual(?EACCES, catch acl:assert_permitted([Ace5, Ace6], User1, ?traverse_container_mask, FileCtx)),
    meck:validate(file_ctx),
    meck:unload().


check_everyone_principal_permission_test() ->
    Id1 = <<"id1">>,
    Id2 = <<"id2">>,
    Principal = <<"EVERYONE@">>,
    FileGuid = <<"file_guid">>,
    FileMeta = #file_meta{owner = Id1},
    FileDoc = #document{key = FileGuid, value = FileMeta},
    FileCtx = file_ctx:new_by_doc(FileDoc, <<"space">>, undefined),
    meck:new(file_ctx, [passthrough]),
    meck:expect(file_ctx, get_file_doc, fun(Ctx) -> {FileDoc, Ctx} end),
    User2 = #document{key = Id2, value = #od_user{}},
    Ace5 = #access_control_entity{acetype = ?allow_mask, aceflags = ?no_flags_mask, identifier = Principal, acemask = ?read_mask},
    Ace6 = #access_control_entity{acetype = ?deny_mask, aceflags = ?no_flags_mask, identifier = Principal, acemask = ?write_mask},
    % read permission
    ?assertMatch({ok, _}, acl:assert_permitted([Ace5, Ace6], User2, ?read_mask, FileCtx)),
    % rdwr permission on different ACEs, not allowed
    ?assertEqual(?EACCES, catch acl:assert_permitted([Ace5, Ace6], User2, ?read_mask bor ?write_mask, FileCtx)),
    % rwx permission, not allowed
    ?assertEqual(?EACCES, catch acl:assert_permitted([Ace5, Ace6], User2, ?read_mask bor ?write_mask bor ?traverse_container_mask, FileCtx)),
    % x permission, not allowed
    ?assertEqual(?EACCES, catch acl:assert_permitted([Ace5, Ace6], User2, ?traverse_container_mask, FileCtx)),
    meck:validate(file_ctx),
    meck:unload().

-endif.