%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Tests of gui acl utils, which converts acl to gui compatible format
%%% @end
%%%--------------------------------------------------------------------
-module(acl_utils_test).
-author("Tomasz Lichon").

-include_lib("eunit/include/eunit.hrl").
-include_lib("ctool/include/posix/acl.hrl").

-define(ACE(Type_, Mask_, Flag_, Id_),
    #access_control_entity{acetype = Type_,
        acemask = Mask_,
        aceflags = Flag_,
        identifier = Id_}
).

-define(JSON_ACE(Type__, Mask__, Subject__, User__, Group__), [
    {<<"type">>, Type__},
    {<<"permissions">>, Mask__},
    {<<"subject">>, Subject__},
    {<<"user">>, User__},
    {<<"group">>, Group__}
]).

user_acl_conversion_test() ->
    UserId = <<"userId">>,
    Mask = 17,
    Acl = [?ACE(?allow_mask, Mask, ?no_flags_mask, UserId)],

    Json = acl_utils:acl_to_json(Acl),
    DecodedAcl = acl_utils:json_to_acl(Json),

    ?assertEqual([?JSON_ACE(<<"allow">>, Mask, <<"user">>, UserId, null)],
        Json),
    ?assertEqual(Acl, DecodedAcl).

group_acl_conversion_test() ->
    GroupId = <<"groupId">>,
    Mask = 17,
    Acl = [?ACE(?allow_mask, Mask, ?identifier_group_mask, GroupId)],

    Json = acl_utils:acl_to_json(Acl),
    DecodedAcl = acl_utils:json_to_acl(Json),

    ?assertEqual([?JSON_ACE(<<"allow">>, Mask, <<"group">>, null, GroupId)],
        Json),
    ?assertEqual(Acl, DecodedAcl).

everyone_acl_conversion_test() ->
    Mask = 17,
    Acl = [?ACE(?allow_mask, Mask, ?no_flags_mask, ?everyone)],

    Json = acl_utils:acl_to_json(Acl),
    DecodedAcl = acl_utils:json_to_acl(Json),

    ?assertEqual([?JSON_ACE(<<"allow">>, Mask, <<"everyone">>, null, null)],
        Json),
    ?assertEqual(Acl, DecodedAcl).

owner_acl_conversion_test() ->
    Mask = 17,
    Acl = [?ACE(?allow_mask, Mask, ?no_flags_mask, ?owner)],

    Json = acl_utils:acl_to_json(Acl),
    DecodedAcl = acl_utils:json_to_acl(Json),

    ?assertEqual([?JSON_ACE(<<"allow">>, Mask, <<"owner">>, null, null)],
        Json),
    ?assertEqual(Acl, DecodedAcl).


multiple_acl_conversion_test() ->
    UserId = <<"userId">>,
    GroupId = <<"groupId">>,
    Mask = 17,
    Acl = [
        ?ACE(?allow_mask, Mask, ?no_flags_mask, UserId),
        ?ACE(?deny_mask, Mask, ?identifier_group_mask, GroupId)
    ],

    Json = acl_utils:acl_to_json(Acl),
    DecodedAcl = acl_utils:json_to_acl(Json),

    ?assertEqual([
        ?JSON_ACE(<<"allow">>, Mask, <<"user">>, UserId, null),
        ?JSON_ACE(<<"deny">>, Mask, <<"group">>, null, GroupId)
    ], Json),
    ?assertEqual(Acl, DecodedAcl).
