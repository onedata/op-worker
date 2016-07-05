%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Util functions converting ACLs for gui purposes.
%%% @end
%%%--------------------------------------------------------------------
-module(acl_utils).
-author("Tomasz Lichon").

-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").

%% API
-export([acl_to_json/2, json_to_acl/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Convert acl to gui compatible json.
%%--------------------------------------------------------------------
-spec acl_to_json(binary(), [#accesscontrolentity{}]) -> list().
acl_to_json(FileId, Acl) ->
    AclJson =
        lists:map(
            fun(#accesscontrolentity{aceflags = Flag, acemask = Mask, acetype = Type, identifier = Identifier}) ->
                [
                    {<<"type">>, type_enum(Type)},
                    {<<"permissions">>, Mask}
                ] ++ subject(Flag, Identifier)
            end, Acl),
    [
        {<<"id">>, FileId},
        {<<"file">>, FileId},
        {<<"acl">>, AclJson}
    ].

%%--------------------------------------------------------------------
%% @doc Convert gui compatible json to acl.
%%--------------------------------------------------------------------
-spec json_to_acl(list()) -> [#accesscontrolentity{}].
json_to_acl(Json) ->
    AclJson = proplists:get_value(<<"acl">>, Json),
    lists:map(fun(Ace) ->
        Type = proplists:get_value(<<"type">>, Ace),
        Mask = proplists:get_value(<<"permissions">>, Ace),
        Subject = proplists:get_value(<<"subject">>, Ace),
        User = proplists:get_value(<<"user">>, Ace),
        Group = proplists:get_value(<<"group">>, Ace),

        {AceFlags, Identifier} = who(Subject, User, Group),
        #accesscontrolentity{
            acetype = type_mask(Type),
            acemask = Mask,
            aceflags = AceFlags,
            identifier = Identifier
        }
    end, AclJson).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Convert acl type mask to type name.
%% @end
%%--------------------------------------------------------------------
-spec type_enum(non_neg_integer()) -> binary().
type_enum(?allow_mask) ->
    ?allow;
type_enum(?deny_mask) ->
    ?deny;
type_enum(?audit_mask) ->
    ?audit.


%%--------------------------------------------------------------------
%% @doc
%% Convert acl type name to type mask.
%% @end
%%--------------------------------------------------------------------
-spec type_mask(binary()) -> non_neg_integer().
type_mask(?allow) ->
    ?allow_mask;
type_mask(?deny) ->
    ?deny_mask;
type_mask(?audit) ->
    ?audit_mask.

%%--------------------------------------------------------------------
%% @doc
%% Convert acl flag and identifier to subject fields informing who is affected by ACE.
%% @end
%%--------------------------------------------------------------------
-spec subject(non_neg_integer(), binary()) -> list().
subject(_, ?everyone) ->
    [
        {<<"subject">>, <<"everyone">>},
        {<<"user">>, null},
        {<<"group">>, null}
    ];
subject(_, ?group) ->
    [
        {<<"subject">>, <<"everyone">>}, % group is treated as everyone also
        {<<"user">>, null},
        {<<"group">>, null}
    ];
subject(_, ?owner) ->
    [
        {<<"subject">>, <<"owner">>},
        {<<"user">>, null},
        {<<"group">>, null}
    ];
subject(?identifier_group_mask, Id) ->
    [
        {<<"subject">>, <<"group">>},
        {<<"user">>, null},
        {<<"group">>, Id}
    ];
subject(_, Id) ->
    [
        {<<"subject">>, <<"user">>},
        {<<"user">>, Id},
        {<<"group">>, null}
    ].

%%--------------------------------------------------------------------
%% @doc
%% Convert acl subject fields to flag and identifier
%% @end
%%--------------------------------------------------------------------
-spec who(Subject :: binary(), UserId :: binary(), GroupId :: binary()) ->
    {AceFlags :: non_neg_integer(), Identifier :: binary()}.
who(<<"everyone">>, _, _) ->
    {?no_flags_mask, ?everyone};
who(<<"owner">>, _, _) ->
    {?no_flags_mask, ?owner};
who(<<"user">>, UserId, _) ->
    {?no_flags_mask, UserId};
who(<<"group">>, _, GroupId) ->
    {?identifier_group_mask, GroupId}.
