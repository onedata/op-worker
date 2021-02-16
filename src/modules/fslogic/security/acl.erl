%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Utility functions for access control list management.
%%% @end
%%%--------------------------------------------------------------------
-module(acl).
-author("Bartosz Walkowicz").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("ctool/include/errors.hrl").

-type acl() :: [ace:ace()].
-type permission() :: binary().         % Permissions defined in acl.hrl

-export_type([acl/0, permission/0]).

%% API
-export([
    check_acl/5,
%%    assert_permitted/4,
    add_names/1, strip_names/1,

    from_json/2, to_json/2,
    validate/2
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec check_acl(
    acl(), od_user:doc(), ace:bitmask(), file_ctx:ctx(), data_access_rights:user_perms_matrix()
) ->
    {allowed | denied, file_ctx:ctx(), data_access_rights:user_perms_matrix()}.
check_acl(_Acl, _User, ?no_flags_mask, FileCtx, UserPermsMatrix) ->
    {allowed, FileCtx, UserPermsMatrix};
check_acl([], _User, _Operations, FileCtx, {No, AllowedPerms, _DeniedPerms}) ->
    % After reaching then end of ACL all not explicitly granted perms are denied
    {denied, FileCtx, {No + 1, AllowedPerms, bnot AllowedPerms}};
check_acl([Ace | Rest], User, Operations, FileCtx, {No, PrevAllowedPerms, PrevDeniedPerms}) ->
    case ace:is_applicable(User, FileCtx, Ace) of
        {true, FileCtx2} ->
            case ace:check_against(Operations, PrevAllowedPerms, PrevDeniedPerms, Ace) of
                {inconclusive, LeftoverOperations, AllAllowedPerms, AllDeniedPerms} ->
                    check_acl(
                        Rest, User, LeftoverOperations, FileCtx2,
                        {No + 1, AllAllowedPerms, AllDeniedPerms}
                    );
                {AllowedOrDenied, AllAllowedPerms, AllDeniedPerms} ->
                    {AllowedOrDenied, FileCtx, {No + 1, AllAllowedPerms, AllDeniedPerms}}
            end;
        {false, FileCtx2} ->
            check_acl(
                Rest, User, Operations, FileCtx2,
                {No + 1, PrevAllowedPerms, PrevDeniedPerms}
            )
    end.

%%
%%-spec assert_permitted(acl(), od_user:doc(), ace:bitmask(), file_ctx:ctx()) ->
%%    {ok, file_ctx:ctx()} | no_return().
%%assert_permitted(_Acl, _User, ?no_flags_mask, FileCtx) ->
%%    {ok, FileCtx};
%%assert_permitted([], _User, _Operations, _FileCtx) ->
%%    throw(?EACCES);
%%assert_permitted([Ace | Rest], User, Operations, FileCtx) ->
%%    case ace:is_applicable(User, FileCtx, Ace) of
%%        {true, FileCtx2} ->
%%            case ace:check_against(Operations, Ace) of
%%                allowed ->
%%                    {ok, FileCtx2};
%%                {inconclusive, LeftoverOperations} ->
%%                    assert_permitted(Rest, User, LeftoverOperations, FileCtx2);
%%                denied ->
%%                    throw(?EACCES)
%%            end;
%%        {false, FileCtx2} ->
%%            assert_permitted(Rest, User, Operations, FileCtx2)
%%    end.


%%--------------------------------------------------------------------
%% @doc
%% Resolves name for given access_control_entity record based on identifier
%% value and identifier type (user or group).
%% @end
%%--------------------------------------------------------------------
-spec add_names(acl()) -> acl().
add_names(Acl) ->
    lists:map(
        fun(#access_control_entity{identifier = Id, aceflags = Flags} = Ace) ->
            Name = case ?has_flag(Flags, ?identifier_group_mask) of
                true -> group_id_to_ace_name(Id);
                false -> user_id_to_ace_name(Id)
            end,
            Ace#access_control_entity{name = Name}
        end, Acl).


-spec strip_names(acl()) -> acl().
strip_names(Acl) ->
    [Ace#access_control_entity{name = undefined} || Ace <- Acl].


-spec from_json([map()], Format :: gui | cdmi) -> acl() | no_return().
from_json(JsonAcl, Format) ->
    try
        [ace:from_json(JsonAce, Format) || JsonAce <- JsonAcl]
    catch Type:Reason ->
        ?debug_stacktrace("Failed to translate json(~p) to acl due to: ~p:~p", [
            Format, Type, Reason
        ]),
        throw({error, ?EINVAL})
    end.


-spec to_json(acl(), Format :: gui | cdmi) -> [map()] | no_return().
to_json(Acl, Format) ->
    try
        [ace:to_json(Ace, Format) || Ace <- Acl]
    catch Type:Reason ->
        ?debug_stacktrace("Failed to convert acl to json(~p) due to: ~p:~p", [
            Format, Type, Reason
        ]),
        throw({error, ?EINVAL})
    end.


-spec validate(acl(), FileType :: file | dir) -> ok | no_return().
validate(Acl, FileType) ->
    lists:foreach(fun(Ace) -> ace:validate(Ace, FileType) end, Acl).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec user_id_to_ace_name(od_user:id() | binary()) ->
    undefined | od_user:full_name().
user_id_to_ace_name(?owner) ->
    undefined;
user_id_to_ace_name(?group) ->
    undefined;
user_id_to_ace_name(?everyone) ->
    undefined;
user_id_to_ace_name(UserId) ->
    case user_logic:get_full_name(UserId) of
        {ok, FullName} -> FullName;
        {error, _} -> undefined
    end.


%% @private
-spec group_id_to_ace_name(od_group:id()) -> undefined | od_group:name().
group_id_to_ace_name(GroupId) ->
    case group_logic:get_name(GroupId) of
        {ok, Name} -> Name;
        {error, _} -> undefined
    end.
