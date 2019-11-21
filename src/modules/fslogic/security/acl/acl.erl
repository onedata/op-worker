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
    assert_permitted/4,
    add_names/1, strip_names/1,

    from_json/2, to_json/2,
    validate/2
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec assert_permitted(acl(), od_user:doc(), ace:bitmask(), file_ctx:ctx()) ->
    {ok, file_ctx:ctx()} | no_return().
assert_permitted(_Acl, _User, ?no_flags_mask, FileCtx) ->
    {ok, FileCtx};
assert_permitted([], _User, _Operations, _FileCtx) ->
    throw(?EACCES);
assert_permitted([Ace | Rest], User, Operations, FileCtx) ->
    case ace:is_applicable(User, FileCtx, Ace) of
        {true, FileCtx2} ->
            case ace:check_against(Operations, Ace) of
                allowed ->
                    {ok, FileCtx2};
                {inconclusive, LeftoverOperations} ->
                    assert_permitted(Rest, User, LeftoverOperations, FileCtx2);
                denied ->
                    throw(?EACCES)
            end;
        {false, FileCtx2} ->
            assert_permitted(Rest, User, Operations, FileCtx2)
    end.


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
                true -> gid_to_ace_name(Id);
                false -> uid_to_ace_name(Id)
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
-spec uid_to_ace_name(od_user:id() | binary()) ->
    undefined | od_user:full_name().
uid_to_ace_name(?owner) ->
    undefined;
uid_to_ace_name(?group) ->
    undefined;
uid_to_ace_name(?everyone) ->
    undefined;
uid_to_ace_name(UserId) ->
    case user_logic:get_full_name(?ROOT_SESS_ID, UserId) of
        {ok, FullName} -> FullName;
        {error, _} -> undefined
    end.


%% @private
-spec gid_to_ace_name(od_group:id()) -> undefined | od_group:name().
gid_to_ace_name(GroupId) ->
    case group_logic:get_name(?ROOT_SESS_ID, GroupId) of
        {ok, Name} -> Name;
        {error, _} -> undefined
    end.
