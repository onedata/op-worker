%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module provides fslogic access control list utility
%%% functions
%%% @end
%%%--------------------------------------------------------------------
-module(fslogic_acl).
-author("Tomasz Lichon").

-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("ctool/include/posix/errors.hrl").

%% API
-export([ace_to_map/1, map_to_ace/1, from_acl_to_json_format/1, from_json_format_to_acl/1, check_permission/3]).
-export([ace_name_to_uid/1, uid_to_ace_name/1, gid_to_ace_name/1, ace_name_to_gid/1]).
-export([bitmask_to_binary/1, binary_to_bitmask/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Traverses given ACL in order to check if a Principal (in our case GRUID),
%% has permissions specified in 'OperationMask' (according to this ACL)
%% @end
%%--------------------------------------------------------------------
-spec check_permission(ACL :: [#accesscontrolentity{}], User :: #document{value :: #od_user{}}, OperationMask :: non_neg_integer()) -> ok | no_return().
check_permission([], _User, ?no_flags_mask) -> ok;
check_permission([], _User, _OperationMask) -> throw(?EACCES);
check_permission([#accesscontrolentity{acetype = Type, identifier = GroupId, aceflags = Flags, acemask = AceMask} | Rest],
    #document{value = #od_user{effective_group_ids = Groups}} = User, Operation)
    when ?has_flag(Flags, ?identifier_group_mask) ->
    case is_list(Groups) andalso lists:member(GroupId, Groups) of
        false ->
            check_permission(Rest, User, Operation); % if no group matches, ignore this ace
        true -> case Type of
            ?allow_mask ->
                case (Operation band AceMask) of
                    Operation -> ok;
                    OtherAllowedBits ->
                        check_permission(Rest, User, Operation bxor OtherAllowedBits)
                end;
            ?deny_mask ->
                case (Operation band AceMask) of
                    ?no_flags_mask ->
                        check_permission(Rest, User, Operation);
                    _ -> throw(?EACCES)
                end
        end
    end;
check_permission([#accesscontrolentity{acetype = ?allow_mask, identifier = SameUserId, acemask = AceMask} | Rest], #document{key = SameUserId} = User, Operation) ->
    case (Operation band AceMask) of
        Operation -> ok;
        OtherAllowedBits ->
            check_permission(Rest, User, Operation bxor OtherAllowedBits)
    end;
check_permission([#accesscontrolentity{acetype = ?deny_mask, identifier = SameUserId, acemask = AceMask} | Rest], #document{key = SameUserId} = User, Operation) ->
    case (Operation band AceMask) of
        ?no_flags_mask -> check_permission(Rest, User, Operation);
        _ -> throw(?EACCES)
    end;
check_permission([#accesscontrolentity{} | Rest], User = #document{}, Operation) ->
    check_permission(Rest, User, Operation).

%%--------------------------------------------------------------------
%% @doc
%% Parses list of access control entities to format suitable for jiffy:encode
%% @end
%%--------------------------------------------------------------------
-spec from_acl_to_json_format(Acl :: [#accesscontrolentity{}]) -> Result when
    Result :: list().
from_acl_to_json_format(Acl) ->
    [ace_to_map(Ace) || Ace <- Acl].

%%--------------------------------------------------------------------
%% @doc Parses proplist decoded json obtained from jiffy:decode to
%% list of access control entities
%% @end
%%--------------------------------------------------------------------
-spec from_json_format_to_acl(JsonAcl :: list()) -> [#accesscontrolentity{}].
from_json_format_to_acl(JsonAcl) ->
    [map_to_ace(AceProplist) || AceProplist <- JsonAcl].

%%--------------------------------------------------------------------
%% @doc Parses access control entity to format suitable for jiffy:encode
%%--------------------------------------------------------------------
-spec ace_to_map(#accesscontrolentity{}) -> maps:map().
ace_to_map(#accesscontrolentity{acetype = Type, aceflags = Flags, identifier = Who, acemask = AccessMask}) ->
    #{
        <<"acetype">> => bitmask_to_binary(Type),
        <<"identifier">> =>
        case ?has_flag(Flags, ?identifier_group_mask) of
            true -> gid_to_ace_name(Who);
            false -> uid_to_ace_name(Who)
        end,
        <<"aceflags">> => bitmask_to_binary(Flags),
        <<"acemask">> => bitmask_to_binary(AccessMask)
    }.

%%--------------------------------------------------------------------
%% @doc Parses map decoded json obtained from jiffy:decode to
%% access control entity
%% @end
%%--------------------------------------------------------------------
-spec map_to_ace(Map :: maps:map()) -> #accesscontrolentity{}.
map_to_ace(Map) ->
    Type = maps:get(<<"acetype">>, Map, undefined),
    Flags = maps:get(<<"aceflags">>, Map, undefined),
    Mask = maps:get(<<"acemask">>, Map, undefined),
    Name = maps:get(<<"identifier">>, Map, undefined),

    Acetype = binary_to_bitmask(Type),
    Aceflags = binary_to_bitmask(Flags),
    Acemask = binary_to_bitmask(Mask),
    Identifier = case ?has_flag(Aceflags, ?identifier_group_mask) of
        true -> ace_name_to_gid(Name);
        false -> ace_name_to_uid(Name)
    end,

    #accesscontrolentity{acetype = Acetype, aceflags = Aceflags, acemask = Acemask, identifier = Identifier}.

%%--------------------------------------------------------------------
%% @doc Transforms global id to acl name representation (name and hash suffix)
%% i. e. "fif3nhh238hdfg33f3" -> "John Dow#fif3n"
%% @end
%%--------------------------------------------------------------------
-spec uid_to_ace_name(Uid :: binary()) -> binary().
uid_to_ace_name(?owner) ->
    ?owner;
uid_to_ace_name(?everyone) ->
    ?everyone;
uid_to_ace_name(?group) ->
    ?group;
uid_to_ace_name(Uid) ->
    {ok, #document{value = #od_user{name = Name}}} = od_user:get(Uid),
    <<Name/binary, "#", Uid/binary>>.

%%--------------------------------------------------------------------
%% @doc Transforms acl name representation (name and hash suffix) to global id
%% i. e. "John Dow#fif3n" -> "fif3n"
%% @end
%%--------------------------------------------------------------------
-spec ace_name_to_uid(Name :: binary()) -> binary().
ace_name_to_uid(?owner) ->
    ?owner;
ace_name_to_uid(?everyone) ->
    ?everyone;
ace_name_to_uid(?group) ->
    ?group;
ace_name_to_uid(AceName) ->
    case binary:split(AceName, <<"#">>, [global]) of
        [_UserName, Uid] ->
            Uid;
        [Uid] ->
            Uid
    end.

%%--------------------------------------------------------------------
%% @doc Transforms global group id to acl group name representation (name and hash suffix)
%% i. e. "fif3nhh238hdfg33f3" -> "group1#fif3n"
%% @end
%%--------------------------------------------------------------------
-spec gid_to_ace_name(GRGroupId :: binary()) -> binary().
gid_to_ace_name(GroupId) ->
    {ok, #document{value = #od_group{name = Name}}} = od_group:get(GroupId),
    <<Name/binary, "#", GroupId/binary>>.

%%--------------------------------------------------------------------
%% @doc Transforms acl group name representation (name and hash suffix) to global id
%% i. e. "group1#fif3n" -> "fif3n"
%% @end
%%--------------------------------------------------------------------
-spec ace_name_to_gid(Name :: binary()) -> binary().
ace_name_to_gid(AceName) ->
    case binary:split(AceName, <<"#">>, [global]) of
        [_GroupName, GroupId] ->
            GroupId;
        [GroupId] ->
            GroupId
    end.

%%--------------------------------------------------------------------
%% @doc maps bitmask to binary format starting with "0x"
%%--------------------------------------------------------------------
-spec bitmask_to_binary(non_neg_integer()) -> binary().
bitmask_to_binary(Mask) -> <<"0x", (integer_to_binary(Mask, 16))/binary>>.

%%--------------------------------------------------------------------
%% @doc map acetype to bitmask
%%--------------------------------------------------------------------
-spec binary_to_bitmask(binary()) -> non_neg_integer().
binary_to_bitmask(<<"0x", Mask/binary>>) -> binary_to_integer(Mask, 16).

%%%===================================================================
%%% Internal functions
%%%===================================================================