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

-include("modules/datastore/datastore.hrl").
-include("modules/fslogic/fslogic_acl.hrl").
-include_lib("ctool/include/posix/errors.hrl").

%% API
%% -export([ace_to_proplist/1, proplist_to_ace/1, from_acl_to_json_format/1, from_json_fromat_to_acl/1, get_virtual_acl/2, check_permission/3]).
-export([ace_to_proplist/1, proplist_to_ace/1, from_acl_to_json_format/1, from_json_fromat_to_acl/1, check_permission/3]).
-export([name_to_gruid/1]).

%%%===================================================================
%%% API
%%%===================================================================

%% %%--------------------------------------------------------------------
%% %% @doc Converts posix permission of file, to ACL format. Such acl is called virtual
%% %% because it is based only on file perms and it is not stored in db.
%% %% @end
%% %%--------------------------------------------------------------------
%% -spec get_virtual_acl(FullfileName :: string(), FileDoc :: record(db_document)) -> [#accesscontrolentity{}].
%% get_virtual_acl(FullfileName, FileDoc) ->
%%     #db_document{record = #file{perms = Perms, uid = Uid}} = FileDoc,
%%     {ok, #space_info{users = Users}} = fslogic_utils:get_space_info_for_path(FullfileName),
%%     {ok, #db_document{record = #user{global_id = OwnerGlobalId}}} = fslogic_objects:get_user({uuid, Uid}),
%%     OwnerPerms = posix_perms_to_acl_mask(Perms, true, true),
%%     OwnerAce = #accesscontrolentity{acetype = ?allow_mask, aceflags = ?no_flags_mask, identifier = utils:ensure_binary(OwnerGlobalId), acemask = OwnerPerms},
%%     RestAceList = [ #accesscontrolentity{
%%         acetype = ?allow_mask,
%%         aceflags = ?no_flags_mask,
%%         identifier = utils:ensure_binary(UserGlobalId),
%%         acemask = posix_perms_to_acl_mask(Perms, false, true)
%%     } || UserGlobalId <- Users -- [utils:ensure_binary(OwnerGlobalId)]],
%%     case Uid of
%%         "0" -> RestAceList;
%%         _ -> [OwnerAce | RestAceList]
%%     end.

%%--------------------------------------------------------------------
%% @doc Traverses given ACL in order to check if a Principal (in our case GRUID),
%% has permissions specified in 'OperationMask' (according to this ACL)
%% @end
%%--------------------------------------------------------------------
-spec check_permission(ACL :: [#accesscontrolentity{}], User :: #document{value :: #onedata_user{}}, OperationMask :: non_neg_integer() | read | write | execute) -> ok | no_return().
check_permission(ACL, User, read) -> check_permission(ACL, User, ?read_mask);
check_permission(ACL, User, write) -> check_permission(ACL, User, ?write_mask);
check_permission(ACL, User, execute) ->
    check_permission(ACL, User, ?execute_mask);
check_permission([], _User, ?no_flags_mask) -> ok;
check_permission([], _User, _OperationMask) -> throw(?EPERM);
check_permission([#accesscontrolentity{acetype = Type, identifier = GroupId, aceflags = Flags, acemask = AceMask} | Rest],
  #document{value = #onedata_user{group_ids = Groups}} = User, Operation)
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
                         _ -> throw(?EPERM)
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
        _ -> throw(?EPERM)
    end;
check_permission([#accesscontrolentity{} | Rest], User = #document{}, Operation) ->
    check_permission(Rest, User, Operation).

%%--------------------------------------------------------------------
%% @doc
%% Parses list of access control entities to format suitable for mochijson2:encode
%% @end
%%--------------------------------------------------------------------
-spec from_acl_to_json_format(Acl :: [#accesscontrolentity{}]) -> Result when
    Result :: list().
from_acl_to_json_format(Acl) ->
    [ace_to_proplist(Ace) || Ace <- Acl].

%%--------------------------------------------------------------------
%% @doc Parses proplist decoded json obtained from mochijson2:decode to
%% list of access control entities
%% @end
%%--------------------------------------------------------------------
-spec from_json_fromat_to_acl(JsonAcl :: list()) -> [#accesscontrolentity{}].
from_json_fromat_to_acl(JsonAcl) ->
    [proplist_to_ace(AceProplist) || AceProplist <- JsonAcl].

%%--------------------------------------------------------------------
%% @doc Parses access control entity to format suitable for mochijson2:encode
%%--------------------------------------------------------------------
-spec ace_to_proplist(#accesscontrolentity{}) -> list().
ace_to_proplist(#accesscontrolentity{acetype = Type, aceflags = Flags, identifier = Who, acemask = AccessMask}) ->
    [
        {<<"acetype">>, bitmask_to_type(Type)},
        {<<"identifier">>,
            case ?has_flag(Type, ?identifier_group_mask) of
                true -> gid_to_group_name(Who);
                false -> gruid_to_name(Who)
            end},
        {<<"aceflags">>, binary_list_to_csv(bitmask_to_flag_list(Flags))},
        {<<"acemask">>, binary_list_to_csv(bitmask_to_perm_list(AccessMask))}
    ].

%%--------------------------------------------------------------------
%% @doc Parses proplist decoded json obtained from mochijson2:decode to
%% access control entity
%% @end
%%--------------------------------------------------------------------
-spec proplist_to_ace(List :: list()) -> #accesscontrolentity{}.
proplist_to_ace(List) ->
    Type = proplists:get_value(<<"acetype">>, List),
    Flags = proplists:get_value(<<"aceflags">>, List),
    Mask = proplists:get_value(<<"acemask">>, List),
    Name = proplists:get_value(<<"identifier">>, List),

    Acetype = type_to_bitmask(Type),
    Aceflags = flags_to_bitmask(Flags),
    Acemask = perm_list_to_bitmask(Mask),
    Identifier = case ?has_flag(Aceflags, ?identifier_group_mask) of
             true -> group_name_to_gid(Name);
             false -> name_to_gruid(Name)
         end,

    #accesscontrolentity{acetype = Acetype, aceflags = Aceflags, acemask = Acemask, identifier = Identifier}.

%%--------------------------------------------------------------------
%% @doc Transforms global id to acl name representation (name and hash suffix)
%% i. e. "fif3nhh238hdfg33f3" -> "John Dow#fif3n"
%% @end
%%--------------------------------------------------------------------
-spec gruid_to_name(Uid :: binary()) -> binary().
gruid_to_name(Uid) ->
    {ok, #document{value = #onedata_user{name = Name}}} = onedata_user:get(Uid),
    <<Name/binary,"#", Uid/binary>>.

%%--------------------------------------------------------------------
%% @doc Transforms acl name representation (name and hash suffix) to global id
%% i. e. "John Dow#fif3n" -> "fif3nhh238hdfg33f3"
%% @end
%%--------------------------------------------------------------------
-spec name_to_gruid(Name :: binary()) -> binary().
name_to_gruid(AceName) ->
    [_UserName, Uid] = binary:split(AceName, <<"#">>, [global]),
    Uid.

%%--------------------------------------------------------------------
%% @doc Transforms global group id to acl group name representation (name and hash suffix)
%% i. e. "fif3nhh238hdfg33f3" -> "group1#fif3n"
%% @end
%%--------------------------------------------------------------------
-spec gid_to_group_name(GRGroupId :: binary()) -> binary().
gid_to_group_name(GroupId) ->
    {ok, #document{value = #onedata_group{name = Name}}} = onedata_group:get(GroupId),
    <<Name/binary,"#", GroupId/binary>>.

%%--------------------------------------------------------------------
%% @doc Transforms acl group name representation (name and hash suffix) to global id
%% i. e. "group1#fif3n" -> "fif3nhh238hdfg33f3"
%% @end
%%--------------------------------------------------------------------
-spec group_name_to_gid(Name :: binary()) -> binary().
group_name_to_gid(AceName) ->
    [_GroupName, GroupId] = binary:split(AceName, <<"#">>, [global]),
    GroupId.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc maps bitmask to acetype
%%--------------------------------------------------------------------
-spec bitmask_to_type(non_neg_integer()) -> binary().
bitmask_to_type(?allow_mask) -> ?allow;
bitmask_to_type(?deny_mask) -> ?deny;
bitmask_to_type(?audit_mask) -> ?audit;
bitmask_to_type(_) -> undefined.

%%--------------------------------------------------------------------
%% @doc maps bitmask to aceflags
%%--------------------------------------------------------------------
-spec bitmask_to_flag_list(non_neg_integer()) -> [binary()].
bitmask_to_flag_list(Hex) -> lists:reverse(bitmask_to_flag_list2(Hex, [])).
bitmask_to_flag_list2(Hex, List) when ?has_flag(Hex, ?identifier_group_mask) ->
    bitmask_to_flag_list2(Hex bxor ?identifier_group_mask, [?identifier_group | List]);
bitmask_to_flag_list2(?no_flags_mask, []) -> [?no_flags];
bitmask_to_flag_list2(?no_flags_mask, List) -> List;
bitmask_to_flag_list2(_, _) -> undefined.

%%--------------------------------------------------------------------
%% @doc maps bitmask to perm list
%%--------------------------------------------------------------------
-spec bitmask_to_perm_list(non_neg_integer()) -> [binary()].
bitmask_to_perm_list(Hex) -> lists:reverse(bitmask_to_perm_list2(Hex, [])).
bitmask_to_perm_list2(Hex, List) when ?has_flag(Hex, ?read_mask) ->
    bitmask_to_perm_list2(Hex bxor ?read_mask, [?read | List]);
bitmask_to_perm_list2(Hex, List) when ?has_flag(Hex, ?write_mask) ->
    bitmask_to_perm_list2(Hex bxor ?write_mask, [?write | List]);
bitmask_to_perm_list2(Hex, List) when ?has_flag(Hex, ?execute_mask) ->
    bitmask_to_perm_list2(Hex bxor ?execute_mask, [?execute | List]);
bitmask_to_perm_list2(?no_flags_mask, List) -> List;
bitmask_to_perm_list2(_, _) -> undefined.

%%--------------------------------------------------------------------
%% @doc map acetype to bitmask
%%--------------------------------------------------------------------
-spec type_to_bitmask(binary()) -> non_neg_integer().
type_to_bitmask(?allow) -> ?allow_mask;
type_to_bitmask(?deny) -> ?deny_mask;
type_to_bitmask(?audit) -> ?audit_mask.

%%--------------------------------------------------------------------
%% @doc maps coma separated binary of aceflags to bitmask
%%--------------------------------------------------------------------
-spec flags_to_bitmask(binary()) -> non_neg_integer().
flags_to_bitmask(Flags) when is_binary(Flags) -> flags_to_bitmask(csv_to_binary_list(Flags));
flags_to_bitmask([]) -> ?no_flags_mask;
flags_to_bitmask([?no_flags | Rest]) -> ?no_flags_mask bor flags_to_bitmask(Rest);
flags_to_bitmask([?identifier_group | Rest]) -> ?identifier_group_mask bor flags_to_bitmask(Rest).

%%--------------------------------------------------------------------
%% @doc maps coma separated binary of permissions to bitmask
%%--------------------------------------------------------------------
-spec perm_list_to_bitmask(binary()) -> non_neg_integer().
perm_list_to_bitmask(MaskNames) when is_binary(MaskNames) ->
    FlagList = lists:map(fun utils:trim_spaces/1, binary:split(MaskNames, <<",">>, [global])),
    perm_list_to_bitmask(FlagList);
perm_list_to_bitmask([]) -> ?no_flags_mask;
perm_list_to_bitmask([?read | Rest]) -> ?read_mask bor perm_list_to_bitmask(Rest);
perm_list_to_bitmask([?write | Rest]) -> ?write_mask bor perm_list_to_bitmask(Rest);
perm_list_to_bitmask([?execute | Rest]) -> ?execute_mask bor perm_list_to_bitmask(Rest).

%%--------------------------------------------------------------------
%% @doc converts list of binaries to one, coma separated binary.
%% i. e. binary_list_to_csv(<<"a">>, <<"b">>) -> <<"a, b">>
%% @end
%%--------------------------------------------------------------------
-spec binary_list_to_csv([binary()]) -> binary().
binary_list_to_csv(List) ->
    lists:foldl(
        fun(Name, <<"">>) -> Name;
            (Name, Acc) -> <<Acc/binary, ", ", Name/binary>>
        end, <<"">>, List).

%%--------------------------------------------------------------------
%% @doc converts coma separated binary to list of binaries,
%% i. e. binary_list_to_csv(<<"a, b">>) -> [<<"a">>, <<"b">>]
%% @end
%%--------------------------------------------------------------------
-spec csv_to_binary_list([binary()]) -> binary().
csv_to_binary_list(BinaryCsv) ->
    lists:map(fun utils:trim_spaces/1, binary:split(BinaryCsv, <<",">>, [global])).

%% %%--------------------------------------------------------------------
%% %% @doc converts posix perm mask, to acl perm mask
%% %%--------------------------------------------------------------------
%% -spec posix_perms_to_acl_mask(PosixPerms :: non_neg_integer(), FileOwner :: boolean(), GroupOwner :: boolean()) -> non_neg_integer().
%% posix_perms_to_acl_mask(PosixPerms, FileOwner, GroupOwner) ->
%%     (case fslogic_perms:has_permission(read, PosixPerms, FileOwner, GroupOwner) of true -> ?read_mask; false -> ?no_flags_mask end) bor
%%     (case fslogic_perms:has_permission(write, PosixPerms, FileOwner, GroupOwner) of true -> ?write_mask; false -> ?no_flags_mask end) bor
%%     (case fslogic_perms:has_permission(execute, PosixPerms, FileOwner, GroupOwner) of true -> ?execute_mask; false -> ?no_flags_mask end).
