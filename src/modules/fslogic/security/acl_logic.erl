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
-module(acl_logic).
-author("Tomasz Lichon").

-include("proto/oneprovider/provider_messages.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([ace_to_map/1, map_to_ace/1, from_acl_to_json_format/1,
    from_json_format_to_acl/1, check_permission/4]).
-export([ace_name_to_uid/1, uid_to_ace_name/1, gid_to_ace_name/1, ace_name_to_gid/1]).
-export([bitmask_to_binary/1, binary_to_bitmask/1]).

-type ace() :: #access_control_entity{}.
-type acl() :: #acl{}.

-export_type([ace/0, acl/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Traverses given ACL in order to check if a Principal (in our case GRUID),
%% has permissions specified in 'OperationMask' (according to this ACL)
%% @end
%%--------------------------------------------------------------------
-spec check_permission([ace()], od_user:doc(), non_neg_integer(),
    file_ctx:ctx()) -> ok | no_return().
check_permission([], _User, ?no_flags_mask, _FileCtx) -> ok;
check_permission([], _User, _OperationMask, _FileCtx) -> throw(?EACCES);
check_permission([ACE = #access_control_entity{
    identifier = ?owner
} | Rest],
    User = #document{key = UserId}, Operation, FileCtx
) ->

    {OwnerId, FileCtx2} = file_ctx:get_owner(FileCtx),
    case UserId of
        OwnerId ->
            check_permission([ACE#access_control_entity{identifier = OwnerId} | Rest],
                User, Operation, FileCtx2);
        _ ->
            check_permission(Rest, User, Operation, FileCtx2)
    end;
check_permission([ACE = #access_control_entity{
    identifier = ?group,
    aceflags = Flags
} | Rest],
    User, Operation, FileCtx
) ->
    {GroupOwnerId, FileCtx2} = file_ctx:get_group_owner(FileCtx),
    check_permission([ACE#access_control_entity{
        identifier = GroupOwnerId,
        aceflags = Flags bor ?identifier_group_mask
    } | Rest], User, Operation, FileCtx2);
check_permission([ACE = #access_control_entity{
    identifier = ?everyone
} | Rest],
    User = #document{key = UserId}, Operation, FileCtx
) ->
    check_permission([ACE#access_control_entity{identifier = UserId} | Rest],
        User, Operation, FileCtx);
check_permission([#access_control_entity{
    acetype = Type,
    identifier = GroupId,
    aceflags = Flags,
    acemask = AceMask
} | Rest],
    User = #document{value = #od_user{eff_groups = Groups}},
    Operation, FileCtx) when ?has_flag(Flags, ?identifier_group_mask) ->

    case is_list(Groups) andalso lists:member(GroupId, Groups) of
        false ->
            check_permission(Rest, User, Operation, FileCtx); % if no group matches, ignore this ace
        true -> case Type of
            ?allow_mask ->
                case (Operation band AceMask) of
                    Operation -> ok;
                    OtherAllowedBits ->
                        check_permission(Rest, User, Operation bxor OtherAllowedBits, FileCtx)
                end;
            ?deny_mask ->
                case (Operation band AceMask) of
                    ?no_flags_mask ->
                        check_permission(Rest, User, Operation, FileCtx);
                    _ -> throw(?EACCES)
                end
        end
    end;
check_permission([#access_control_entity{
    acetype = ?allow_mask,
    identifier = SameUserId,
    acemask = AceMask
} | Rest],
    User = #document{key = SameUserId},
    Operation, FileCtx) ->

    case (Operation band AceMask) of
        Operation -> ok;
        OtherAllowedBits ->
            check_permission(Rest, User, Operation bxor OtherAllowedBits, FileCtx)
    end;
check_permission([#access_control_entity{acetype = ?deny_mask, identifier = SameUserId, acemask = AceMask} | Rest], #document{key = SameUserId} = User, Operation, FileCtx) ->
    case (Operation band AceMask) of
        ?no_flags_mask -> check_permission(Rest, User, Operation, FileCtx);
        _ -> throw(?EACCES)
    end;
check_permission([#access_control_entity{} | Rest], User = #document{}, Operation, FileCtx) ->
    check_permission(Rest, User, Operation, FileCtx).

%%--------------------------------------------------------------------
%% @doc
%% Parses list of access control entities to format suitable for jiffy:encode
%% @end
%%--------------------------------------------------------------------
-spec from_acl_to_json_format(Acl :: [ace()]) ->
    custom_metadata:json_array().
from_acl_to_json_format(Acl) ->
    [ace_to_map(Ace) || Ace <- Acl].

%%--------------------------------------------------------------------
%% @doc Parses proplist decoded json obtained from jiffy:decode to
%% list of access control entities
%% @end
%%--------------------------------------------------------------------
-spec from_json_format_to_acl(custom_metadata:json_array()) -> [ace()].
from_json_format_to_acl(JsonAcl) ->
    [map_to_ace(AceProplist) || AceProplist <- JsonAcl].

%%--------------------------------------------------------------------
%% @doc Parses access control entity to format suitable for jiffy:encode
%%--------------------------------------------------------------------
-spec ace_to_map(ace()) -> custom_metadata:json_object().
ace_to_map(#access_control_entity{acetype = Type, aceflags = Flags, identifier = Who, acemask = AccessMask}) ->
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
-spec map_to_ace(custom_metadata:json_object()) -> ace().
map_to_ace(Map) ->
    Type = maps:get(<<"acetype">>, Map, undefined),
    Flags = maps:get(<<"aceflags">>, Map, undefined),
    Mask = maps:get(<<"acemask">>, Map, undefined),
    Name = maps:get(<<"identifier">>, Map, undefined),

    Acetype = type_to_bitmask(Type),
    Aceflags = flags_to_bitmask(Flags),
    Acemask = mask_to_bitmask(Mask),
    Identifier = case ?has_flag(Aceflags, ?identifier_group_mask) of
        true -> ace_name_to_gid(Name);
        false -> ace_name_to_uid(Name)
    end,

    #access_control_entity{acetype = Acetype, aceflags = Aceflags, acemask = Acemask, identifier = Identifier}.

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

%%--------------------------------------------------------------------
%% @doc map acetype to bitmask
%%--------------------------------------------------------------------
-spec type_to_bitmask(binary()) -> non_neg_integer().
type_to_bitmask(<<"0x", _/binary>> = Type) -> binary_to_bitmask(Type);
type_to_bitmask(?allow) -> ?allow_mask;
type_to_bitmask(?deny) -> ?deny_mask;
type_to_bitmask(?audit) -> ?audit_mask.

%%--------------------------------------------------------------------
%% @doc maps coma separated binary of aceflags to bitmask
%%--------------------------------------------------------------------
-spec flags_to_bitmask(binary() | [binary()]) -> non_neg_integer().
flags_to_bitmask(<<"0x", _/binary>> = Flags) -> binary_to_bitmask(Flags);
flags_to_bitmask(Flags) when is_binary(Flags) ->
    flags_to_bitmask(csv_to_binary_list(Flags));
flags_to_bitmask([]) -> ?no_flags_mask;
flags_to_bitmask([?no_flags | Rest]) ->
    ?no_flags_mask bor flags_to_bitmask(Rest);
flags_to_bitmask([?identifier_group | Rest]) ->
    ?identifier_group_mask bor flags_to_bitmask(Rest).

%%--------------------------------------------------------------------
%% @doc maps coma separated binary of permissions to bitmask
%%--------------------------------------------------------------------
-spec mask_to_bitmask(binary() | [binary()]) -> non_neg_integer().
mask_to_bitmask(<<"0x", _/binary>> = Mask) -> binary_to_bitmask(Mask);
mask_to_bitmask(MaskNames) when is_binary(MaskNames) ->
    FlagList = lists:map(fun utils:trim_spaces/1, binary:split(MaskNames, <<",">>, [global])),
    mask_to_bitmask(FlagList);
mask_to_bitmask([]) -> ?no_flags_mask;
mask_to_bitmask([?all_perms | Rest]) ->
    ?all_perms_mask bor mask_to_bitmask(Rest);
mask_to_bitmask([?rw | Rest]) -> ?rw_mask bor mask_to_bitmask(Rest);
mask_to_bitmask([?read | Rest]) -> ?read_mask bor mask_to_bitmask(Rest);
mask_to_bitmask([?write | Rest]) -> ?write_mask bor mask_to_bitmask(Rest);
mask_to_bitmask([?read_object | Rest]) ->
    ?read_object_mask bor mask_to_bitmask(Rest);
mask_to_bitmask([?list_container | Rest]) ->
    ?list_container_mask bor mask_to_bitmask(Rest);
mask_to_bitmask([?write_object | Rest]) ->
    ?write_object_mask bor mask_to_bitmask(Rest);
mask_to_bitmask([?add_object | Rest]) ->
    ?add_object_mask bor mask_to_bitmask(Rest);
mask_to_bitmask([?append_data | Rest]) ->
    ?append_data_mask bor mask_to_bitmask(Rest);
mask_to_bitmask([?add_subcontainer | Rest]) ->
    ?add_subcontainer_mask bor mask_to_bitmask(Rest);
mask_to_bitmask([?read_metadata | Rest]) ->
    ?read_metadata_mask bor mask_to_bitmask(Rest);
mask_to_bitmask([?write_metadata | Rest]) ->
    ?write_metadata_mask bor mask_to_bitmask(Rest);
mask_to_bitmask([?execute | Rest]) -> ?execute_mask bor mask_to_bitmask(Rest);
mask_to_bitmask([?traverse_container | Rest]) ->
    ?traverse_container_mask bor mask_to_bitmask(Rest);
mask_to_bitmask([?delete_object | Rest]) ->
    ?delete_object_mask bor mask_to_bitmask(Rest);
mask_to_bitmask([?delete_subcontainer | Rest]) ->
    ?delete_subcontainer_mask bor mask_to_bitmask(Rest);
mask_to_bitmask([?read_attributes | Rest]) ->
    ?read_attributes_mask bor mask_to_bitmask(Rest);
mask_to_bitmask([?write_attributes | Rest]) ->
    ?write_attributes_mask bor mask_to_bitmask(Rest);
mask_to_bitmask([?delete | Rest]) -> ?delete_mask bor mask_to_bitmask(Rest);
mask_to_bitmask([?read_acl | Rest]) -> ?read_acl_mask bor mask_to_bitmask(Rest);
mask_to_bitmask([?write_acl | Rest]) ->
    ?write_acl_mask bor mask_to_bitmask(Rest);
mask_to_bitmask([?write_owner | Rest]) ->
    ?write_owner_mask bor mask_to_bitmask(Rest).

%%--------------------------------------------------------------------
%% @doc converts coma separated binary to list of binaries,
%% i. e. csv_to_binary_list(<<"a, b">>) -> [<<"a">>, <<"b">>]
%% @end
%%--------------------------------------------------------------------
-spec csv_to_binary_list(binary()) -> [binary()].
csv_to_binary_list(BinaryCsv) ->
    lists:map(fun utils:trim_spaces/1, binary:split(BinaryCsv, <<",">>, [global])).