%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module provides utility functions for access control entry
%%% (#access_control_entity{}), or ace in short, management. The
%%% overall structure of ace is described in cdmi 1.1.1 spec book 16.1.
%%% As for implemented subset of flags and masks they listed in acl.hrl.
%%% @end
%%%--------------------------------------------------------------------
-module(ace).
-author("Bartosz Walkowicz").

-include("modules/auth/acl.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/errors.hrl").

-type ace() :: #access_control_entity{}.
-type bitmask() :: non_neg_integer().

-export_type([ace/0, bitmask/0]).

%% API
-export([
    is_applicable/3,
    check_against/2,

    from_json/2, to_json/2,
    validate/2
]).

-define(ALL_FLAGS_BITMASK, (?no_flags_mask bor ?identifier_group_mask)).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Checks whether given ace for specified file can be used to verify
%% permissions of specified user.
%% @end
%%--------------------------------------------------------------------
-spec is_applicable(od_user:doc(), file_ctx:ctx(), ace()) ->
    {boolean(), file_ctx:ctx()}.
is_applicable(#document{key = UserId}, FileCtx, #access_control_entity{
    identifier = ?owner
}) ->
    {OwnerId, FileCtx2} = file_ctx:get_owner(FileCtx),
    {OwnerId == UserId, FileCtx2};

is_applicable(UserDoc, FileCtx, #access_control_entity{
    identifier = ?group
}) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    {user_logic:has_eff_space(UserDoc, SpaceId), FileCtx};

is_applicable(_UserDoc, FileCtx, #access_control_entity{
    identifier = ?everyone
}) ->
    {true, FileCtx};

is_applicable(#document{key = ?GUEST_USER_ID}, FileCtx, #access_control_entity{
    identifier = ?anonymous
}) ->
    {true, FileCtx};

is_applicable(#document{value = User}, FileCtx, #access_control_entity{
    identifier = GroupId,
    aceflags = AceFlagsBitmask
}) when ?has_flag(AceFlagsBitmask, ?identifier_group_mask) ->
    {lists:member(GroupId, User#od_user.eff_groups), FileCtx};

is_applicable(#document{key = UserId}, FileCtx, #access_control_entity{
    identifier = UserId
}) ->
    {true, FileCtx};

is_applicable(_, FileCtx, _) ->
    {false, FileCtx}.


%%--------------------------------------------------------------------
%% @doc
%% Checks if given ace permits or denies (depending on ace type) specified
%% operations.
%% In case of 'allow' ace it returns `allowed` when all operations are
%% explicitly permitted or `{inconclusive, OperationsToBeAllowed}` for
%% operations which can't be authoritatively allowed by this ace.
%% In case of 'deny' ace it returns `denied` if even one operation is
%% forbidden or `{inconclusive, Operations}` if none of operations can
%% be authoritatively denied.
%% @end
%%--------------------------------------------------------------------
-spec check_against(bitmask(), ace()) ->
    allowed | {inconclusive, bitmask()} | denied.
check_against(Operations, #access_control_entity{
    acetype = ?allow_mask,
    acemask = AceMask
}) ->
    case (Operations band AceMask) of
        Operations -> allowed;
        AllowedOperations -> {inconclusive, Operations bxor AllowedOperations}
    end;
check_against(Operations, #access_control_entity{
    acetype = ?deny_mask,
    acemask = AceMask
}) ->
    case (Operations band AceMask) of
        ?no_flags_mask -> {inconclusive, Operations};
        _ -> denied
    end.


-spec from_json(Data :: map(), Format :: gui | cdmi) -> ace().
from_json(#{
    <<"aceType">> := AceType,
    <<"identifier">> := Identifier,
    <<"aceFlags">> := AceFlagsBitmask,
    <<"aceMask">> := AceMaskBitmask
}, gui) ->
    #access_control_entity{
        acetype = case AceType of
            ?allow -> ?allow_mask;
            ?deny -> ?deny_mask
        end,
        aceflags = AceFlagsBitmask,
        identifier = Identifier,
        acemask = AceMaskBitmask
    };

from_json(#{
    <<"acetype">> := AceType,
    <<"identifier">> := Identifier0,
    <<"aceflags">> := AceFlags,
    <<"acemask">> := AceMask
}, cdmi) ->
    {Identifier, Name} = decode_cdmi_identifier(Identifier0),

    #access_control_entity{
        acetype = cdmi_acetype_to_bitmask(AceType),
        aceflags = cdmi_aceflags_to_bitmask(AceFlags),
        identifier = Identifier,
        name = Name,
        acemask = cdmi_acemask_to_bitmask(AceMask)
    }.


-spec to_json(ace(), Format :: gui | cdmi) -> map().
to_json(#access_control_entity{
    acetype = Type,
    aceflags = Flags,
    identifier = Identifier,
    acemask = AccessMask
}, gui) ->
    #{
        <<"aceType">> => case Type of
            ?allow_mask -> ?allow;
            ?deny_mask -> ?deny
        end,
        <<"identifier">> => Identifier,
        <<"aceFlags">> => Flags,
        <<"aceMask">> => AccessMask
    };

to_json(#access_control_entity{
    acetype = Type,
    aceflags = Flags,
    identifier = Identifier,
    name = Name,
    acemask = Mask
}, cdmi) ->
    #{
        <<"acetype">> => bitmask_to_binary(Type),
        <<"identifier">> => encode_cdmi_identifier(Identifier, Name),
        <<"aceflags">> => bitmask_to_binary(Flags),
        <<"acemask">> => bitmask_to_binary(Mask)
    }.


-spec validate(ace(), FileType :: file | dir) -> ok | no_return().
validate(#access_control_entity{
    acetype = Type,
    aceflags = Flags,
    acemask = Mask
}, FileType) ->
    ValidType = lists:member(Type, [?allow_mask, ?deny_mask]),
    ValidFlags = ?has_flag(?ALL_FLAGS_BITMASK, Flags),
    ValidMask = case FileType of
        file -> ?has_flag(?all_object_perms_mask, Mask);
        dir -> ?has_flag(?all_container_perms_mask, Mask)
    end,

    case ValidType andalso ValidFlags andalso ValidMask of
        true -> ok;
        false -> throw({error, ?EINVAL})
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec cdmi_acetype_to_bitmask(binary()) -> bitmask().
cdmi_acetype_to_bitmask(<<"0x", _/binary>> = AceType) ->
    binary_to_bitmask(AceType);
cdmi_acetype_to_bitmask(?allow) -> ?allow_mask;
cdmi_acetype_to_bitmask(?deny) -> ?deny_mask.


%% @private
-spec cdmi_aceflags_to_bitmask(binary() | [binary()]) -> bitmask().
cdmi_aceflags_to_bitmask(<<"0x", _/binary>> = AceFlags) ->
    binary_to_bitmask(AceFlags);
cdmi_aceflags_to_bitmask(AceFlagsBin) when is_binary(AceFlagsBin) ->
    cdmi_aceflags_to_bitmask(parse_csv(AceFlagsBin));
cdmi_aceflags_to_bitmask(AceFlagsList) ->
    lists:foldl(
        fun
            (?no_flags, Bitmask) -> Bitmask;
            (?identifier_group, BitMask) -> BitMask bor ?identifier_group_mask
        end,
        0,
        AceFlagsList
    ).


-spec cdmi_acemask_to_bitmask(binary() | [binary()]) -> bitmask().
cdmi_acemask_to_bitmask(<<"0x", _/binary>> = AceMask) ->
    binary_to_bitmask(AceMask);
cdmi_acemask_to_bitmask(PermsBin) when is_binary(PermsBin) ->
    cdmi_acemask_to_bitmask(parse_csv(PermsBin));
cdmi_acemask_to_bitmask(PermsList) ->
    % op doesn't differentiate between ?delete_object and ?delete_subcontainer
    % so they must be either specified both or none of them.
    HasDeleteObject = lists:member(?delete_object, PermsList),
    HasDeleteSubcontainer = lists:member(?delete_subcontainer, PermsList),
    case {HasDeleteObject, HasDeleteSubcontainer} of
        {false, false} -> ok;
        {true, true} -> ok;
        _ -> throw({error, ?EINVAL})
    end,

    lists:foldl(fun(Perm, Bitmask) ->
        Bitmask bor permission_to_bitmask(Perm)
    end, 0, PermsList).


%% @private
-spec permission_to_bitmask(binary()) -> bitmask().
permission_to_bitmask(?list_container) -> ?list_container_mask;
permission_to_bitmask(?read_object) -> ?read_object_mask;
permission_to_bitmask(?write_object) -> ?write_object_mask;
permission_to_bitmask(?add_object) -> ?add_object_mask;
permission_to_bitmask(?add_subcontainer) -> ?add_subcontainer_mask;
permission_to_bitmask(?read_metadata) -> ?read_metadata_mask;
permission_to_bitmask(?write_metadata) -> ?write_metadata_mask;
permission_to_bitmask(?traverse_container) -> ?traverse_container_mask;
permission_to_bitmask(?delete) -> ?delete_mask;
permission_to_bitmask(?delete_object) -> ?delete_child_mask;
permission_to_bitmask(?delete_subcontainer) -> ?delete_child_mask;
permission_to_bitmask(?read_attributes) -> ?read_attributes_mask;
permission_to_bitmask(?write_attributes) -> ?write_attributes_mask;
permission_to_bitmask(?read_acl) -> ?read_acl_mask;
permission_to_bitmask(?write_acl) -> ?write_acl_mask.


%% @private
-spec encode_cdmi_identifier(Id :: binary(), Name :: undefined | binary()) ->
    CdmiIdentifier :: binary().
encode_cdmi_identifier(Id, undefined) -> Id;
encode_cdmi_identifier(Id, Name) -> <<Name/binary, "#", Id/binary>>.


%% @private
-spec decode_cdmi_identifier(CdmiIdentifier :: binary()) ->
    {Id :: binary(), Name :: undefined | binary()}.
decode_cdmi_identifier(CdmiIdentifier) ->
    case binary:split(CdmiIdentifier, <<"#">>, [global]) of
        [Name, Id] -> {Id, Name};
        [Id] -> {Id, undefined}
    end.


%% @private
-spec bitmask_to_binary(bitmask()) -> binary().
bitmask_to_binary(Mask) -> <<"0x", (integer_to_binary(Mask, 16))/binary>>.


%% @private
-spec binary_to_bitmask(binary()) -> bitmask().
binary_to_bitmask(<<"0x", Mask/binary>>) -> binary_to_integer(Mask, 16).


%% @private
-spec parse_csv(binary()) -> [binary()].
parse_csv(Bin) ->
    lists:map(fun utils:trim_spaces/1, binary:split(Bin, <<",">>, [global])).
