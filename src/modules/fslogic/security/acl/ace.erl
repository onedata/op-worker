%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Utility functions for access control entity management.
%%% @end
%%%--------------------------------------------------------------------
-module(ace).
-author("Bartosz Walkowicz").

-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("ctool/include/posix/errors.hrl").

-type ace() :: #access_control_entity{}.
-type bitmask() :: non_neg_integer().

-export_type([ace/0, bitmask/0]).

%% API
-export([
    is_applicable/3,
    check_permission/2,

    from_json/2, to_json/2
]).


-define(ALL_FLAGS_BITMASK, (
    ?no_flags_mask bor
    ?identifier_group_mask
)).
-define(ALL_PERMS_BITMASK, (
    ?read_object_mask bor
    ?list_container_mask bor
    ?write_object_mask bor
    ?add_object_mask bor
    ?append_data_mask bor
    ?add_subcontainer_mask bor
    ?read_metadata_mask bor
    ?write_metadata_mask bor
    ?execute_mask bor
    ?traverse_container_mask bor
    ?delete_object_mask bor
    ?delete_subcontainer_mask bor
    ?read_attributes_mask bor
    ?write_attributes_mask bor
    ?delete_mask bor
    ?read_acl_mask bor
    ?write_acl_mask bor
    ?write_owner_mask
)).


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

is_applicable(#document{value = User}, FileCtx, #access_control_entity{
    identifier = ?group
}) ->
    {GroupOwnerId, FileCtx2} = file_ctx:get_group_owner(FileCtx),
    {lists:member(GroupOwnerId, User#od_user.eff_groups), FileCtx2};

is_applicable(_UserDoc, FileCtx, #access_control_entity{
    identifier = ?everyone
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
%% Checks if given ace permits specified operations.
%% In case of allow ace returns operations which have yet to be permitted or
%% denied by other ace or 'ok' if all are permitted by this one.
%% In case of deny ace throws ?EACCES if even one operation is forbidden.
%% @end
%%--------------------------------------------------------------------
-spec check_permission(bitmask(), ace()) -> ok | bitmask() | no_return().
check_permission(Operations, #access_control_entity{
    acetype = ?allow_mask,
    acemask = AceMask
}) ->
    case (Operations band AceMask) of
        Operations -> ok;
        AllowedOperations -> Operations bxor AllowedOperations
    end;
check_permission(Operations, #access_control_entity{
    acetype = ?deny_mask,
    acemask = AceMask
}) ->
    case (Operations band AceMask) of
        ?no_flags_mask -> Operations;
        _ -> throw(?EACCES)
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
            ?deny -> ?deny_mask;
            ?audit -> ?audit_mask
        end,
        aceflags = verify_aceflags_bitmask(AceFlagsBitmask),
        identifier = Identifier,
        acemask = verify_acemask_bitmask(AceMaskBitmask)
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
            ?deny_mask -> ?deny;
            ?audit_mask -> ?audit
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


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec cdmi_acetype_to_bitmask(binary()) -> bitmask().
cdmi_acetype_to_bitmask(<<"0x", _/binary>> = AceType) ->
    verify_acetype_bitmask(binary_to_bitmask(AceType));
cdmi_acetype_to_bitmask(?allow) -> ?allow_mask;
cdmi_acetype_to_bitmask(?deny) -> ?deny_mask;
cdmi_acetype_to_bitmask(?audit) -> ?audit_mask.


%% @private
-spec verify_acetype_bitmask(bitmask()) -> bitmask() | no_return().
verify_acetype_bitmask(?allow_mask) -> ?allow_mask;
verify_acetype_bitmask(?deny_mask) -> ?deny_mask;
verify_acetype_bitmask(?audit_mask) -> ?audit_mask;
verify_acetype_bitmask(_) -> throw({error, ?EINVAL}).


%% @private
-spec cdmi_aceflags_to_bitmask(binary() | [binary()]) -> bitmask().
cdmi_aceflags_to_bitmask(<<"0x", _/binary>> = AceFlags) ->
    verify_aceflags_bitmask(binary_to_bitmask(AceFlags));
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


%% @private
-spec verify_aceflags_bitmask(bitmask()) -> bitmask() | no_return().
verify_aceflags_bitmask(AceFlagsBitmask) ->
    case AceFlagsBitmask band ?ALL_FLAGS_BITMASK of
        AceFlagsBitmask -> AceFlagsBitmask;
        _ -> throw({error, ?EINVAL})
    end.


-spec cdmi_acemask_to_bitmask(binary() | [binary()]) -> bitmask().
cdmi_acemask_to_bitmask(<<"0x", _/binary>> = AceMask) ->
    verify_acemask_bitmask(binary_to_bitmask(AceMask));
cdmi_acemask_to_bitmask(PermsBin) when is_binary(PermsBin) ->
    cdmi_acemask_to_bitmask(parse_csv(PermsBin));
cdmi_acemask_to_bitmask(PermsList) ->
    lists:foldl(fun(Perm, Bitmask) ->
        Bitmask bor permission_to_bitmask(Perm)
    end, 0, PermsList).


%% @private
-spec verify_acemask_bitmask(bitmask()) -> bitmask() | no_return().
verify_acemask_bitmask(AceMaskBitmask) ->
    case AceMaskBitmask band ?ALL_PERMS_BITMASK of
        AceMaskBitmask -> AceMaskBitmask;
        _ -> throw({error, ?EINVAL})
    end.


%% @private
-spec permission_to_bitmask(binary()) -> bitmask().
permission_to_bitmask(?rw) -> ?rw_mask;
permission_to_bitmask(?read) -> ?read_mask;
permission_to_bitmask(?write) -> ?write_mask;
permission_to_bitmask(?list_container) -> ?list_container_mask;
permission_to_bitmask(?read_object) -> ?read_object_mask;
permission_to_bitmask(?write_object) -> ?write_object_mask;
permission_to_bitmask(?append_data) -> ?append_data_mask;
permission_to_bitmask(?add_object) -> ?add_object_mask;
permission_to_bitmask(?add_subcontainer) -> ?add_subcontainer_mask;
permission_to_bitmask(?read_metadata) -> ?read_metadata_mask;
permission_to_bitmask(?write_metadata) -> ?write_metadata_mask;
permission_to_bitmask(?execute) -> ?execute_mask;
permission_to_bitmask(?traverse_container) -> ?traverse_container_mask;
permission_to_bitmask(?delete) -> ?delete_mask;
permission_to_bitmask(?delete_object) -> ?delete_object_mask;
permission_to_bitmask(?delete_subcontainer) -> ?delete_subcontainer_mask;
permission_to_bitmask(?read_attributes) -> ?read_attributes_mask;
permission_to_bitmask(?write_attributes) -> ?write_attributes_mask;
permission_to_bitmask(?read_acl) -> ?read_acl_mask;
permission_to_bitmask(?write_acl) -> ?write_acl_mask;
permission_to_bitmask(?write_owner) -> ?write_owner_mask;
permission_to_bitmask(?all_perms) -> ?all_perms_mask.


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
