%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019-2021 ACK CYFRONET AGH
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

-include("modules/fslogic/data_access_control.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/errors.hrl").

-type ace() :: #access_control_entity{}.

-export_type([ace/0]).

%% API
-export([
    is_applicable/3,
    check_against/3,

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
}) when ?has_all_flags(AceFlagsBitmask, ?identifier_group_mask) ->
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
%% operations. The result can be either:
%% - 'allowed' meaning that all permissions have been granted,
%% - 'denied' meaning that at least one permission has been denied,
%% - 'inconclusive' meaning that some permissions may have been granted
%%    but not all of them (leftover permissions to be granted or denied are
%%    returned).
%% @end
%%--------------------------------------------------------------------
-spec check_against(
    data_access_control:bitmask(),
    ace(),
    data_access_control:user_access_check_progress()
) ->
    {
        allowed | denied | {inconclusive, LeftoverRequiredOps :: data_access_control:bitmask()},
        data_access_control:user_access_check_progress()
    }.
check_against(
    RequiredOps,
    #access_control_entity{acetype = ?allow_mask, acemask = AceMask},
    #user_access_check_progress{
        finished_step = ?ACL_CHECK(AceNo),
        allowed = PrevAllowedOps,
        denied = PrevDeniedOps
    } = UserAccessCheckProgress
) ->
    NewUserAccessCheckProgress = UserAccessCheckProgress#user_access_check_progress{
        finished_step = ?ACL_CHECK(AceNo + 1),
        allowed = ?set_flags(PrevAllowedOps, ?reset_flags(AceMask, PrevDeniedOps))
    },
    case ?reset_flags(RequiredOps, AceMask) of
        ?no_flags_mask ->
            {allowed, NewUserAccessCheckProgress};
        LeftoverRequiredOps ->
            {{inconclusive, LeftoverRequiredOps}, NewUserAccessCheckProgress}
    end;

check_against(
    RequiredOps,
    #access_control_entity{acetype = ?deny_mask, acemask = AceMask},
    #user_access_check_progress{
        finished_step = ?ACL_CHECK(AceNo),
        allowed = PrevGrantedOps,
        denied = PrevDeniedOps
    } = UserAccessCheckProgress
) ->
    NewUserAccessCheckProgress = UserAccessCheckProgress#user_access_check_progress{
        finished_step = ?ACL_CHECK(AceNo + 1),
        denied = ?set_flags(PrevDeniedOps, ?reset_flags(AceMask, PrevGrantedOps))
    },
    case ?common_flags(RequiredOps, AceMask) of
        ?no_flags_mask ->
            {{inconclusive, RequiredOps}, NewUserAccessCheckProgress};
        _ ->
            {denied, NewUserAccessCheckProgress}
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
    ValidFlags = ?has_all_flags(?ALL_FLAGS_BITMASK, Flags),
    ValidMask = case FileType of
        file -> ?has_all_flags(?all_object_perms_mask, Mask);
        dir -> ?has_all_flags(?all_container_perms_mask, Mask)
    end,

    case ValidType andalso ValidFlags andalso ValidMask of
        true -> ok;
        false -> throw({error, ?EINVAL})
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec cdmi_acetype_to_bitmask(binary()) -> data_access_control:bitmask().
cdmi_acetype_to_bitmask(<<"0x", _/binary>> = AceType) ->
    binary_to_bitmask(AceType);
cdmi_acetype_to_bitmask(?allow) -> ?allow_mask;
cdmi_acetype_to_bitmask(?deny) -> ?deny_mask.


%% @private
-spec cdmi_aceflags_to_bitmask(binary() | [binary()]) -> data_access_control:bitmask().
cdmi_aceflags_to_bitmask(<<"0x", _/binary>> = AceFlags) ->
    binary_to_bitmask(AceFlags);
cdmi_aceflags_to_bitmask(AceFlagsBin) when is_binary(AceFlagsBin) ->
    cdmi_aceflags_to_bitmask(parse_csv(AceFlagsBin));
cdmi_aceflags_to_bitmask(AceFlagsList) ->
    lists:foldl(
        fun
            (?no_flags, Bitmask) -> Bitmask;
            (?identifier_group, BitMask) -> ?set_flags(BitMask, ?identifier_group_mask)
        end,
        0,
        AceFlagsList
    ).


-spec cdmi_acemask_to_bitmask(binary() | [binary()]) -> data_access_control:bitmask().
cdmi_acemask_to_bitmask(<<"0x", _/binary>> = AceMask) ->
    binary_to_bitmask(AceMask);
cdmi_acemask_to_bitmask(OpsBin) when is_binary(OpsBin) ->
    cdmi_acemask_to_bitmask(parse_csv(OpsBin));
cdmi_acemask_to_bitmask(OpsList) ->
    % op doesn't differentiate between ?delete_object and ?delete_subcontainer
    % so they must be either specified both or none of them.
    HasDeleteObject = lists:member(?delete_object, OpsList),
    HasDeleteSubContainer = lists:member(?delete_subcontainer, OpsList),
    case {HasDeleteObject, HasDeleteSubContainer} of
        {false, false} -> ok;
        {true, true} -> ok;
        _ -> throw({error, ?EINVAL})
    end,

    lists:foldl(fun(Perm, Bitmask) ->
        ?set_flags(Bitmask, operation_to_bitmask(Perm))
    end, 0, OpsList).


%% @private
-spec operation_to_bitmask(binary()) -> data_access_control:bitmask().
operation_to_bitmask(?list_container) -> ?list_container_mask;
operation_to_bitmask(?read_object) -> ?read_object_mask;
operation_to_bitmask(?write_object) -> ?write_object_mask;
operation_to_bitmask(?add_object) -> ?add_object_mask;
operation_to_bitmask(?add_subcontainer) -> ?add_subcontainer_mask;
operation_to_bitmask(?read_metadata) -> ?read_metadata_mask;
operation_to_bitmask(?write_metadata) -> ?write_metadata_mask;
operation_to_bitmask(?traverse_container) -> ?traverse_container_mask;
operation_to_bitmask(?delete) -> ?delete_mask;
operation_to_bitmask(?delete_object) -> ?delete_child_mask;
operation_to_bitmask(?delete_subcontainer) -> ?delete_child_mask;
operation_to_bitmask(?read_attributes) -> ?read_attributes_mask;
operation_to_bitmask(?write_attributes) -> ?write_attributes_mask;
operation_to_bitmask(?read_acl) -> ?read_acl_mask;
operation_to_bitmask(?write_acl) -> ?write_acl_mask.


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
-spec bitmask_to_binary(data_access_control:bitmask()) -> binary().
bitmask_to_binary(Mask) -> <<"0x", (integer_to_binary(Mask, 16))/binary>>.


%% @private
-spec binary_to_bitmask(binary()) -> data_access_control:bitmask().
binary_to_bitmask(<<"0x", Mask/binary>>) -> binary_to_integer(Mask, 16).


%% @private
-spec parse_csv(binary()) -> [binary()].
parse_csv(Bin) ->
    lists:map(fun utils:trim_spaces/1, binary:split(Bin, <<",">>, [global])).
