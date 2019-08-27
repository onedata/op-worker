%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Utility functions for access control list management.
%%% @end
%%%--------------------------------------------------------------------
-module(acl).
-author("Tomasz Lichon").

-include("modules/fslogic/metadata.hrl").

-type ace() :: #access_control_entity{}.
-type acl() :: [ace()].

-export_type([ace/0, acl/0]).

%% API
-export([get/1, exists/1]).
-export([from_json/1, to_json/1]).
-export([mask_to_bitmask/1]).

% Exported for tests
-export([identifier_acl_to_json/2, bitmask_to_binary/1]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Returns file acl, or undefined if the acl is not defined.
%% @end
%%--------------------------------------------------------------------
-spec get(file_ctx:ctx()) -> undefined | acl().
get(FileCtx) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    case custom_metadata:get_xattr_metadata(FileUuid, ?ACL_KEY, false) of
        {ok, Val} ->
            from_json(Val);
        {error, not_found} ->
            undefined
    end.


%%--------------------------------------------------------------------
%% @doc
%% Checks if acl with given UUID exists.
%% @end
%%--------------------------------------------------------------------
-spec exists(file_ctx:ctx()) -> boolean().
exists(FileCtx) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    custom_metadata:exists_xattr_metadata(FileUuid, ?ACL_XATTR_NAME).


%%--------------------------------------------------------------------
%% @doc
%% Parses maps decoded json obtained from jiffy:decode to
%% list of access control entities
%% @end
%%--------------------------------------------------------------------
-spec from_json([map()]) -> acl().
from_json(JsonAcl) ->
    [map_to_ace(JsonAce) || JsonAce <- JsonAcl].


%%--------------------------------------------------------------------
%% @doc
%% Parses list of access control entities to format suitable for jiffy:encode
%% @end
%%--------------------------------------------------------------------
-spec to_json(acl()) -> [map()].
to_json(Acl) -> [ace_to_map(Ace) || Ace <- Acl].


-spec mask_to_bitmask(binary() | [binary()]) -> non_neg_integer().
mask_to_bitmask(<<"0x", _/binary>> = Mask) ->
    binary_to_bitmask(Mask);
mask_to_bitmask(MaskNamesBin) when is_binary(MaskNamesBin) ->
    mask_to_bitmask(parse_csv(MaskNamesBin));
mask_to_bitmask(MaskNames) ->
    lists:foldl(fun(MaskName, BitMask) ->
        BitMask bor mask_name_to_bitmask(MaskName)
    end, ?no_flags_mask, MaskNames).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec ace_to_map(ace()) -> map().
ace_to_map(#access_control_entity{
    acetype = Type,
    aceflags = Flags,
    identifier = Identifier,
    name = Name,
    acemask = AccessMask
}) ->
    #{
        <<"acetype">> => bitmask_to_binary(Type),
        <<"identifier">> => identifier_acl_to_json(Identifier, Name),
        <<"aceflags">> => bitmask_to_binary(Flags),
        <<"acemask">> => bitmask_to_binary(AccessMask)
    }.


%% @private
-spec map_to_ace(map()) -> ace().
map_to_ace(Map) ->
    Type = maps:get(<<"acetype">>, Map, undefined),
    Flags = maps:get(<<"aceflags">>, Map, undefined),
    Mask = maps:get(<<"acemask">>, Map, undefined),
    JsonId = maps:get(<<"identifier">>, Map, undefined),

    {Identifier, Name} = identifier_json_to_acl(JsonId),

    #access_control_entity{
        acetype = type_to_bitmask(Type),
        aceflags = flags_to_bitmask(Flags),
        identifier = Identifier,
        name = Name,
        acemask = mask_to_bitmask(Mask)
    }.


-spec identifier_acl_to_json(Id :: binary(), Name :: undefined | binary()) ->
    JsonId :: binary().
identifier_acl_to_json(Id, undefined) -> Id;
identifier_acl_to_json(Id, Name) -> <<Name/binary, "#", Id/binary>>.


-spec identifier_json_to_acl(JsonId :: binary()) ->
    {Id :: binary(), Name :: undefined | binary()}.
identifier_json_to_acl(JsonId) ->
    case binary:split(JsonId, <<"#">>, [global]) of
        [Name, Id] ->
            {Id, Name};
        [Id] ->
            {Id, undefined}
    end.


%% @private
-spec type_to_bitmask(binary()) -> non_neg_integer().
type_to_bitmask(<<"0x", _/binary>> = Type) -> binary_to_bitmask(Type);
type_to_bitmask(?allow) -> ?allow_mask;
type_to_bitmask(?deny) -> ?deny_mask;
type_to_bitmask(?audit) -> ?audit_mask.


%% @private
-spec flags_to_bitmask(binary() | [binary()]) -> non_neg_integer().
flags_to_bitmask(<<"0x", _/binary>> = Flags) ->
    binary_to_bitmask(Flags);
flags_to_bitmask(FlagsBin) when is_binary(FlagsBin) ->
    flags_to_bitmask(parse_csv(FlagsBin));
flags_to_bitmask(Flags) ->
    lists:foldl(fun(Flag, BitMask) ->
        BitMask bor flag_to_bitmask(Flag)
    end, ?no_flags_mask, Flags).


%% @private
-spec flag_to_bitmask(binary()) -> non_neg_integer().
flag_to_bitmask(?no_flags) -> ?no_flags_mask;
flag_to_bitmask(?identifier_group) -> ?identifier_group_mask.


%% @private
-spec mask_name_to_bitmask(binary()) -> non_neg_integer().
mask_name_to_bitmask(?all_perms) -> ?all_perms_mask;
mask_name_to_bitmask(?rw) -> ?rw_mask;
mask_name_to_bitmask(?read) -> ?read_mask;
mask_name_to_bitmask(?write) -> ?write_mask;
mask_name_to_bitmask(?read_object) -> ?read_object_mask;
mask_name_to_bitmask(?list_container) -> ?list_container_mask;
mask_name_to_bitmask(?write_object) -> ?write_object_mask;
mask_name_to_bitmask(?add_object) -> ?add_object_mask;
mask_name_to_bitmask(?append_data) -> ?append_data_mask;
mask_name_to_bitmask(?add_subcontainer) -> ?add_subcontainer_mask;
mask_name_to_bitmask(?read_metadata) -> ?read_metadata_mask;
mask_name_to_bitmask(?write_metadata) -> ?write_metadata_mask;
mask_name_to_bitmask(?execute) -> ?execute_mask;
mask_name_to_bitmask(?traverse_container) -> ?traverse_container_mask;
mask_name_to_bitmask(?delete_object) -> ?delete_object_mask;
mask_name_to_bitmask(?delete_subcontainer) -> ?delete_subcontainer_mask;
mask_name_to_bitmask(?read_attributes) -> ?read_attributes_mask;
mask_name_to_bitmask(?write_attributes) -> ?write_attributes_mask;
mask_name_to_bitmask(?delete) -> ?delete_mask;
mask_name_to_bitmask(?read_acl) -> ?read_acl_mask;
mask_name_to_bitmask(?write_acl) -> ?write_acl_mask;
mask_name_to_bitmask(?write_owner) -> ?write_owner_mask.


%% @private
-spec bitmask_to_binary(non_neg_integer()) -> binary().
bitmask_to_binary(Mask) -> <<"0x", (integer_to_binary(Mask, 16))/binary>>.


%% @private
-spec binary_to_bitmask(binary()) -> non_neg_integer().
binary_to_bitmask(<<"0x", Mask/binary>>) -> binary_to_integer(Mask, 16).


%% @private
parse_csv(Bin) ->
    lists:map(fun utils:trim_spaces/1, binary:split(Bin, <<",">>, [global])).
