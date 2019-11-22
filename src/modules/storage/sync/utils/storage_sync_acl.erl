%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Functions for handling NFSv4 ACLs.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_sync_acl).
-author("Jakub Kudzia").

-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([decode_and_normalize/3, encode/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% Converts ACL from binary form to list of #access_control_entity
%% records and resolves 'who' fields in all ACEs in given ACL
%% form user@nfsdomain.org or group@nfsdomain.org to onedata user/group id.
%% Whether given identifier is associated with user or group is
%% determined by identifier_group_mask in acemask field.
%% @end
%%-------------------------------------------------------------------
-spec decode_and_normalize(binary(), od_space:id(), od_storage:id()) ->
    {ok, acl:acl()}.
decode_and_normalize(ACLBin, SpaceId, StorageId) ->
    {ok, ACL} = decode(ACLBin),
    normalize(ACL, SpaceId, StorageId).

%%-------------------------------------------------------------------
%% @doc
%% Converts list of #access_control_entity records to binary form. 
%% @end
%%-------------------------------------------------------------------
-spec encode(acl:acl()) -> binary().
encode(Acl) ->
    encode(Acl, []).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Converts ACL from binary form to list of #access_control_entity
%% records.
%% ATTENTION!!! 'who' field will not be resolved to onedata user/group id.
%% ?MODULE/normalize/2 must be called on decoded ACL to resolve
%% user/group ids.
%% @end
%%-------------------------------------------------------------------
-spec decode(binary()) -> {ok, acl:acl()}.
decode(ACLBin) ->
    {ACLLen, {ACLBin, Pos1}} = xdrlib:dec_unsigned_int({ACLBin, 1}),
    ACLBin2 = binary:part(ACLBin, Pos1 - 1, byte_size(ACLBin) - Pos1 + 1),
    Acl = decode(ACLBin2, ACLLen, []),
    {ok, Acl}.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Resolves 'who' fields in all ACEs in given ACL
%% form user@nfsdomain.org or group@nfsdomain.org to onedata user/group id.
%% Whether given identifier is associated with user or group is
%% determined by identifier_group_mask in acemask field.
%% @end
%%-------------------------------------------------------------------
-spec normalize(acl:acl(), od_space:id(), od_storage:id()) ->
    {ok, acl:acl()}.
normalize(Acl, SpaceId, StorageId) ->
    {ok, normalize(Acl, [], SpaceId, StorageId)}.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Tail-recursive helper function for decode_acl/1.
%% @end
%%-------------------------------------------------------------------
-spec decode(binary(), non_neg_integer(), acl:acl()) -> acl:acl().
decode(_, 0, DecodedACL) ->
    lists:reverse(DecodedACL);
decode(ACLBin, N, DecodedACL) ->
    {ACE, ACLBin2} = decode_ace(ACLBin),
    decode(ACLBin2, N - 1, [ACE | DecodedACL]).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Tail-recursive helper function for encode_acl/1.
%% @end
%%-------------------------------------------------------------------
-spec encode(acl:acl(), [binary()]) -> binary().
encode([], EncodedACL) ->
    LengthBin = xdrlib:enc_unsigned_int(length(EncodedACL)),
    <<LengthBin/binary, (list_to_binary(lists:reverse(EncodedACL)))/binary>>;
encode([ACE | ACLRest], EncodedACL) ->
    EncodedACE = encode_ace(ACE),
    encode(ACLRest, [EncodedACE | EncodedACL]).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Decodes ACE from binary form.
%% @end
%%-------------------------------------------------------------------
-spec decode_ace(binary()) -> {ace:ace(), binary()}.
decode_ace(ACLBin) ->
    {Type, {ACLBin, P1}} = xdrlib:dec_unsigned_int({ACLBin, 1}),
    {Flags, {ACLBin, P2}} = xdrlib:dec_unsigned_int({ACLBin, P1}),
    {Mask, {ACLBin, P3}} = xdrlib:dec_unsigned_int({ACLBin, P2}),
    {Who, {ACLBin, P4}} = xdrlib:dec_string({ACLBin, P3}),
    ACE = #access_control_entity{
        acetype = Type,
        aceflags = Flags,
        identifier = Who,
        acemask = Mask
    },
    ACLBin2 = binary:part(ACLBin, P4 - 1, byte_size(ACLBin) - P4 + 1),
    {ACE, ACLBin2}.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Encodes ACE to binary form.
%% @end
%%-------------------------------------------------------------------
-spec encode_ace(ace:ace()) -> binary().
encode_ace(#access_control_entity{
    acetype = Type,
    aceflags = Flags,
    identifier = Who,
    acemask = Mask
}) ->
    TypeBin = xdrlib:enc_unsigned_int(Type),
    FlagsBin = xdrlib:enc_unsigned_int(Flags),
    MaskBin = xdrlib:enc_unsigned_int(Mask),
    WhoBin = xdrlib:enc_string(Who),
    <<TypeBin/binary, FlagsBin/binary, MaskBin/binary, WhoBin/binary>>.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Tail-recursive helper function for normalize/2.
%% @end
%%-------------------------------------------------------------------
-spec normalize(acl:acl(), acl:acl(), od_space:id(), od_storage:id()) ->
    acl:acl().
normalize([], NormalizedACL, _SpaceId, _StorageId) ->
    lists:reverse(NormalizedACL);
normalize([ACE | Rest], NormalizedACL, SpaceId, StorageId) ->
    NormalizedACE = normalize_ace(ACE, SpaceId, StorageId),
    normalize(Rest, [NormalizedACE | NormalizedACL], SpaceId, StorageId).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Normalizes given #access_control_entity.
%% @end
%%-------------------------------------------------------------------
-spec normalize_ace(ace:ace(), od_space:id(), od_storage:id()) -> ace:ace().
normalize_ace(ACE = #access_control_entity{identifier = ?owner}, _SpaceId, _StorageId) ->
    ACE;
normalize_ace(ACE = #access_control_entity{identifier = ?group}, _SpaceId, _StorageId) ->
    ACE;
normalize_ace(ACE = #access_control_entity{identifier = ?everyone}, _SpaceId, _StorageId) ->
    ACE;
normalize_ace(ACE = #access_control_entity{
    aceflags = Flags,
    identifier = Who
}, SpaceId, StorageId) ->
    NormalizedWho = normalize_who(Flags, Who, SpaceId, StorageId),
    ACE#access_control_entity{identifier = NormalizedWho}.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Resolves 'who' fields in form user@nfsdomain.org or group@nfsdomain.org
%% to onedata user/group id. Whether given identifier is associated with
%% user or group is determined by identifier_group_mask in acemask field.
%% @end
%%-------------------------------------------------------------------
-spec normalize_who(non_neg_integer(), binary(), od_space:id(), od_storage:id()) ->
    od_user:id() | od_group:id().
normalize_who(Flags, Who, SpaceId, StorageId) when ?has_flag(Flags, ?identifier_group_mask) ->
    {ok, GroupId} = reverse_luma:get_group_id_by_name(Who, SpaceId, StorageId),
    GroupId;
normalize_who(_, Who, _SpaceId, StorageId) ->
    {ok, UserId} = reverse_luma:get_user_id_by_name(Who, StorageId),
    UserId.
