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
-module(nfs4_acl).
-author("Jakub Kudzia").

-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([decode_and_normalize/2, encode/1]).

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
-spec decode_and_normalize(binary(), storage_file_ctx:ctx()) -> {ok, acl_logic:acl()}.
decode_and_normalize(ACLBin, StorageFileCtx) ->
    {ok,  ACL} = decode(ACLBin),
    normalize(ACL, StorageFileCtx).

%%-------------------------------------------------------------------
%% @doc
%% Converts list of #access_control_entity records to binary form. 
%% @end
%%-------------------------------------------------------------------
-spec encode(acl_logic:acl()) -> binary().
encode(#acl{value = ACEs}) ->
    encode(ACEs, []).

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
-spec decode(binary()) -> {ok, acl_logic:acl()}.
decode(ACLBin) ->
    {ACLLen, {ACLBin, Pos1}} = xdrlib:dec_unsigned_int({ACLBin, 1}),
    ACLBin2 = binary:part(ACLBin, Pos1 - 1, byte_size(ACLBin) - Pos1 + 1),
    ACEs = decode(ACLBin2, ACLLen, []),
    {ok, #acl{value = ACEs}}.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Resolves 'who' fields in all ACEs in given ACL
%% form user@nfsdomain.org or group@nfsdomain.org to onedata user/group id.
%% Whether given identifier is associated with user or group is
%% determined by identifier_group_mask in acemask field.
%% @end
%%-------------------------------------------------------------------
-spec normalize(acl_logic:acl(), storage_file_ctx:ctx()) ->
    {ok, acl_logic:acl()}.
normalize(Acl = #acl{value = ACEs}, StorageFileCtx) ->
    {ok, Acl#acl{value = normalize(ACEs, [], StorageFileCtx)}}.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Tail-recursive helper function for decode_acl/1.
%% @end
%%-------------------------------------------------------------------
-spec decode(binary(), non_neg_integer(), [acl_logic:ace()]) -> [acl_logic:ace()].
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
-spec encode([acl_logic:ace()], [binary()]) -> binary().
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
-spec decode_ace(binary()) -> {acl_logic:ace(), binary()}.
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
-spec encode_ace(acl_logic:ace()) -> binary().
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
-spec normalize([acl_logic:ace()], [acl_logic:ace()], storage_file_ctx:ctx()) ->
    [acl_logic:ace()].
normalize([], NormalizedACL, _StorageFileCtx) ->
    lists:reverse(NormalizedACL);
normalize([ACE | Rest], NormalizedACL, StorageFileCtx) ->
    {NormalizedACE, StorageFileCtx2} = normalize_ace(ACE, StorageFileCtx),
    normalize(Rest, [NormalizedACE | NormalizedACL], StorageFileCtx2).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Normalizes given #access_control_entity.
%% @end
%%-------------------------------------------------------------------
-spec normalize_ace(acl_logic:ace(), storage_file_ctx:ctx()) ->
    {acl_logic:ace(), storage_file_ctx:ctx()}.
normalize_ace(ACE = #access_control_entity{identifier = ?owner}, StorageFileCtx) ->
    {ACE, StorageFileCtx};
normalize_ace(ACE = #access_control_entity{identifier = ?group}, StorageFileCtx) ->
    {ACE, StorageFileCtx};
normalize_ace(ACE = #access_control_entity{identifier = ?everyone}, StorageFileCtx) ->
    {ACE, StorageFileCtx};
normalize_ace(ACE = #access_control_entity{
    aceflags = Flags,
    identifier = Who
}, StorageFileCtx) ->
    {NormalizedWho, StorageFileCtx2} = normalize_who(Flags, Who, StorageFileCtx),
    {ACE#access_control_entity{identifier = NormalizedWho}, StorageFileCtx2}.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Resolves 'who' fields in form user@nfsdomain.org or group@nfsdomain.org
%% to onedata user/group id. Whether given identifier is associated with
%% user or group is determined by identifier_group_mask in acemask field.
%% @end
%%-------------------------------------------------------------------
-spec normalize_who(non_neg_integer(), binary(), storage_file_ctx:ctx()) ->
    {od_user:id() | od_group:id(), storage_file_ctx:ctx()}.
normalize_who(Flags, Who, StorageFileCtx) when ?has_flag(Flags, ?identifier_group_mask) ->
    {StorageDoc, StorageFileCtx2} = storage_file_ctx:get_storage_doc(StorageFileCtx),
    {ok, GroupId} = reverse_luma:get_group_id_by_name(Who, StorageDoc),
    {GroupId, StorageFileCtx2};
normalize_who(_, Who, StorageFileCtx) ->
    {StorageDoc, StorageFileCtx2} = storage_file_ctx:get_storage_doc(StorageFileCtx),
    {ok, UserId} = reverse_luma:get_user_id_by_name(Who, StorageDoc),
    {UserId, StorageFileCtx2}.
