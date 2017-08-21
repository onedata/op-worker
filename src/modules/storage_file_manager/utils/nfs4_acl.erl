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

-include("modules/datastore/datastore_specific_models_def.hrl").
-include("proto/oneprovider/provider_messages.hrl").

%% API
-export([decode/1, encode/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% Converts ACL from binary form to list of #access_control_entity
%% records.
%% @end
%%-------------------------------------------------------------------
-spec decode(binary()) -> {ok, acl_logic:acl()}.
decode(ACLBin) ->
    {ACLLen, {ACLBin, Pos1}} = xdrlib:dec_unsigned_int({ACLBin, 1}),
    ACLBin2 = binary:part(ACLBin, Pos1 - 1, byte_size(ACLBin) - Pos1 + 1),
    ACEs = decode_acl(ACLBin2, ACLLen, []),
    {ok, #acl{value = ACEs}}.

%%-------------------------------------------------------------------
%% @doc
%% Converts list of #access_control_entity records to binary form. 
%% @end
%%-------------------------------------------------------------------
-spec encode(acl_logic:acl()) -> binary().
encode(#acl{value = ACEs}) ->
    encode_acl(ACEs, []).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Tail-recursive helper function for decode_acl/1.
%% @end
%%-------------------------------------------------------------------
-spec decode_acl(binary(), non_neg_integer(), [acl_logic:ace()]) -> [acl_logic:ace()].
decode_acl(_, 0, DecodedACL) ->
    lists:reverse(DecodedACL);
decode_acl(ACLBin, N, DecodedACL) ->
    {ACE, ACLBin2} = decode_ace(ACLBin),
    decode_acl(ACLBin2, N - 1, [ACE | DecodedACL]).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Tail-recursive helper function for encode_acl/1.
%% @end
%%-------------------------------------------------------------------
-spec encode_acl([acl_logic:ace()], [binary()]) -> binary().
encode_acl([], EncodedACL) ->
    LengthBin = xdrlib:enc_unsigned_int(length(EncodedACL)),
    <<LengthBin/binary, (list_to_binary(lists:reverse(EncodedACL)))/binary>>;
encode_acl([ACE | ACLRest], EncodedACL) ->
    EncodedACE = encode_ace(ACE),
    encode_acl(ACLRest, [EncodedACE | EncodedACL]).

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



