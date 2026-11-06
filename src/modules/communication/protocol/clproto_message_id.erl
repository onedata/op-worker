%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module providing various utility functions for handling message_id.
%%% @end
%%%-------------------------------------------------------------------
-module(clproto_message_id).
-author("Tomasz Lichon").

-include("proto/common/clproto_message_id.hrl").

%% API
-export([generate/1, generate/2, encode/1, decode/1]).

-type id() :: #message_id{}.
-type issuer() :: oneprovider:id() | client.

-export_type([id/0, issuer/0]).

-define(INT64, 16#FFFFFFFFFFFFFFF).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @equiv generate(Recipient, oneprovider:get_id()).
%% @end
%%--------------------------------------------------------------------
-spec generate(Recipient :: pid() | undefined) -> {ok, MsgId :: id()}.
generate(Recipient) ->
    generate(Recipient, oneprovider:get_id()).

%%--------------------------------------------------------------------
%% @doc
%% Generates ID with encoded handler pid and given issuer type.
%% @end
%%--------------------------------------------------------------------
-spec generate(Recipient :: pid() | undefined, issuer()) ->
    {ok, MsgId :: id()}.
generate(undefined, Issuer) ->
    {ok, #message_id{
        issuer = Issuer,
        id = integer_to_binary(rand:uniform(?INT64 + 1) - 1),
        recipient = undefined
    }};
generate(Recipient, Issuer) ->
    {ok, #message_id{
        issuer = Issuer,
        id = integer_to_binary(rand:uniform(?INT64 + 1) - 1),
        recipient = term_to_binary(Recipient)
    }}.

%%--------------------------------------------------------------------
%% @doc
%% Encodes message_id to binary form.
%% @end
%%--------------------------------------------------------------------
-spec encode(MsgId :: undefined | id()) ->
    {ok, EncodedMsgId :: undefined | binary()}.
encode(undefined) ->
    {ok, undefined};
encode(#message_id{issuer = client, recipient = undefined, id = Id}) ->
    {ok, Id};
encode(#message_id{} = MsgId) ->
    {ok, term_to_binary(MsgId)}.

%%--------------------------------------------------------------------
%% @doc
%% Decodes message_id from binary form.
%% @end
%%--------------------------------------------------------------------
-spec decode(EncodedMsgId :: undefined | binary()) ->
    {ok, MsgId :: undefined | id()}.
decode(undefined) ->
    {ok, undefined};
decode(Id) ->
    try binary_to_term(Id) of
        #message_id{} = MsgId ->
            {ok, MsgId}
    catch
        _:_ ->
            {ok, #message_id{issuer = client, id = Id}}
    end.
