%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for generating and encoding/decoding
%%% message ID.
%%% @end
%%%-------------------------------------------------------------------
-module(message_id).
-author("Tomasz Lichon").

-include("proto/oneclient/message_id.hrl").

%% API
-export([generate/0, generate/1, encode/1, decode/1]).

-export_type([id/0]).

-type id() :: #message_id{}.

-define(INT64, 16#FFFFFFFFFFFFFFF).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @equiv generate(undefined).
%% @end
%%--------------------------------------------------------------------
-spec generate() -> {ok, #message_id{}}.
generate() ->
    generate(undefined).


%%--------------------------------------------------------------------
%% @doc
%% Generates ID with encoded handler pid.
%% @end
%%--------------------------------------------------------------------
-spec generate(Recipient :: pid() | undefined) -> {ok, MsgId :: #message_id{}}.
generate(Recipient) ->
    {ok, #message_id{
        issuer = server,
        id = integer_to_binary(crypto:rand_uniform(0, ?INT64)),
        recipient = Recipient
    }}.

%%--------------------------------------------------------------------
%% @doc
%% Encodes message_id to binary form.
%% @end
%%--------------------------------------------------------------------
-spec encode(MsgId :: #message_id{} | undefined) -> {ok, undefined | binary()}.
encode(undefined) ->
    {ok, undefined};
encode(#message_id{issuer = client, id = Id}) ->
    {ok, Id};
encode(MsgId = #message_id{}) ->
    {ok, term_to_binary(MsgId)}.

%%--------------------------------------------------------------------
%% @doc
%% Decodes message_id from binary form.
%% @end
%%--------------------------------------------------------------------
-spec decode(Id :: binary()) -> {ok, #message_id{}}.
decode(undefined) ->
    {ok, undefined};
decode(Id) ->
    try binary_to_term(Id) of
        #message_id{} = MsgId ->
            {ok, MsgId};
        _ ->
            {ok, #message_id{issuer = client, id = Id}}
    catch
        _:_ ->
            {ok, #message_id{issuer = client, id = Id}}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================