%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Session management model, frequently invoked by incoming tcp
%%% connections in connection
%%% @end
%%%-------------------------------------------------------------------
-module(message_id).
-author("Tomasz Lichon").

-include("proto/oneclient/message_id.hrl").

%% API
-export([generate/1, generate/2, encode/1, decode/1]).

-type id() :: #message_id{}.
-type issuer() :: oneprovider:id() | client.

-export_type([id/0]).

-define(INT64, 16#FFFFFFFFFFFFFFF).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Generates ID with encoded handler pid.
%% @end
%%--------------------------------------------------------------------
-spec generate(Recipient :: pid() | undefined) -> {ok, MsgId :: #message_id{}}.
generate(Recipient) ->
    generate(Recipient, oneprovider:get_id(fail_with_throw)).

%%--------------------------------------------------------------------
%% @doc
%% Generates ID with encoded handler pid and given issuer type.
%% @end
%%--------------------------------------------------------------------
-spec generate(Recipient :: pid() | undefined, Issuer :: issuer()) -> {ok, MsgId :: #message_id{}}.
generate(undefined, Issuer) ->
    {ok, #message_id{
        issuer = Issuer,
        id = integer_to_binary(crypto:rand_uniform(0, ?INT64)),
        recipient = undefined
    }};
generate(Recipient, Issuer) ->
    {ok, #message_id{
        issuer = Issuer,
        id = integer_to_binary(crypto:rand_uniform(0, ?INT64)),
        recipient = term_to_binary(Recipient)
    }}.

%%--------------------------------------------------------------------
%% @doc
%% Encodes message_id to binary form.
%% @end
%%--------------------------------------------------------------------
-spec encode(MsgId :: #message_id{} | undefined) -> {ok, undefined | binary()}.
encode(undefined) ->
    {ok, undefined};
encode(#message_id{issuer = client, recipient = undefined, id = Id}) ->
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
            {ok, MsgId}
    catch
        _:_ ->
            {ok, #message_id{issuer = client, id = Id}}
    end.
