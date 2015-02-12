%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Generate and encode/decode message id
%%% @end
%%%-------------------------------------------------------------------
-module(message_id).
-author("Tomasz Lichon").

%% API
-export([generate/0, generate/1, encode/1, decode/1]).

-include("cluster_elements/protocol_handler/message_id.hrl").

-define(INT64, 9223372036854775807).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @equiv generate(undefined).
%% @end
%%--------------------------------------------------------------------
-spec generate() -> binary().
generate() ->
    generate(undefined).


%%--------------------------------------------------------------------
%% @doc
%% generate id with encoded handler pid
%% @end
%%--------------------------------------------------------------------
-spec generate(Handler :: pid() | undefined) -> binary().
generate(Handler) ->
    #message_id{issuer = server, id = integer_to_binary(random:uniform(?INT64)),
        handler = Handler}.

%%--------------------------------------------------------------------
%% @doc
%% encode message_id to binary form
%% @end
%%--------------------------------------------------------------------
-spec encode(#message_id{}) -> {ok, binary()}.
encode(undefined) ->
    undefined;
encode(#message_id{issuer = client, id = Id}) ->
    Id;
encode(MsgId = #message_id{}) ->
    term_to_binary(MsgId).

%%--------------------------------------------------------------------
%% @doc
%% decode message_id from binary form
%% @end
%%--------------------------------------------------------------------
-spec decode(Id :: binary()) -> {ok, #message_id{}}.
decode(undefined) ->
    undefined;
decode(Id) ->
    try binary_to_term(Id) of
        #message_id{} = MsgId ->
            MsgId;
        _ -> #message_id{issuer = client, id = Id}
    catch
        _:_ ->
            #message_id{issuer = client, id = Id}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================