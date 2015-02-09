%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(serializator).
-author("Tomasz Lichon").

-include("proto/oneclient/messages.hrl").
-include("proto_internal/oneclient/client_messages.hrl").
-include("proto_internal/oneclient/server_messages.hrl").

%% API
-export([deserialize_client_message/2, serialize_server_message/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec deserialize_client_message(Message :: binary(), Cred :: #credentials{}) ->
    {ok, Record :: term()} | {error, term()}.
deserialize_client_message(Message, Cred) ->
    #'ClientMessage'{response_id = Id, seq_num = SeqNum,
        last_message = Last, client_message = Msg} =
        client_messages:decode_msg(Message, 'ClientMessage'),

    #client_message{response_id = Id, seq_num = SeqNum, last_message = Last,
        credentials = Cred, client_message = translator:translate(Msg)}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec serialize_server_message(#server_message{}) -> binary() | {error, term()}.
serialize_server_message(#server_message{response_id = Id, seq_num = Seq,
    last_message = Last, server_message = Msg}) ->
    ServerMessage = #'ServerMessage'{response_id = Id, seq_num = Seq,
        last_message = Last, server_message = translator:translate(Msg)},
    server_messages:encode_msg(ServerMessage).

%%%===================================================================
%%% Internal functions
%%%===================================================================
