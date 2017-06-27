%%%-------------------------------------------------------------------
%%% @author Konrad Zemek
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% gateway_protocol_handler is responsible for handling an incoming connection
%%% managed by a Ranch server. The module accepts requests from a remote node
%%% and replies with a result. The connection is closed after a period of
%%% inactivity.
%%% @end
%%%-------------------------------------------------------------------
-module(gateway_protocol_handler).
-author("Konrad Zemek").
-behavior(ranch_protocol).

-include("modules/rtransfer/gateway.hrl").
-include("modules/rtransfer/registered_names.hrl").
-include_lib("clproto/include/messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% ranch_protocol callbacks
-export([start_link/4, init/4]).


%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Initializes the acceptor.
%% @end
%%--------------------------------------------------------------------
start_link(Ref, Socket, Transport, RtransferOpts) ->
    Pid = spawn_link(?MODULE, init, [Ref, Socket, Transport, RtransferOpts]),
    {ok, Pid}.


%%--------------------------------------------------------------------
%% @doc
%% Handle data read from socket. On data read,
%% processes a request and replies with the result. On error, closes the socket
%% and stops the loop.
%% @end
%%--------------------------------------------------------------------
init(Ref, Socket, Transport, RtransferOpts) ->
    ok = ranch:accept_ack(Ref),
    ok = Transport:setopts(Socket, [{packet, 4}]),
    loop(Socket, Transport, RtransferOpts).

loop(Socket, Transport, RtransferOpts) ->
    case Transport:recv(Socket, 0, ?connection_close_timeout) of
        {ok, Data} ->
            #'FetchRequest'{file_id = FileId, offset = Offset, size = Size} =
                messages:decode_msg(Data, 'FetchRequest'),

            Hash = gateway:compute_request_hash(Data),

            OpenFun = proplists:get_value(open_fun, RtransferOpts),
            ReadFun = proplists:get_value(read_fun, RtransferOpts),
            CloseFun = proplists:get_value(close_fun, RtransferOpts),

            {ok, Handle} = OpenFun(FileId, read),
            %% TODO: loop
            {ok, NewHandle, Content} = ReadFun(Handle, Offset, Size),
            CloseFun(NewHandle),

            Reply = #'FetchReply'{request_hash = Hash, content = Content},
            ok = Transport:send(Socket, messages:encode_msg(Reply)),
            loop(Socket, Transport, RtransferOpts);

        {error, closed} ->
            ok;

        {error, _Reason} ->
            Transport:close(Socket)
    end.
