%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Library allowing server to contact with clients.
%%% @end
%%%-------------------------------------------------------------------
-module(client_communicator).
-author("Tomasz Lichon").

-include("proto_internal/oneclient/server_messages.hrl").
-include("proto_internal/oneclient/client_messages.hrl").
-include("workers/datastore/datastore_models.hrl").

-define(DEFAULT_REQUEST_TIMEOUT, timer:seconds(30)).

%% API
-export([send/2, communicate/2, communicate_async/2, communicate_async/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Sends request to client identified by given session_id. No answer is
%% expected.
%% @end
%%--------------------------------------------------------------------
-spec send(Msg :: #server_message{}, SessionId :: session:id()) ->
    ok | {error, term()}.
send(Msg, SessionId) ->
    {ok, #document{value = #session{connections = Cons}}} = session:get(SessionId),
    RandomIndex = random:uniform(length(Cons)),
    Pid = lists:nth(RandomIndex, Cons),
    protocol_handler:call(Pid, Msg#server_message{message_id = undefined}).

%%--------------------------------------------------------------------
%% @doc
%% Sends server_message to client and waits for its answer.
%% @end
%%--------------------------------------------------------------------
-spec communicate(ServerMsg :: #server_message{}, SessionId :: session:id()) ->
    {ok, #client_message{}} | {error, term()}.
communicate(ServerMsg, SessionId) ->
    case communicate_async(ServerMsg, SessionId, self()) of
        {ok, MsgId} ->
            receive
                #client_message{message_id = MsgId} = ClientMsg -> {ok, ClientMsg}
            after
                ?DEFAULT_REQUEST_TIMEOUT ->
                    {error, timeout}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Sends message and expects answer (with generated id) in message's default worker.
%% @equiv communicate_async(Msg, SessionId, undefined)
%% @end
%%--------------------------------------------------------------------
-spec communicate_async(Msg :: #server_message{}, SessionId :: session:id()) ->
    {ok, message_id:id()} | {error, term()}.
communicate_async(Msg, SessionId) ->
    communicate_async(Msg, SessionId, undefined).


%%--------------------------------------------------------------------
%% @doc
%% Sends server_message to client, identified by given session_id.
%% This function overrides message_id of request.
%% When ReplyPid is undefined, the answer will be routed to default handler worker.
%% Otherwise the client answer will be send to ReplyPid process as:
%% #client_message{message_id = MessageId}
%% @end
%%--------------------------------------------------------------------
-spec communicate_async(Msg :: #server_message{}, SessionId :: session:id(),
    Recipient :: pid() | undefined) -> {ok, message_id:id()} | {error, term()}.
communicate_async(Msg, SessionId, Recipient) ->
    GeneratedId = message_id:generate(Recipient),
    MsgWithId = Msg#server_message{message_id = GeneratedId},
    {ok, #document{value = #session{connections = Conn}}} = session:get(SessionId),
    RandomIndex = random:uniform(length(Conn)),
    Pid = lists:nth(RandomIndex, Conn),
    case protocol_handler:call(Pid, MsgWithId) of
        ok -> {ok, GeneratedId};
        Error -> Error
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================
