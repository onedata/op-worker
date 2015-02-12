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

-define(INT64, 9223372036854775807).
-define(DEFAULT_REQUEST_TIMEOUT, timer:seconds(30)).

%% API
-export([send_message/2, send_message_receive_answer/2,
    send_message_expect_answer/2, send_message_expect_answer/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Send request to client identified by given session_id. No answer is
%% expected.
%% @end
%%--------------------------------------------------------------------
-spec send_message(#server_message{}, session_id()) -> ok | {error, term()}.
send_message(Msg, SessionId) ->
    #document{value = #session{connections = Conn}} = session:get(SessionId),
    RandomIndex = random:uniform(length(Conn)),
    Pid = lists:nth(RandomIndex, Conn),
    protocol_handler:call(Pid, Msg#server_message{message_id = undefined}).

%%--------------------------------------------------------------------
%% @doc
%% Send server_message to client and wait for its answer
%% @end
%%--------------------------------------------------------------------
-spec send_message_receive_answer(#server_message{}, session_id()) ->
    {ok, #client_message{}} | {error, term()}.
send_message_receive_answer(Msg, SessionId) ->
    case send_message_expect_answer(Msg, SessionId, self()) of
        {ok, Id} ->
            receive
                #client_message{message_id = Id} = Msg -> {ok, Msg}
            after
                ?DEFAULT_REQUEST_TIMEOUT ->
                    {error, timeout}
            end;
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Send message and expect answer (with generated id) in message's default worker.
%% @equiv send_message_expect_answer(Msg, SessionId, undefined)
%% @end
%%--------------------------------------------------------------------
-spec send_message_expect_answer(#server_message{}, session_id()) ->
    {ok, #message_id{}} | {error, term()}.
send_message_expect_answer(Msg, SessionId) ->
    send_message_expect_answer(Msg, SessionId, undefined).


%%--------------------------------------------------------------------
%% @doc
%% Send server_message to client, identified by given session_id.
%% This function overrides message_id of request.
%% When AnswerPid is undefined, the answer will be routed to default handler worker.
%% Otherwise the client answer will be send to AnswerPid process as:
%% #client_message{message_id = MessageId}
%% @end
%%--------------------------------------------------------------------
-spec send_message_expect_answer(#server_message{}, session_id(), pid() | undefined) ->
    {ok, #message_id{}} | {error, term()}.
send_message_expect_answer(Msg, SessionId, AnswerPid) ->
    GeneratedId = message_id:generate(AnswerPid),
    MsgWithId = Msg#server_message{message_id = GeneratedId},
    #document{value = #session{connections = Conn}} = session:get(SessionId),
    RandomIndex = random:uniform(length(Conn)),
    Pid = lists:nth(RandomIndex, Conn),
    case protocol_handler:call(Pid, MsgWithId) of
        ok -> {ok, GeneratedId};
        Error -> Error
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================
