%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module decides where to send incomming client messages.
%%% @end
%%%-------------------------------------------------------------------
-module(router).
-author("Tomasz Lichon").

-include("proto_internal/oneclient/client_messages.hrl").

%% API
-export([preroute_message/2, route_message/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Check if message is sequential, if so - proxy it throught sequencer
%% @end
%%--------------------------------------------------------------------
-spec preroute_message(SeqMan :: pid(), Msg :: #client_message{}) ->
    ok | {error, term()}.
preroute_message(_SeqMan, #client_message{seq_num = Seq} = Msg) when Seq =/= undefined ->
    route_message(Msg);
preroute_message(SeqMan, Msg) ->
    gen_server:cast(SeqMan, Msg).

%%--------------------------------------------------------------------
%% @doc
%% Route message to adequate handler
%% @end
%%--------------------------------------------------------------------
-spec route_message(Msg :: #client_message{}) -> ok | {error, term()}.
route_message(#client_message{}) ->
    % todo integrate with worker hosts
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================