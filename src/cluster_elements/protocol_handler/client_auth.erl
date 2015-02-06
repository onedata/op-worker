%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Client authentication
%%% @end
%%%-------------------------------------------------------------------
-module(client_auth).
-author("Tomasz Lichon").

%% API
-export([handle_auth_info/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Handle first client message, which describes authentication method
%% (cert/token)
%% @end
%%--------------------------------------------------------------------
-spec handle_auth_info(Message :: binary()) -> {ok, ClientId :: binary()} | {error, term()}.
handle_auth_info(Message) ->
    case mochijson2:decode(Message, [{format, proplist}]) of
        [{<<"token">>, Token}] ->
            authenticate_using_token(Token);
        [{<<"cert">>, OneproxySessionId}] ->
            authenticate_using_certificate(OneproxySessionId)
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Authenticate client using given token, returns client uuid
%% @end
%%--------------------------------------------------------------------
-spec authenticate_using_token(Token :: binary()) -> {ok, ClientId :: binary()} | {error, term()}.
authenticate_using_token(_Token) ->
    {ok, <<"uuid">>}.

%%--------------------------------------------------------------------
%% @doc
%% Authenticate client using given SessionId. The certificate is obtained
%% from oneproxy. Returns client uuid.
%% @end
%%--------------------------------------------------------------------
-spec authenticate_using_certificate(Token :: binary()) -> {ok, ClientId :: binary()} | {error, term()}.
authenticate_using_certificate(_OneproxySessionId) ->
    {ok, <<"uuid">>}.

