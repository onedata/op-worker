%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This is an globalregistry mock description used by appmock.
%%% @end
%%%-------------------------------------------------------------------
-module(gr_mock_description).
-author("Tomasz Lichon").

-behaviour(mock_app_description_behaviour).

-include_lib("appmock/include/appmock.hrl").
-include_lib("ctool/include/logging.hrl").

-export([rest_mocks/0, tcp_server_mocks/0]).

rest_mocks() -> [
    #rest_mock{port = 8443, path = <<"/user">>,
        response = fun(Req, State) ->
            case req:header(<<"authorization">>, Req) of
                <<"Bearer TOKEN">> ->
                    ResponseBody = mochijson2:encode([
                        {<<"userId">>, <<"test_id">>},
                        {<<"name">>, <<"test_name">>}
                    ]),
                    {#rest_response{code = 200, body = ResponseBody, content_type = <<"application/json">>}, State};
                _ ->
                    {#rest_response{code = 403}, State}
            end
        end,
        initial_state = undefined}
].

tcp_server_mocks() -> [].