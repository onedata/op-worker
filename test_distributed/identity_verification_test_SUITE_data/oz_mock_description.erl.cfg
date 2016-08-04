%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This is an OZ mock description used by appmock.
%%% @end
%%%-------------------------------------------------------------------
-module(oz_mock_description).
-author("Michal Zmuda").

-behaviour(mock_app_description_behaviour).

-include_lib("appmock/include/appmock.hrl").
-include_lib("ctool/include/logging.hrl").

-export([rest_mocks/0, tcp_server_mocks/0]).

rest_mocks() ->
    [
        #rest_mock{port = 8443, path = <<"/api/v3/onezone/publickey/[:id]">>,
            response = fun(Req, State) ->
                ?emergency("Req ~p", [Req]),
                ID = req:binding(id, Req),
                {Method, _} = cowboy_req:method(Req),
                ?emergency("ID ~p", [ID]),
                ?emergency("State ~p", [State]),
                ?emergency("Method ~p", [Method]),
                case Method of
                    <<"GET">> ->
                        ResponseBody = json_utils:encode([
                            {<<"publicKey">>, maps:get(ID, State, undefined)}
                        ]),
                        ?emergency("ResponseBody ~p", [ResponseBody]),
                        {#rest_response{code = 200, body = ResponseBody, content_type = <<"application/json">>}, State};
                    <<"POST">> ->
                        Body = req:body(Req),
                        ?emergency("Body ~p", [Body]),
                        Params = json_utils:decode(Body),
                        ?emergency("Params ~p", [Params]),
                        EncodedPublicKey = proplists:get_value(<<"publicKey">>, Params),
                        ?emergency("EncodedPublicKey ~p", [EncodedPublicKey]),
                        PublicKey = binary_to_term(base64:decode(EncodedPublicKey)),
                        ?emergency("PublicKey ~p", [PublicKey]),
                        NewState = maps:put(ID, EncodedPublicKey, State),
                        ?emergency("NewState ~p", [NewState]),
                        {#rest_response{code = 204}, NewState};
                    _ ->
                        {#rest_response{code = 500}, State}
                end
            end,
            initial_state = #{}}
    ].

tcp_server_mocks() -> [].
