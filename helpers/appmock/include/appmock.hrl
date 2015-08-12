%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This header file contains record definitions and macros used in
%%% mock app description files.
%%% @end
%%%-------------------------------------------------------------------

%%%===================================================================
%%% Mocking responses
%%%===================================================================

% This record represents a single response returned by a mocked endpoint.
-record(rest_response, {
    code = 200 :: integer(),
    content_type = <<"application/json">>,
    headers = [],
    body = <<"">>
}).

% This record describes a single mapping, uniquely distinguished by port and path.
% It's used to mock HTTP endpoints that will return predefined or dynamically generated answers.
-record(rest_mock, {
    % port on which requests will be accepted
    port = 443 :: integer(),
    % cowboy_router compatible path on which requests will be accepted
    path = <<"/">> :: binary(),
    % response can be:
    % 1) explicit #mock_resp record, will be returned every time the endpoint is used
    % 2) a list of #mock_resp records that will be returned in given sequence, cyclically
    % 3) a 2 argument function:
    %       fun(Req, State) -> {#mock_resp{}, NewState}
    %       Req - cowboy #req{} record, read only
    %       State - carries state between consecutive requests on the same stub
    response = #rest_response{} :: #rest_response{} | [#rest_response{}] | function(),
    % initial state of the stub
    initial_state = [] :: term()
}).

% This record represents a TCP server mock.
-record(tcp_server_mock, {
    port = 5555,
    ssl = true,
    packet = raw
}).