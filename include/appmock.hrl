
%%%===================================================================
%%% Mocking responses
%%%===================================================================
-record(mock_resp, {
    code = 200 :: integer(),
    content_type = <<"application/json">>,
    headers = [],
    body = <<"">>
}).

-record(mock_resp_mapping, {
    % port on which requests will be accepted
    port = 443 :: integer(),
    % cowboy_router compatible path on which requests will be accepted
    path = "/" :: string(),
    % explicit #mock_resp record or a 2 argument function
    % fun(Req, State) -> {#mock_resp{}, NewState}
    % Req - cowboy #req{} record, read only
    % State - carries state between consecutive requests on the same stub
    response = #mock_resp{} :: #mock_resp{} | function(),
    % Initial state of the stub
    initial_state = []
}).

%%%===================================================================
%%% Performing remote-controlled requests
%%%===================================================================
-record(mock_req, {
    url = "" :: string(),
    port = 0 :: integer(),
    content_type = <<"application/json">>,
    headers = [],
    body = <<"">>
}).

-record(mock_req_mapping, {
    % Performing a GET under this path will trigger the request
    % The remote control API is available at a port specified in app env
    trigger_path = "/" :: string(),
    % explicit #mock_req record or a 1 argument function
    % fun(State) -> {#mock_req{}, NewState}
    % State - carries state between consecutive runs of this trigger
    % NewState will be passed to validation function, if there is one
    request = #mock_req{} :: #mock_req{} | function(),
    % Initial state of the stub
    initial_state = [],
    % Timeout after which the request is considered failed
    timeout = 1000 :: integer(),
    % ok if no response validation is required, or
    % 4 argument function
    % fun(Code, Headers, Body, State) -> {ok | error, NewState}
    % Code - HTTP code of returned response
    % Headers - List of headers sent by with response
    % Body - response body
    % State - carries state between consecutive runs of this trigger
    % NewState will be passed to the next run of this trigger
    validator = ok :: ok | function()
}).