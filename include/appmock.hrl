-define(APP_NAME, appmock).

-record(response, {
    code = 200 :: integer(),
    content_type = <<"application/json">>,
    headers = [],
    body = <<"">>
}).

-record(mapping, {
    % port on which requests will be accepted
    port = 443 :: integer(),
    % cowboy_router compatible path on which requests will be accepted
    path = "/" :: string(),
    % explicit binary or a 2 argument function
    % fun(Req, State) -> #reponse{}
    % Req - cowboy #req{} record, read only
    % State - carries state between consecutive requests on the same stub
    response = #response{} :: #response{} | function(),
    % Initial state of the stub
    initial_state = []
}).

-record(mapping_state, {
    response = #response{} :: #response{} | function(),
    % Used to remember state between requests on the same stub
    state = []
}).