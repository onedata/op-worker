-record(reponse, {
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
    % fun(Req, Counter) -> #reponse{}
    % Req - cowboy #req{} record, read only
    % Counter - increments with every request that matches this mapping
    response = #reponse{} :: #reponse{} | function()
}).