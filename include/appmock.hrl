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