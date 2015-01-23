-module(example_app_description).
-behaviour(mock_app_description).

-include("appmock.hrl").
-include_lib("ctool/include/logging.hrl").

-export([response_mocks/0]).

response_mocks() -> [
    #mock_resp_mapping{port = 8080, path = <<"/test1">>, response = #mock_resp{body = <<"this is test1 endpoint">>}},

    #mock_resp_mapping{port = 8080, path = <<"/test2">>, response = #mock_resp{body = <<"this is test2 endpoint">>}},

    #mock_resp_mapping{port = 9090, path = <<"/test_with_state">>,
        response = fun(_Req, State) ->
            R = #mock_resp{body = <<"Counter: ", (integer_to_binary(State))/binary>>},
            {R, State + 1}
        end,
    initial_state = 0}
].