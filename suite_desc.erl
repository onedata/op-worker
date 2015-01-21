-module(suite_desc).
-behaviour(mock_app_description).

-include("appmock.hrl").
-include_lib("ctool/include/logging.hrl").

-export([response_mocks/0]).

response_mocks() -> [
    #mock_resp_mapping{port = 8080, path = "/path80801", response = #mock_resp{body = <<"path80801">>}},

    #mock_resp_mapping{port = 8080, path = "/path80802", response = #mock_resp{body = <<"path80802">>}},

    #mock_resp_mapping{port = 8080, path = "/path80803", response = #mock_resp{body = <<"path80803">>}},

    #mock_resp_mapping{port = 8080, path = "/path80804", response = #mock_resp{body = <<"path80804">>}},

    #mock_resp_mapping{port = 9090, path = "/path9090",
        response = fun(_Req, State) ->
            R = #mock_resp{body = <<"Counter: ", (integer_to_binary(State))/binary>>},
            {R, State + 1}
        end,
    initial_state = 0}
].