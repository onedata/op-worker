-module(suite_desc).
-behaviour(mock_app_description).

-include("appmock.hrl").
-include_lib("ctool/include/logging.hrl").

-export([request_mappings/0]).

request_mappings() -> [
    #mapping{port = 8080, path = "/path80801", response = #response{body = <<"path80801">>}},
    #mapping{port = 8080, path = "/path80802", response = #response{body = <<"path80802">>}},
    #mapping{port = 8080, path = "/path80803", response = #response{body = <<"path80803">>}},
    #mapping{port = 8080, path = "/path80804", response = #response{body = <<"path80804">>}},
    #mapping{port = 9090, path = "/path9090",
        response = fun(_Req, State) ->
            R = #response{body = <<"Counter: ", (integer_to_binary(State))/binary>>},
            {R, State + 1}
        end,
    initial_state = 0}
].