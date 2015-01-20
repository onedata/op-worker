-module(suite_desc).
-behaviour(mock_app_description).

-include("appmock.hrl").
-include_lib("ctool/include/logging.hrl").

-export([request_mappings/0]).

request_mappings() -> [
    #mapping{port = 8080, path = "/path8080", response = #reponse{body = <<"path8080">>}},
    #mapping{port = 9090, path = "/path9090",
        response = fun(_Req, Counter) ->
            #reponse{body = <<"Counter: ", (integer_to_binary(Counter))/binary>>}
        end}
].