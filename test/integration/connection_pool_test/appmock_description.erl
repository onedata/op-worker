-module(appmock_description).
-behaviour(mock_app_description_behaviour).

-include_lib("appmock/include/appmock.hrl").
-include_lib("ctool/include/logging.hrl").

-export([rest_mocks/0, tcp_server_mocks/0]).

rest_mocks() -> [].

tcp_server_mocks() -> [
    #tcp_server_mock{
        port = 5555,
        ssl = true,
        packet = 4
    }
].
