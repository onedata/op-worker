-module(suite_desc).
-behaviour(mock_app_behaviour).

-include("appmock.hrl").
-include_lib("ctool/include/logging.hrl").

-export([request_mappings/0]).

request_mappings() -> [
    #mapp
].