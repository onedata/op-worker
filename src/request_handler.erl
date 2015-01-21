

-module(request_handler).

-include_lib("ctool/include/logging.hrl").
-include("appmock.hrl").

%% Cowboy API
-export([init/3, handle/2, terminate/3]).


init(_Type, Req, [ETSKey]) ->
    {ok, Req, [ETSKey]}.


handle(Req, [ETSKey]) ->
    {ok, Req2} = appmock_logic:handle_request(Req, ETSKey),
    {ok, Req2, [ETSKey]}.


terminate(_Reason, _Req, _State) ->
    ok.