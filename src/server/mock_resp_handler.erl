-module(mock_resp_handler).

-include_lib("ctool/include/logging.hrl").
-include("appmock.hrl").

%% Cowboy API
-export([init/3, handle/2, terminate/3]).


init(_Type, Req, [ETSKey]) ->
    {ok, Req, [ETSKey]}.


handle(Req, [ETSKey]) ->
    {ok, Req2} =
        try
            {ok, _NewReq} = appmock_logic:produce_mock_resp(Req, ETSKey)
        catch T:M ->
            {Port, Path} = ETSKey,
            ?error_stacktrace("Error in mock_resp_handler. Path: ~p. Port: ~p. ~p:~p.",
                [Path, Port, T, M]),
            {ok, _ErrorReq} = cowboy_req:reply(500, Req)
        end,
    {ok, Req2, [ETSKey]}.


terminate(_Reason, _Req, _State) ->
    ok.