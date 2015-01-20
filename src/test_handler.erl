-module(test_handler).

-export([init/3]).

init(Req, Opts) ->
    Req2 = cowboy_req:reply(200, [
        {<<"content-type">>, <<"text/plain">>}
    ], <<"Hello world!">>, Req),
    {ok, Req2, Opts}.