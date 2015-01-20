-module(test_handler).

-export([init/2]).

-include_lib("ctool/include/logging.hrl").

init(Req, Opts) ->
    Req2 = cowboy_req:reply(200, [
        {<<"content-type">>, <<"text/plain">>}
    ], <<"Hello world!">>, Req),
    ?dump(cowboy_req:path(Req)),
    {ok, Req2, Opts}.