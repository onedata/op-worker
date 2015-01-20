-module(request_handler).

-include_lib("ctool/include/logging.hrl").

%% Cowboy API
-export([init/3, handle/2, terminate/3]).

%% API
-export([load_description/1]).


init(_Type, Req, []) ->
    {ok, Req, undefined}.


handle(Req, State) ->
    {ok, Req2} = cowboy_req:reply(200, [
        {<<"content-type">>, <<"text/plain">>}
    ], <<"Hello world!">>, Req),
    {ok, Req2, State}.


terminate(_Reason, _Req, _State) ->
    ok.


load_description(FilePath) ->
    os:cmd("cp " ++ FilePath ++ " ."),
    FileName = filename:basename(FilePath),
    {ok, ModuleName} = compile:file(FileName),
    {ok, Bin} = file:read_file(filename:rootname(FileName) ++ ".beam"),
    erlang:load_module(ModuleName, Bin),
    ok.