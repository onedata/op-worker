-module(remote_control_handler).

-include_lib("ctool/include/logging.hrl").
-include("appmock.hrl").

%% Cowboy API
-export([init/3, handle/2, terminate/3]).


init(_Type, Req, Args) ->
    {ok, Req, Args}.


handle(Req, State) ->
    {ok, Req2} = appmock_logic:verify_mocks(Req),
    {ok, Req2, State}.


terminate(_Reason, _Req, _State) ->
    ok.