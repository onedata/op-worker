-module(remote_control_handler).

-include_lib("ctool/include/logging.hrl").
-include("appmock_internal.hrl").

%% Cowboy API
-export([init/3, handle/2, terminate/3]).


init(_Type, Req, Args) ->
    {ok, Req, Args}.


handle(Req, State) ->
    [Path] = State,
    {ok, Req2} =
        try
            case Path of
                ?VERIFY_ALL_PATH ->
                    appmock_logic:verify_all_mocks(Req);
                ?VERIFY_MOCK_PATH ->
                    appmock_logic:verify_mock(Req)
            end
        catch T:M ->
            ?error_stacktrace("Error in remote_control_handler. Path: ~p. ~p:~p.",
                [Path, T, M]),
            {ok, _ErrorReq} = cowboy_req:reply(500, Req)
        end,
    {ok, Req2, State}.


terminate(_Reason, _Req, _State) ->
    ok.