-module(appmock_client).

-include_lib("ctool/include/logging.hrl").
-include("appmock_internal.hrl").

%% API
-export([verify_mock/4, verify_all_mocks/2]).


verify_mock(Hostname, Port, Path, NumberOfCalls) ->
    try
        JSON = appmock_utils:encode_to_json(?VERIFY_MOCK_PACK_REQUEST(Port, Path, NumberOfCalls)),
        {ok, RemoteControlPort} = application:get_env(?APP_NAME, remote_control_port),
        {200, _, RespBodyJSON} = appmock_utils:https_request(Hostname, RemoteControlPort,
            ?VERIFY_MOCK_PATH, post, [], JSON),
        RespBody = appmock_utils:decode_from_json(RespBodyJSON),
        ?dump(RespBody),
        case RespBody of
            ?OK_RESULT ->
                ok;
            _ ->
                Number = ?VERIFY_MOCK_UNPACK_ERROR(RespBody),
                {different, Number}
        end
    catch T:M ->
        ?error("Error in verify_mock - ~p:~p", [T, M]),
        {error, M}
    end.


verify_all_mocks(Hostname, VerificationList) ->
    try
        JSON = appmock_utils:encode_to_json(?VERIFY_ALL_PACK_REQUEST(VerificationList)),
        {ok, RemoteControlPort} = application:get_env(?APP_NAME, remote_control_port),
        {200, _, RespBodyJSON} = appmock_utils:https_request(Hostname, RemoteControlPort,
            ?VERIFY_ALL_PATH, post, [], JSON),
        RespBody = appmock_utils:decode_from_json(RespBodyJSON),
        case RespBody of
            ?OK_RESULT ->
                ok;
            _ ->
                HistoryBin = ?VERIFY_ALL_UNPACK_ERROR(RespBody),
                History = [{binary_to_integer(Port), Path} || {Port, Path} <- HistoryBin],
                {different, History}
        end
    catch T:M ->
        ?error("Error in verify_all_mocks - ~p:~p", [T, M]),
        {error, M}
    end.
