-module(appmock_client).

-include_lib("ctool/include/logging.hrl").
-include("appmock_internal.hrl").

%% API
-export([verify_mocks/2]).

verify_mocks(Hostname, VerificationList) ->
    VerificationListBin = lists:map(
        fun({Port, Path}) ->
            {Port, list_to_binary(Path)}
        end, VerificationList),
    JSON = appmock_utils:encode_to_json(VerificationListBin),
    {ok, RemoteControlPort} = application:get_env(?APP_NAME, remote_control_port),
    {Code, RespHeaders, RespBody} = appmock_utils:https_request(Hostname, RemoteControlPort,
        ?REMOTE_CONTROL_VERIFY_PATH, post, [], JSON),
    ?dump({Code, RespHeaders, RespBody}).
