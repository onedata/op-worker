%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This header file contains record definitions and macros used in appmock client to
%%% interface with remote control endpoint.
%%% @end
%%%-------------------------------------------------------------------

% Term that is sent back when an operation has completed successfully.
-define(OK_RESULT, [{<<"result">>, <<"ok">>}]).

% Endpoint used to verify if all mocked endpoint were requested in proper order.
-define(VERIFY_ALL_PATH, "/verify_all").
% Transform a proplist of pairs {Port, Path} into a term that is sent as JSON to verify_all endpoint (client side).
-define(VERIFY_ALL_PACK_REQUEST(_VerificationList),
    lists:map(
        fun({_Port, _Path}) ->
            {<<"mapping">>, [{<<"port">>, _Port}, {<<"path">>, _Path}]}
        end, _VerificationList)
).
% Transform a struct obtained by decoding JSON into a proplist of pairs {Port, Path} (server side).
-define(VERIFY_ALL_UNPACK_REQUEST(_Struct),
    lists:map(
        fun({<<"mapping">>, [{<<"port">>, _Port}, {<<"path">>, _Path}]}) ->
            {_Port, _Path}
        end, _Struct)
).
% Produces an error message if verification fails (server side).
-define(VERIFY_ALL_PACK_ERROR(_History),
    [{<<"result">>, <<"error">>}, {<<"history">>, ?VERIFY_ALL_PACK_REQUEST(_History)}]).
% Retrieves the error details from verify_all error (actual request history) (client side).
-define(VERIFY_ALL_UNPACK_ERROR(_RespBody),
    begin
        [{<<"result">>, <<"error">>}, {<<"history">>, _Struct}] = _RespBody,
        ?VERIFY_ALL_UNPACK_REQUEST(_Struct)
    end
).


% Endpoint used to verify if a mocked endpoint has been requested certain amount of times.
-define(VERIFY_MOCK_PATH, "/verify").
% Creates a term that is sent as JSON to verify_mock endpoint (client side).
-define(VERIFY_MOCK_PACK_REQUEST(_Port, _Path, _Number),
    [
        {<<"port">>, _Port},
        {<<"path">>, _Path},
        {<<"number">>, _Number}
    ]
).
% Retrieves params sent to verify_mock endpoint (server side).
-define(VERIFY_MOCK_UNPACK_REQUEST(_Struct),
    {
        proplists:get_value(<<"port">>, _Struct),
        proplists:get_value(<<"path">>, _Struct),
        proplists:get_value(<<"number">>, _Struct)
    }
).
% Produces an error message if verification fails (server side).
-define(VERIFY_MOCK_PACK_ERROR(_Number),
    [{<<"result">>, <<"error">>}, {<<"number">>, _Number}]).
% Produces an error message if the endpoint requested to be verified does not exis (server side).
-define(VERIFY_MOCK_PACK_ERROR_WRONG_ENDPOINT,
    [{<<"result">>, <<"error">>}, {<<"reason">>, <<"wrong_endpoint">>}]).
% Retrieves the error details from verify_mock error (client side).
-define(VERIFY_MOCK_UNPACK_ERROR(_RespBody),
    case _RespBody of
        [{<<"result">>, <<"error">>}, {<<"number">>, _Number}] -> {error, _Number};
        [{<<"result">>, <<"error">>}, {<<"reason">>, <<"wrong_endpoint">>}] -> {error, wrong_endpoint}
    end
).