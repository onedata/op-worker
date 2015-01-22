-define(OK_RESULT, [{<<"result">>, <<"ok">>}]).

-define(VERIFY_ALL_PATH, "/verify_all").
-define(VERIFY_ALL_REQUEST(_VerificationList),
    lists:map(
        fun({_Port, _Path}) ->
            {<<"mapping">>, [{<<"port">>, _Port}, {<<"path">>, _Path}]}
        end, _VerificationList)
).
-define(VERIFY_ALL_PARAMS(_Struct),
    lists:map(
        fun({<<"mapping">>, [{<<"port">>, _Port}, {<<"path">>, _Path}]}) ->
            {_Port, _Path}
        end, _Struct)
).
-define(VERIFY_ALL_ERROR(_History),
    [{<<"result">>, <<"error">>}, {<<"history">>, _History}]).
-define(GET_MESSAGE_VERIFY_ALL_ERROR(_RespBody),
    begin
        [{<<"result">>, <<"error">>}, {<<"history">>, _History}] = _RespBody,
        _History
    end
).


-define(VERIFY_MOCK_PATH, "/verify").
-define(VERIFY_MOCK_REQUEST(_Port, _Path, _Number),
    [
        {<<"port">>, _Port},
        {<<"path">>, _Path},
        {<<"number">>, _Number}
    ]
).
-define(VERIFY_MOCK_PARAMS(_Struct),
    {
        proplists:get_value(<<"port">>, _Struct),
        proplists:get_value(<<"path">>, _Struct),
        proplists:get_value(<<"number">>, _Struct)
    }
).
-define(VERIFY_MOCK_ERROR(_Number),
    [{<<"result">>, <<"error">>}, {<<"number">>, _Number}]).
-define(GET_MESSAGE_VERIFY_MOCK_ERROR(_RespBody),
    begin
        [{<<"result">>, <<"error">>}, {<<"number">>, _Number}] = _RespBody,
        _Number
    end
).