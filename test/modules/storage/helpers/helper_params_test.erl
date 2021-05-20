%%%-------------------------------------------------------------------
%%% @author Piotr Duleba
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Test for functions defined in helper_params module.
%%% @end
%%%-------------------------------------------------------------------
-module(helper_params_test).
-author("Piotr Duleba").

-ifdef(TEST).

-include("global_definitions.hrl").
-include("modules/storage/helpers/helpers.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

-define(CORRECT_URL_BATCH, [
    {<<"hostname:80">>, {ok, http, <<"hostname:80/">>}},
    {<<"hostname:80/path">>, {ok, http, <<"hostname:80/path">>}},
    {<<"hostname:443">>, {ok, https, <<"hostname:443/">>}},
    {<<"hostname:443/path">>, {ok, https, <<"hostname:443/path">>}},

    {<<"http://hostname">>, {ok, http, <<"hostname:80/">>}},
    {<<"http://hostname/path">>, {ok, http, <<"hostname:80/path">>}},
    {<<"https://hostname">>, {ok, https, <<"hostname:443/">>}},
    {<<"https://hostname/path">>, {ok, https, <<"hostname:443/path">>}},

    {<<"http://hostname:80/">>, {ok, http, <<"hostname:80/">>}},
    {<<"http://hostname:80/path">>, {ok, http, <<"hostname:80/path">>}},
    {<<"https://hostname:443/">>, {ok, https, <<"hostname:443/">>}},
    {<<"https://hostname:443/path">>, {ok, https, <<"hostname:443/path">>}},

    {<<"http://hostname:1234/">>, {ok, http, <<"hostname:1234/">>}},
    {<<"http://hostname:1234/path">>, {ok, http, <<"hostname:1234/path">>}},
    {<<"https://hostname:1234/">>, {ok, https, <<"hostname:1234/">>}},
    {<<"https://hostname:1234/path">>, {ok, https, <<"hostname:1234/path">>}}
]).

-define(INCORRECT_URL_BATCH, [
    <<"hostname">>,
    <<"hostname/path">>,
    <<"hostname:1234">>,
    <<"hostname:1234/path">>
]).


correct_url_parse_test() ->
    lists:foreach(fun({InputUrl, ExpResult}) ->
        ?assertEqual(ExpResult, helper_params:parse_url(InputUrl))
    end, ?CORRECT_URL_BATCH).


incorrect_url_parse_test() ->
    lists:foreach(fun(InputUrl) ->
        ?assertThrow(?ERROR_MALFORMED_DATA, helper_params:parse_url(InputUrl))
    end, ?INCORRECT_URL_BATCH).


%%%===================================================================
%%% Internal functions
%%%===================================================================

-endif.

