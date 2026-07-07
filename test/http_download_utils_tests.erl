%%%--------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Unit tests for http_download_utils module.
%%% @end
%%%--------------------------------------------------------------------
-module(http_download_utils_tests).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").


-define(f(X), http_download_utils:ascii_filename_fallback(X)).


ascii_filename_fallback_test_() ->
    [
        ?_assertEqual(<<"simple.txt">>,       ?f(<<"simple.txt"/utf8>>)),
        ?_assertEqual(<<"report 2024.pdf">>,  ?f(<<"report 2024.pdf"/utf8>>)),
        ?_assertEqual(<<"na__ve caf__.txt">>, ?f(<<"naïve café.txt"/utf8>>)),
        ?_assertEqual(<<"_________.txt">>,    ?f(<<"日本語.txt"/utf8>>)),
        ?_assertEqual(<<"file (copy).txt">>,  ?f(<<"file (copy).txt"/utf8>>)),
        ?_assertEqual(<<"100% done.txt">>,    ?f(<<"100% done.txt"/utf8>>)),
        ?_assertEqual(<<"r__sum__.pdf">>,     ?f(<<"résumé.pdf"/utf8>>)),
        ?_assertEqual(<<"say _hello_.txt">>,  ?f(<<"say \"hello\".txt"/utf8>>)),
        ?_assertEqual(<<"path;name.txt">>,    ?f(<<"path;name.txt"/utf8>>)),
        ?_assertEqual(<<"back_slash.txt">>,   ?f(<<"back\\slash.txt"/utf8>>)),
        ?_assertEqual(<<"">>,                 ?f(<<""/utf8>>))
    ].


-endif.
