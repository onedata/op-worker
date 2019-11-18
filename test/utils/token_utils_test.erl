%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Unit tests for token_utils module.
%%% @end
%%%--------------------------------------------------------------------
-module(token_utils_test).
-author("Bartosz Walkowicz").

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").


converge_paths_test_() -> [
    ?_assertEqual(
        [],
        token_utils:intersect_paths([<<"/q/w/e">>], [<<"/a/s/d">>])
    ),
    ?_assertEqual(
        [<<"/q/w/e">>],
        token_utils:intersect_paths([<<"/q/w/e">>], [<<"/e/w/q">>, <<"/q/w/e">>])
    ),
    ?_assertEqual(
        [<<"/z/x/c/d/e">>],
        token_utils:intersect_paths([<<"/q/w/e">>, <<"/z/x/c">>], [<<"/a/s/d">>, <<"/z/x/c/d/e">>])
    )
].


consolidate_paths_test_() -> [
    ?_assertEqual(
        [<<"/c/b">>],
        token_utils:consolidate_paths([<<"/c/b">>, <<"/c/b/q">>])
    ),
    ?_assertEqual(
        [<<"/a/b/c">>],
        token_utils:consolidate_paths([<<"/a/b/c/d">>, <<"/a/b/c">>])
    ),
    ?_assertEqual(
        [<<"/a/b/c">>, <<"p/o/i">>],
        token_utils:consolidate_paths([<<"/a/b/c/d">>, <<"/a/b/c">>, <<"p/o/i">>, <<"/a/b/c/e/w/q">>])
    ),
    ?_assertEqual(
        [<<"/a/b/c">>, <<"/c/b/a">>],
        token_utils:consolidate_paths([<<"/c/b/a">>, <<"/a/b/c">>])
    )
].


-endif.