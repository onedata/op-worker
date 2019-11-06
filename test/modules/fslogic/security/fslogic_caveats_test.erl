%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Unit tests for fslogic_caveats module.
%%% @end
%%%--------------------------------------------------------------------
-module(fslogic_caveats_test).
-author("Bartosz Walkowicz").

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").


check_data_path_relation_test_() -> [
    ?_assertEqual(
        {undefined, undefined},
        fslogic_caveats:check_data_path_relation(
            <<"/root/binar">>,
            [<<"boot">>, <<"/root/users">>]
        )
    ),
    ?_assertEqual(
        {undefined, undefined},
        fslogic_caveats:check_data_path_relation(
            <<"/root/binar">>,
            [<<"/root/bin">>, <<"/root/binaries">>]
        )
    ),
    ?_assertEqual(
        {ancestor, gb_sets:add(<<"asd">>, gb_sets:add(<<"chmod">>, gb_sets:new()))},
        fslogic_caveats:check_data_path_relation(
            <<"/root/bin">>,
            [<<"/boot/binaries">>, <<"/root/bin/chmod">>, <<"/boot/root/asd">>, <<"/root/bin/asd">>]
        )
    ),
    ?_assertEqual(
        {subpath, undefined},
        fslogic_caveats:check_data_path_relation(
            <<"/root/binaries/chmod">>,
            [<<"/boot/bin">>, <<"/root/binaries">>]
        )
    ),
    ?_assertEqual(
        {subpath, undefined},
        fslogic_caveats:check_data_path_relation(
            <<"/root/bin/users">>,
            [<<"/boot/bin/users/qwe">>, <<"/root/bin">>]
        )
    )
].


is_subpath_test_() -> [
    ?_assertEqual(
        false,
        fslogic_caveats:is_subpath(<<"/c/b/a">>, <<"/a/b/c">>)
    ),
    ?_assertEqual(
        false,
        fslogic_caveats:is_subpath(<<"/a/b/c">>, <<"/a/b/c/">>)
    ),
    ?_assertEqual(
        true,
        fslogic_caveats:is_subpath(<<"/a/b/c">>, <<"/a/b/c">>)
    ),
    ?_assertEqual(
        true,
        fslogic_caveats:is_subpath(<<"/a/b/c/d">>, <<"/a/b/c">>)
    )
].


-endif.
