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
-module(data_constraints_test).
-author("Bartosz Walkowicz").

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").
-include_lib("ctool/include/aai/caveats.hrl").


-define(CV_PATH(__PATHS), #cv_data_path{whitelist = __PATHS}).
-define(CV_OBJECTID(__OBJECTIDS), #cv_data_objectid{whitelist = __OBJECTIDS}).


is_ancestor_test_() -> [
    ?_assertEqual(
        false,
        is_ancestor(<<"/c/b/a">>, <<"/a/b/c">>)
    ),
    ?_assertEqual(
        false,
        is_ancestor(<<"/a/b/c/d">>, <<"/a/b/c/">>)
    ),
    ?_assertEqual(
        false,
        is_ancestor(<<"/a/b/c">>, <<"/a/b/c">>)
    ),
    ?_assertEqual(
        {true, <<"c">>},
        is_ancestor(<<"/a/b">>, <<"/a/b/c">>)
    ),
    ?_assertEqual(
        {true, <<"b">>},
        is_ancestor(<<"/a">>, <<"/a/b/c">>)
    )
].


is_subpath_test_() -> [
    ?_assertEqual(
        false,
        is_subpath(<<"/c/b/a">>, <<"/a/b/c">>)
    ),
    ?_assertEqual(
        false,
        is_subpath(<<"/a/b/c">>, <<"/a/b/c/">>)
    ),
    ?_assertEqual(
        false,
        is_subpath(<<"/a/b/c">>, <<"/a/b/c">>)
    ),
    ?_assertEqual(
        true,
        is_subpath(<<"/a/b/c/d">>, <<"/a/b/c">>)
    )
].


is_path_or_subpath_test_() -> [
    ?_assertEqual(
        false,
        data_constraints:is_path_or_subpath(<<"/c/b/a">>, <<"/a/b/c">>)
    ),
    ?_assertEqual(
        false,
        data_constraints:is_path_or_subpath(<<"/a/b/c">>, <<"/a/b/c/">>)
    ),
    ?_assertEqual(
        true,
        data_constraints:is_path_or_subpath(<<"/a/b/c">>, <<"/a/b/c">>)
    ),
    ?_assertEqual(
        true,
        data_constraints:is_path_or_subpath(<<"/a/b/c/d">>, <<"/a/b/c">>)
    )
].


intersect_path_whitelists_test_() -> [
    ?_assertEqual(
        [],
        data_constraints:intersect_path_whitelists([<<"/q/w/e">>], [<<"/a/s/d">>])
    ),
    ?_assertEqual(
        [<<"/q/w/e">>],
        data_constraints:intersect_path_whitelists([<<"/q/w/e">>], [<<"/e/w/q">>, <<"/q/w/e">>])
    ),
    ?_assertEqual(
        [<<"/z/x/c/d/e">>],
        data_constraints:intersect_path_whitelists([<<"/q/w/e">>, <<"/z/x/c">>], [<<"/a/s/d">>, <<"/z/x/c/d/e">>])
    )
].


consolidate_paths_test_() -> [
    ?_assertEqual(
        [<<"/c/b">>],
        data_constraints:consolidate_paths([<<"/c/b/">>, <<"/c/b/q">>])
    ),
    ?_assertEqual(
        [<<"/a/b/c">>],
        data_constraints:consolidate_paths([<<"/a/b/c/d">>, <<"/a/b/c">>])
    ),
    ?_assertEqual(
        [<<"/a/b/c">>, <<"p/o/i">>],
        data_constraints:consolidate_paths([<<"/a/b/c/d">>, <<"/a/b/c">>, <<"p/o/i">>, <<"/a/b/c/e/w/q">>])
    ),
    ?_assertEqual(
        [<<"/a/b/c">>, <<"/c/b/a">>],
        data_constraints:consolidate_paths([<<"/c/b/a">>, <<"/a/b/c">>])
    )
].


get_test_() ->
    Guid1 = <<"Z3VpZCNfaGFzaF9mNmQyOGY4OTNjOTkxMmVh">>,
    {ok, ObjectId1} = file_id:guid_to_objectid(Guid1),

    Guid2 = <<"V3ZpZCNfaGFzaF9mNmQyOGY4OTNjOTkxMmVh">>,
    {ok, ObjectId2} = file_id:guid_to_objectid(Guid2),

    [
        ?_assertEqual(
            {ok, {constraints, any, any}},
            data_constraints:get([])
        ),
        ?_assertEqual(
            {error, invalid_constraints},
            data_constraints:get([?CV_PATH([<<"/a/b/c">>]), ?CV_PATH([<<"/c/b/a">>])])
        ),
        ?_assertEqual(
            {error, invalid_constraints},
            data_constraints:get([?CV_OBJECTID([])])
        ),
        ?_assertEqual(
            {ok, {constraints, any, [[Guid1, Guid2]]}},
            data_constraints:get([?CV_OBJECTID([ObjectId1, ObjectId2])])
        ),
        ?_assertEqual(
            {ok, {constraints, [<<"/z/x/c/d/e">>], [[Guid2], [Guid1]]}},
            data_constraints:get([
                ?CV_PATH([<<"/q/w/e">>, <<"/z/x/c">>]),
                ?CV_OBJECTID([ObjectId1]),
                ?CV_PATH([<<"/a/s/d">>, <<"/z/x/c/d/e">>]),
                ?CV_OBJECTID([ObjectId2])
            ])
        )
    ].


check_data_path_relation_test_() -> [
    ?_assertEqual(
        undefined,
        data_constraints:check_against_allowed_paths(
            <<"/qwe/binar">>,
            [<<"asd">>, <<"/qwe/users">>]
        )
    ),
    ?_assertEqual(
        undefined,
        data_constraints:check_against_allowed_paths(
            <<"/qwe/binar">>,
            [<<"/qwe/bin">>, <<"/qwe/binaries">>]
        )
    ),
    ?_assertEqual(
        {ancestor, ordsets:from_list([<<"asd">>, <<"chmod">>])},
        data_constraints:check_against_allowed_paths(
            <<"/qwe/bin">>,
            [<<"/asd/binaries">>, <<"/qwe/bin/chmod">>, <<"/asd/qwe/asd">>, <<"/qwe/bin/asd">>]
        )
    ),
    ?_assertEqual(
        subpath,
        data_constraints:check_against_allowed_paths(
            <<"/qwe/binaries/chmod">>,
            [<<"/asd/bin">>, <<"/qwe/binaries">>]
        )
    ),
    ?_assertEqual(
        subpath,
        data_constraints:check_against_allowed_paths(
            <<"/qwe/bin/users">>,
            [<<"/asd/bin/users/qwe">>, <<"/qwe/bin">>]
        )
    ),
    ?_assertEqual(
        undefined,
        data_constraints:check_against_allowed_paths(
            <<"/qwe/bin/users">>,
            [<<"/asd/bin/users/qwe">>]
        )
    )
].


is_ancestor(Path, PossibleSubPath) ->
    data_constraints:is_ancestor(Path, size(Path), PossibleSubPath).


is_subpath(PossibleSubPath, Path) ->
    data_constraints:is_subpath(PossibleSubPath, Path, size(Path)).


-endif.
