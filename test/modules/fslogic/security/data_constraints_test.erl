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


-define(CV_READONLY, #cv_data_readonly{}).
-define(CV_PATH(__PATHS), #cv_data_path{whitelist = __PATHS}).
-define(CV_OBJECTID(__OBJECTIDS), #cv_data_objectid{whitelist = __OBJECTIDS}).


get_test_() ->
    Guid1 = <<"Z3VpZCNfaGFzaF9mNmQyOGY4OTNjOTkxMmVh">>,
    {ok, ObjectId1} = file_id:guid_to_objectid(Guid1),

    Guid2 = <<"V3ZpZCNfaGFzaF9mNmQyOGY4OTNjOTkxMmVh">>,
    {ok, ObjectId2} = file_id:guid_to_objectid(Guid2),

    [
        ?_assertEqual(
            {ok, {constraints, any, any, false}},
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
            {ok, {constraints, any, [[Guid1, Guid2]], false}},
            data_constraints:get([?CV_OBJECTID([ObjectId1, ObjectId2])])
        ),
        ?_assertEqual(
            {ok, {constraints, [<<"/z/x/c/d/e">>], [[Guid2], [Guid1]], false}},
            data_constraints:get([
                ?CV_PATH([<<"/q/w/e">>, <<"/z/x/c">>]),
                ?CV_OBJECTID([ObjectId1]),
                ?CV_PATH([<<"/a/s/d">>, <<"/z/x/c/d/e">>]),
                ?CV_OBJECTID([ObjectId2])
            ])
        ),
        ?_assertEqual(
            {ok, {constraints, any, any, true}},
            data_constraints:get([?CV_READONLY])
        )
    ].


-endif.
