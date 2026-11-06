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
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/aai/caveats.hrl").


-define(SPACE_ALPHA, <<"0409171e7ba30b60d332d45c857745d4">>).
-define(SPACE_GAMMA, <<"bdee07d32260a56d159a980ca0d64357">>).

-define(CV_READONLY, #cv_data_readonly{}).
-define(CV_PATH(__PATHS), #cv_data_path{whitelist = __PATHS}).
-define(CV_OBJECTID(__OBJECTIDS), #cv_data_objectid{whitelist = __OBJECTIDS}).


get_test_() ->
    GuidAlpha = file_id:pack_guid(<<"ee07d32260a56d12260a56d159a980ca0">>, ?SPACE_ALPHA),
    {ok, ObjectIdAlpha} = file_id:guid_to_objectid(GuidAlpha),

    GuidGamma = file_id:pack_guid(<<"0d332d45c857745d430b656d159a980ca">>, ?SPACE_GAMMA),
    {ok, ObjectIdGamma} = file_id:guid_to_objectid(GuidGamma),

    [
        ?_assertEqual(
            {ok, {constraints, any, any, any, false}},
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
            {ok, {constraints, [?SPACE_ALPHA, ?SPACE_GAMMA], any, [[GuidAlpha, GuidGamma]], false}},
            data_constraints:get([?CV_OBJECTID([ObjectIdAlpha, ObjectIdGamma])])
        ),
        ?_assertEqual(
            {ok, {constraints, [], [<<"/z/x/c/d/e">>], [[GuidGamma], [GuidAlpha]], false}},
            data_constraints:get([
                ?CV_PATH([<<"/q/w/e">>, <<"/z/x/c">>]),
                ?CV_OBJECTID([ObjectIdAlpha]),
                ?CV_PATH([<<"/a/s/d">>, <<"/z/x/c/d/e">>]),
                ?CV_OBJECTID([ObjectIdGamma])
            ])
        ),
        ?_assertEqual(
            {ok, {constraints, any, any, any, true}},
            data_constraints:get([?CV_READONLY])
        )
    ].


-endif.
