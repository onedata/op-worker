%%%--------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Unit tests for fslogic_acl module.
%%% @end
%%%--------------------------------------------------------------------
-module(dbsync_utils_tests).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

normalize_seq_test() ->
    ?assertEqual(42342, dbsync_utils:normalize_seq(42342)),
    ?assertEqual(423424234, dbsync_utils:normalize_seq(423424234)),
    ?assertEqual(5464564, dbsync_utils:normalize_seq(<<"5464564">>)),
    ?assertEqual(931809318, dbsync_utils:normalize_seq(<<"931809318::423564564564">>)),
    ?assertEqual(423564564564, dbsync_utils:normalize_seq(<<"423564564564::931809318">>)),

    ok.

-endif.