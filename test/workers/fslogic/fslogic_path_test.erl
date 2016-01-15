%%%--------------------------------------------------------------------
%%% @author Rafał Słota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Unit tests for fslogic_path module.
%%% @end
%%%--------------------------------------------------------------------
-module(fslogic_path_test).
-author("Rafał Słota").

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Test functions
%%%===================================================================

join_test() ->
    ?assertMatch(<<"/1/2/3/4">>, fslogic_path:join([<<"/">>, <<"1">>, <<"2">>, <<"3">>, <<"4">>])),
    ?assertMatch(<<"1/2/3/4">>, fslogic_path:join([<<"1">>, <<"2">>, <<"3">>, <<"4">>])),
    ?assertMatch(<<"/">>, fslogic_path:join([<<"/">>])),
    ?assertMatch(<<"">>, fslogic_path:join([])),
    ?assertMatch(<<"/1/2/3/4">>, fslogic_path:join([<<"/">>, <<"/1">>, <<"//2">>, <<"3">>, <<"4">>])),
    ?assertMatch(<<"/1/2/3/4">>, fslogic_path:join([<<"/">>, <<"1/">>, <<"2////">>, <<"3">>, <<"4">>])),
    ?assertMatch(<<"/1/2/3/4">>, fslogic_path:join([<<"/">>, <<"/1/">>, <<"///2////">>, <<"3">>, <<"4">>])),
    ?assertMatch(<<"/1/2/3/4">>, fslogic_path:join([<<"/">>, <<"/">>, <<"1/">>, <<"///2////">>, <<"3">>, <<"4">>])),

    ok.

split_test() ->
    ?assertMatch([<<"/">>], fslogic_path:split(<<"/">>)),
    ?assertMatch([<<"/">>, <<"1">>, <<"2">>], fslogic_path:split(<<"/1/2">>)),
    ?assertMatch([<<"1">>, <<"2">>], fslogic_path:split(<<"1/2">>)),
    ?assertMatch([<<"/">>, <<"1">>, <<"2">>], fslogic_path:split(<<"///1///2////">>)),
    ?assertMatch([], fslogic_path:split(<<"">>)),

    ok.

-endif.