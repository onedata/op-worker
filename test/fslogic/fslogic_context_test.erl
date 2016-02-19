%%%--------------------------------------------------------------------
%%% @author Piotr Ociepka
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Unit tests for fslogic_context module.
%%% @end
%%%--------------------------------------------------------------------

-module(fslogic_context_test).
-author("Piotr Ociepka").

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Test functions
%%%===================================================================

%% tests gen_global_session_id(_, undefined) call
undefined_id_test() ->
  ?assertEqual(undefined,
    fslogic_context:gen_global_session_id(<<"Provider">>, undefined)).

%% tests function gen_global_session_id/2
gen_id_test() ->
  ?assertEqual(<<"Provider", "::", "Session">>,
    fslogic_context:gen_global_session_id("Provider", "Session")),
  ?assertEqual(<<"2", "::", "3">>,
    fslogic_context:gen_global_session_id(2, 3)),
  ?assertEqual(<<"one", "::", "data">>,
    fslogic_context:gen_global_session_id(one, data)).

%% tests function read_global_session_id/1
read_id_test() ->
  ?assertEqual({<<"Provider">>, <<"Session">>},
    fslogic_context:read_global_session_id(<<"Provider::Session">>)),
  ?assertEqual({<<"123DHK">>, <<"54">>},
    fslogic_context:read_global_session_id(<<"123DHK", "::", "54">>)),
  ?assertException(error, {badmatch, [<<"A">>]},
    fslogic_context:read_global_session_id(<<"A">>)),
  ?assertException(error, {badmatch, [<<"">>]},
    fslogic_context:read_global_session_id(<<"">>)).

%% tests function is_global_session_id/1
is_id_test() ->
  ?assert(fslogic_context:is_global_session_id(<<"123::ABC">>)),
  ?assertNot(fslogic_context:is_global_session_id(<<"123:ABC">>)),
%% @todo Should ID have only one "::" token?
%%   ?assertNot(fslogic_context:is_global_session_id(<<"1::2::3">>)),
  ?assertNot(fslogic_context:is_global_session_id(<<"id">>)),
  ?assertNot(fslogic_context:is_global_session_id(<<"">>)).

%% tests results of sequential use of
%% read_global_session_id/1 and gen_global_session_id/2
cross_test() ->
  {ProviderID, SessionID} =
    fslogic_context:read_global_session_id(<<"Provider::Session">>),
  ?assertEqual(<<"Provider::Session">>,
    fslogic_context:gen_global_session_id(ProviderID, SessionID)),
  ?assertEqual({<<"Provider">>, <<"Session">>},
    fslogic_context:read_global_session_id(
      fslogic_context:gen_global_session_id("Provider", "Session"))).

-endif.