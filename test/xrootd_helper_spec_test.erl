%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Unit tests of xrootd_helper_spec: the check of credentials.
%%% @end
%%%-------------------------------------------------------------------
-module(xrootd_helper_spec_test).
-author("Bartosz Walkowicz").

-include_lib("eunit/include/eunit.hrl").


%%%===================================================================
%%% Tests
%%%===================================================================


pwd_type_without_credentials_passes_test() ->
    ?assertEqual(ok, xrootd_helper_spec:validate_credentials(#{<<"credentialsType">> => <<"pwd">>})).


pwd_type_with_credentials_passes_test() ->
    ?assertEqual(ok, xrootd_helper_spec:validate_credentials(#{
        <<"credentialsType">> => <<"pwd">>,
        <<"credentials">> => <<"admin:password">>
    })).


missing_credentials_type_is_refused_test() ->
    ?assertEqual(
        {error, {missing_field, <<"credentialsType">>}},
        xrootd_helper_spec:validate_credentials(#{<<"credentials">> => <<"admin:password">>})
    ).
