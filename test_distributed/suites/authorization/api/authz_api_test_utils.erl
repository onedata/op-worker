%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020-2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions for authz api tests.
%%% @end
%%%-------------------------------------------------------------------
-module(authz_api_test_utils).
-author("Bartosz Walkowicz").

-export([
    extract_ok/1
]).


%%%===================================================================
%%% Tests
%%%===================================================================


-spec extract_ok
    (ok | {ok, term()} | {ok, term(), term()} | {ok, term(), term(), term()}) -> ok;
    ({error, term()}) -> {error, term()}.
extract_ok(ok) -> ok;
extract_ok({ok, _}) -> ok;
extract_ok({ok, _, _}) -> ok;
extract_ok({ok, _, _, _}) -> ok;
extract_ok({error, _} = Error) -> Error.
