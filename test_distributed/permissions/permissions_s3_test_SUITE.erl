%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Dummy test suite for s3 permissions tests.
%%% @end
%%%--------------------------------------------------------------------
-module(permissions_s3_test_SUITE).
-author("Bartosz Walkowicz").

%% export for ct
-export([all/0]).

%% tests
-export([dummy_test/1]).

all() -> [dummy_test].


%%%===================================================================
%%% Test functions
%%%===================================================================


dummy_test(_Config) ->
    ok.
