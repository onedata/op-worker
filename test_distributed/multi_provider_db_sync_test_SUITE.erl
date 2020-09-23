%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of db_sync and proxy
%%% @end
%%%-------------------------------------------------------------------
-module(multi_provider_db_sync_test_SUITE).
-author("Michal Wrzeszcz").


%% API
-export([all/0]).
-export([foo_test/1]).

all() -> [
    foo_test
].

%%%===================================================================
%%% API
%%%===================================================================

foo_test(_Config) ->
    ok.