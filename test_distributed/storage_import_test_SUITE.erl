%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc This module tests storage import
%%% @end
%%%-------------------------------------------------------------------
-module(storage_import_test_SUITE).
-author("Jakub Kudzia").


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