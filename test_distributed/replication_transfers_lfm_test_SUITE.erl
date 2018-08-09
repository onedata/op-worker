%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains tests of replication scheduled via LFM.
%%% @end
%%%-------------------------------------------------------------------
-module(replication_transfers_lfm_test_SUITE).
-author("Jakub Kudzia").

%% API
-export([all/0, foo_test/1]).

%%%===================================================================
%%% API
%%%===================================================================

all() -> [foo_test].

%%%===================================================================
%%% Internal functions
%%%===================================================================

foo_test(_) ->
    ok.
