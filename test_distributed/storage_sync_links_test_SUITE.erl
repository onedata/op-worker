%%%--------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module tests storage_sync_links module.
%%% @end
%%%--------------------------------------------------------------------
-module(storage_sync_links_test_SUITE).
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