%%%--------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module tests file registration mechanism
%%% @end
%%%--------------------------------------------------------------------
-module(file_registration_test_SUITE).
-author("Jakub Kudzia").

%% export for ct
-export([all/0]).

%% tests
-export([foo_test/1]).

%%%===================================================================
%%% API
%%%===================================================================

all() -> [foo_test].

%%%===================================================================
%%% Internal functions
%%%===================================================================

foo_test(_) ->
    ok.