%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests concerning file metadata get basic API (REST + gs).
%%% @end
%%%-------------------------------------------------------------------
-module(file_metadata_get_api_test_SUITE).
-author("Bartosz Walkowicz").

%% API
-export([all/0]).
-export([dummy_test/1]).

all() -> [
    dummy_test
].


%%%===================================================================
%%% API
%%%===================================================================


dummy_test(_Config) ->
    ok.
