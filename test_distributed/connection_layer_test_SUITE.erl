%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Empty test SUITE to be implemented in version 18.02.2.
%%% @end
%%%-------------------------------------------------------------------
-module(connection_layer_test_SUITE).
-author("Lukasz Opiola").

-include_lib("ctool/include/test/performance.hrl").

%% API
-export([all/0]).
-export([dummy_test/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

all() -> ?ALL([dummy_test]).

dummy_test(_Config) ->
    ok.