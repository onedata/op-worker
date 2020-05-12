%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains tests which check whether files are created on
%%% storage with proper permissions and ownership.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_files_test_SUITE).
-author("Jakub Kudzia").

%% API
-export([all/0]).

%% Test functions
-export([foo_test/1]).

%%%===================================================================
%%% API
%%%===================================================================

all() -> [foo_test].

%%%===================================================================
%%% Test functions
%%%===================================================================

foo_test(_) ->
    ok.