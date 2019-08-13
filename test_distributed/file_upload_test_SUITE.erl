%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% File upload tests
%%% @end
%%%-------------------------------------------------------------------
-module(file_upload_test_SUITE).
-author("Bartosz Walkowicz").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/api_errors.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% API
-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    dummy_test/1
]).

all() ->
    ?ALL([
        dummy_test
    ]).

%%%===================================================================
%%% Test functions
%%%===================================================================


dummy_test(Config) ->
    ok.


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    Config.


end_per_suite(Config) ->
    Config.


init_per_testcase(_Case, Config) ->
    Config.


end_per_testcase(_Case, Config) ->
    ok.
