%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains tests of replica_deletion mechanism
%%% @end
%%%-------------------------------------------------------------------
-module(replica_deletion_test_SUITE).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("proto/oneclient/common_messages.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/logging.hrl").

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