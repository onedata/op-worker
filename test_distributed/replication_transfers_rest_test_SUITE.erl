%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains tests of replication jobs, scheduled via REST.
%%% @end
%%%-------------------------------------------------------------------
-module(replication_transfers_rest_test_SUITE).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("modules/fslogic/acl.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("cluster_worker/include/global_definitions.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).
-export([
    file_replication_failures_should_fail_whole_transfer/1,
    many_simultaneous_failed_transfers/1,
    file_removed_during_replication/1,
    rtransfer_works_between_providers_with_different_ports/1,
    warp_time_during_replication/1
]).

all() -> [
    % file_replication_failures_should_fail_whole_transfer, TODO uncomment after resolving VFS-4742
    many_simultaneous_failed_transfers,
    file_removed_during_replication,
    rtransfer_works_between_providers_with_different_ports,
    warp_time_during_replication
].

%%%===================================================================
%%% API
%%%===================================================================

file_replication_failures_should_fail_whole_transfer(Config) ->
    replication_transfers_test_base:file_replication_failures_should_fail_whole_transfer(Config, rest, guid).

many_simultaneous_failed_transfers(Config) ->
    replication_transfers_test_base:many_simultaneous_failed_transfers(Config, rest, guid).

file_removed_during_replication(Config) ->
    replication_transfers_test_base:file_removed_during_replication(Config, rest, guid).

rtransfer_works_between_providers_with_different_ports(Config) ->
    replication_transfers_test_base:rtransfer_works_between_providers_with_different_ports(Config, rest).

warp_time_during_replication(Config) ->
    replication_transfers_test_base:warp_time_during_replication(Config, rest).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    replication_transfers_test_base:init_per_suite(Config).

end_per_suite(Config) ->
    replication_transfers_test_base:end_per_suite(Config).

init_per_testcase(Case, Config) ->
    replication_transfers_test_base:init_per_testcase(Case ,Config).

end_per_testcase(Case, Config) ->
    replication_transfers_test_base:end_per_testcase(Case, Config).
