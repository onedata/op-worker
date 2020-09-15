%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This is a helper module for storage_import_config module.
%%% It encapsulates #auto_scan_config{} record which stores
%%% the configuration of auto storage import scans.
%%% @end
%%%-------------------------------------------------------------------
-module(auto_storage_import_config).
-author("Jakub Kudzia").

-include("modules/storage/import/storage_import.hrl").

%% API
-export([configure/1, configure/2]).
-export([to_map/1]).
-export([is_continuous_scan_enabled/1, get_scan_interval/1]).

-record(auto_storage_import_config, {
    max_depth :: non_neg_integer(),
    sync_acl :: boolean(),
    continuous_scan :: boolean(),
    scan_interval :: non_neg_integer(),
    detect_modifications :: boolean(),
    detect_deletions :: boolean()
}).

-type config() :: #auto_storage_import_config{}.

%% @formatter:off
-type config_map() :: #{
    max_depth => non_neg_integer(),
    sync_acl => boolean(),
    continuous_scan => boolean(),
    scan_interval => non_neg_integer(),
    detect_modifications => boolean(),
    detect_deletions => boolean()
}.
%% @formatter:on

-export_type([config/0, config_map/0]).

-define(DEFAULT_DETECT_DELETIONS, true).
-define(DEFAULT_DETECT_MODIFICATIONS, true).
-define(DEFAULT_SCAN_INTERVAL, 60). % in seconds
-define(DEFAULT_SYNC_ACL, false).
-define(DEFAULT_MAX_DEPTH, 65535).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec configure(config_map()) -> config().
configure(Diff) ->
    configure(default(), Diff).

-spec configure(config(), config_map()) -> config().
configure(#auto_storage_import_config{
    max_depth = MaxDepth,
    sync_acl = SyncAcl,
    continuous_scan = ContinuousScan,
    scan_interval = ScanInterval,
    detect_modifications = DetectModification,
    detect_deletions = DetectDeletions
}, Diff) ->
    #auto_storage_import_config{
        max_depth = maps:get(max_depth, Diff, MaxDepth),
        sync_acl = maps:get(sync_acl, Diff, SyncAcl),
        continuous_scan = maps:get(continuous_scan, Diff, ContinuousScan),
        scan_interval = maps:get(scan_interval, Diff, ScanInterval),
        detect_modifications = maps:get(detect_modifications, Diff, DetectModification),
        detect_deletions = maps:get(detect_deletions, Diff, DetectDeletions)
    }.


-spec to_map(config()) -> json_utils:json_term().
to_map(#auto_storage_import_config{
    max_depth = MaxDepth,
    sync_acl = SyncAcl,
    continuous_scan = ContinuousScan,
    scan_interval = ScanInterval,
    detect_modifications = DetectModification,
    detect_deletions = DetectDeletions
}) ->
    #{
        max_depth => MaxDepth,
        sync_acl => SyncAcl,
        continuous_scan => ContinuousScan,
        scan_interval => ScanInterval,
        detect_modifications => DetectModification,
        detect_deletions => DetectDeletions
    }.

-spec is_continuous_scan_enabled(config()) -> boolean().
is_continuous_scan_enabled(#auto_storage_import_config{continuous_scan = ContinuousScan}) ->
    ContinuousScan.

-spec get_scan_interval(config()) -> non_neg_integer().
get_scan_interval(#auto_storage_import_config{scan_interval = ScanInterval}) ->
    ScanInterval.


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec default() -> config().
default() ->
    #auto_storage_import_config{
        max_depth = ?DEFAULT_MAX_DEPTH,
        sync_acl = ?DEFAULT_SYNC_ACL,
        continuous_scan = false,
        scan_interval = ?DEFAULT_SCAN_INTERVAL,
        detect_modifications = ?DEFAULT_DETECT_MODIFICATIONS,
        detect_deletions = ?DEFAULT_DETECT_DELETIONS
    }.