%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains helper functions for storage_sync
%%% @end
%%%-------------------------------------------------------------------
-module(storage_sync_logger).
-author("Jakub Kudzia").

-include("global_definitions.hrl").

%% API
-export([log_scan_started/2, log_scan_finished/2, log_scan_cancelled/2,
    log_import/2, log_update/2, log_deletion/2]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec log_scan_started(od_space:id(), non_neg_integer()) -> ok.
log_scan_started(SpaceId, ScanNum) ->
    log("Storage sync scan no. ~p started.", [ScanNum], SpaceId).

-spec log_scan_finished(od_space:id(), non_neg_integer()) -> ok.
log_scan_finished(SpaceId, ScanNum) ->
    log("Storage sync scan no. ~p finished.", [ScanNum], SpaceId).

-spec log_scan_cancelled(od_space:id(), non_neg_integer()) -> ok.
log_scan_cancelled(SpaceId, ScanNum) ->
    log("Storage sync scan no. ~p cancelled.", [ScanNum], SpaceId).

%%-------------------------------------------------------------------
%% @doc
%% Function used to add log of import to sync audit log.
%% @end
%%-------------------------------------------------------------------
-spec log_import(file_meta:path(), od_space:id()) -> ok.
log_import(StorageFileId, SpaceId) ->
    log("File ~s has been imported", [StorageFileId], SpaceId).

%%-------------------------------------------------------------------
%% @doc
%% Function used to add log of update to sync audit log.
%% @end
%%-------------------------------------------------------------------
-spec log_update(file_meta:path(), od_space:id()) -> ok.
log_update(StorageFileId, SpaceId) ->
    log("Update of file ~s has been detected", [StorageFileId], SpaceId).

%%-------------------------------------------------------------------
%% @doc
%% Function used to add log of deletion to sync audit log.
%% @end
%%-------------------------------------------------------------------
-spec log_deletion(file_meta:path(), od_space:id()) -> ok.
log_deletion(StorageFileId, SpaceId) ->
    log("File ~s has been deleted", [StorageFileId], SpaceId).

%%===================================================================
%% Internal functions
%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Wrapper for logger:log_with_rotation/4 function
%% @end
%%-------------------------------------------------------------------
-spec log(Format :: io:format(), Args :: [term()], od_space:id()) -> ok.
log(Format, Args, SpaceId) ->
    LogFile = audit_log_file_name(SpaceId),
    MaxSize = application:get_env(?APP_NAME,
        storage_sync_audit_log_file_format_max_size, 524288000), % 500 MB
    logger:log_with_rotation(LogFile, Format, Args, MaxSize).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns path of sync audit log for given SpaceId.
%% @end
%%-------------------------------------------------------------------
-spec audit_log_file_name(od_space:id()) -> string().
audit_log_file_name(SpaceId) ->
    LogFilePrefix = application:get_env(?APP_NAME, storage_sync_audit_log_file_prefix,
        "/tmp/storage_sync_"),
    LogFileExtension = application:get_env(?APP_NAME, storage_sync_audit_log_file_extension,
        ".log"),
    LogFilePrefix ++ str_utils:to_list(SpaceId) ++ LogFileExtension.