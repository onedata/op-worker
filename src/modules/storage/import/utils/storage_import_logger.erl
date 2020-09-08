%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains functions for adding log entries in sync audit log
%%% file.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_import_logger).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([log_scan_started/3, log_scan_finished/3, log_scan_cancelled/3,
    log_import/4, log_update/5, log_deletion/4]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec log_scan_started(od_space:id(), non_neg_integer(), traverse:id()) -> ok.
log_scan_started(SpaceId, ScanNum, TaskId) ->
    ?debug("Storage sync scan ~p started", [TaskId]),
    log("Storage sync scan no. ~p started.", [ScanNum], SpaceId).

-spec log_scan_finished(od_space:id(), non_neg_integer(), traverse:id()) -> ok.
log_scan_finished(SpaceId, ScanNum, TaskId) ->
    ?debug("Storage sync scan ~p finished", [TaskId]),
    log("Storage sync scan no. ~p finished.", [ScanNum], SpaceId).

-spec log_scan_cancelled(od_space:id(), non_neg_integer(), traverse:id()) -> ok.
log_scan_cancelled(SpaceId, ScanNum, TaskId) ->
    ?debug("Storage sync scan ~p canceled", [TaskId]),
    log("Storage sync scan no. ~p cancelled.", [ScanNum], SpaceId).

%%-------------------------------------------------------------------
%% @doc
%% Function used to add log of import to sync audit log.
%% @end
%%-------------------------------------------------------------------
-spec log_import(helpers:file_id(), file_meta:path(), file_meta:uuid(), od_space:id()) -> ok.
log_import(StorageFileId, CanonicalPath, FileUuid, SpaceId) ->
    log("Creation of storage file ~s has been detected.~n"
    "Corresponding file ~s with uuid ~s has been imported.",
        [StorageFileId, CanonicalPath, FileUuid], SpaceId).

%%-------------------------------------------------------------------
%% @doc
%% Function used to add log of update to sync audit log.
%% @end
%%-------------------------------------------------------------------
-spec log_update(helpers:file_id(), file_meta:path(), file_meta:uuid(), od_space:id(),
    [storage_import_engine:file_attr_name()]) -> ok.
log_update(StorageFileId, CanonicalPath, FileUuid, SpaceId, UpdatedAttrs) ->
    log("Update of storage file ~s has been detected. Updated attrs: ~w.~n"
    "Corresponding file ~s with uuid ~s has been updated.",
        [StorageFileId, UpdatedAttrs, CanonicalPath, FileUuid], SpaceId).

%%-------------------------------------------------------------------
%% @doc
%% Function used to add log of deletion to sync audit log.
%% @end
%%-------------------------------------------------------------------
-spec log_deletion(helpers:file_id(), file_meta:path(), file_meta:uuid(), od_space:id()) -> ok.
log_deletion(StorageFileId, CanonicalPath, FileUuid, SpaceId) ->
    log("Deletion of storage file ~p has been detected.~n"
    "Corresponding file ~s with uuid ~s has been deleted",
        [StorageFileId, CanonicalPath, FileUuid], SpaceId).

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
        storage_import_audit_log_file_max_size, 524288000), % 500 MB
    logger:log_with_rotation(LogFile, Format, Args, MaxSize).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns path of sync audit log for given SpaceId.
%% @end
%%-------------------------------------------------------------------
-spec audit_log_file_name(od_space:id()) -> string().
audit_log_file_name(SpaceId) ->
    LogFilePrefix = application:get_env(?APP_NAME, storage_import_audit_log_file_prefix,
        "/tmp/storage_import_"),
    LogFileExtension = application:get_env(?APP_NAME, storage_import_audit_log_file_extension,
        ".log"),
    LogFilePrefix ++ str_utils:to_list(SpaceId) ++ LogFileExtension.