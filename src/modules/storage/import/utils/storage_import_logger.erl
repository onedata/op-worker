%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains functions for adding log entries in import audit log
%%% file.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_import_logger).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([log_scan_started/3, log_scan_finished/3, log_scan_cancelled/3,
    log_creation/4, log_modification/5, log_deletion/4, log_failure/3]).


%%%===================================================================
%%% API functions
%%%===================================================================

-spec log_scan_started(od_space:id(), non_neg_integer(), traverse:id()) -> ok.
log_scan_started(SpaceId, ScanNum, TaskId) ->
    ?debug("Auto storage import scan ~ts started", [TaskId]),
    log("Auto storage import scan no. ~tp started.", [ScanNum], SpaceId).

-spec log_scan_finished(od_space:id(), non_neg_integer(), traverse:id()) -> ok.
log_scan_finished(SpaceId, ScanNum, TaskId) ->
    ?debug("Auto storage import scan ~ts finished", [TaskId]),
    log("Auto storage import scan no. ~tp finished.", [ScanNum], SpaceId).

-spec log_scan_cancelled(od_space:id(), non_neg_integer(), traverse:id()) -> ok.
log_scan_cancelled(SpaceId, ScanNum, TaskId) ->
    ?debug("Auto storage import scan ~ts canceled", [TaskId]),
    log("Auto storage import scan no. ~tp cancelled.", [ScanNum], SpaceId).

%%-------------------------------------------------------------------
%% @doc
%% Function used to log detection of file creation to import audit log.
%% @end
%%-------------------------------------------------------------------
-spec log_creation(helpers:file_id(), file_meta:path(), file_meta:uuid(), od_space:id()) -> ok.
log_creation(StorageFileId, CanonicalPath, FileUuid, SpaceId) ->
    log("Creation of storage file ~ts has been detected.~n"
    "Corresponding file ~ts with uuid ~ts has been created.",
        [StorageFileId, CanonicalPath, FileUuid], SpaceId).

%%-------------------------------------------------------------------
%% @doc
%% Function used to log detection of file modification to import audit log.
%% @end
%%-------------------------------------------------------------------
-spec log_modification(helpers:file_id(), file_meta:path(), file_meta:uuid(), od_space:id(),
    [storage_import_engine:file_attr_name()]) -> ok.
log_modification(StorageFileId, CanonicalPath, FileUuid, SpaceId, UpdatedAttrs) ->
    log("Modification of storage file ~ts has been detected. Updated attrs: ~w.~n"
    "Corresponding file ~ts with uuid ~ts has been modified.",
        [StorageFileId, UpdatedAttrs, CanonicalPath, FileUuid], SpaceId).

%%-------------------------------------------------------------------
%% @doc
%% Function used to log detection of file deletion to import audit log.
%% @end
%%-------------------------------------------------------------------
-spec log_deletion(helpers:file_id(), file_meta:path(), file_meta:uuid(), od_space:id()) -> ok.
log_deletion(StorageFileId, CanonicalPath, FileUuid, SpaceId) ->
    log("Deletion of storage file ~ts has been detected.~n"
    "Corresponding file ~ts with uuid ~ts has been deleted",
        [StorageFileId, CanonicalPath, FileUuid], SpaceId).


%%-------------------------------------------------------------------
%% @doc
%% Function used to log failure of file processing to import audit log.
%% @end
%%-------------------------------------------------------------------
-spec log_failure(helpers:file_id(), term(), od_space:id()) -> ok.
log_failure(StorageFileId, Error, SpaceId) ->
    log("Processing of storage file ~ts has failed due to ~w.~n",
        [StorageFileId, Error], SpaceId).

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
    onedata_logger:log_with_rotation(LogFile, Format, Args, MaxSize).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns path of import audit log for given SpaceId.
%% @end
%%-------------------------------------------------------------------
-spec audit_log_file_name(od_space:id()) -> string().
audit_log_file_name(SpaceId) ->
    LogRootDir = op_worker:get_env(
        storage_import_audit_log_root_dir, "/tmp/storage_import/"
    ),
    LogFileExtension = op_worker:get_env(storage_import_audit_log_file_extension,
        ".log"),
    LogRootDir ++ str_utils:to_list(SpaceId) ++ LogFileExtension.