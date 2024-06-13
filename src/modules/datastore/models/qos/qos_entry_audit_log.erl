%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc 
%%% This module is responsible for storing audit log of QoS operations on files. 
%%% The audit log stores logs concerning all files affected by a specific QoS entry.
%%% @end
%%%-------------------------------------------------------------------
-module(qos_entry_audit_log).
-author("Michal Stanisz").

-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("cluster_worker/include/audit_log.hrl").
-include_lib("cluster_worker/include/modules/datastore/infinite_log.hrl").

%% API
-export([
    report_synchronization_started/3,
    report_file_synchronized/3,
    report_file_synchronization_skipped/4,
    report_file_synchronization_failed/4,
    destroy/1,
    browse_content/2
]).

-type id() :: qos_entry:id().
-type skip_reason() :: file_deleted_locally | reconciliation_already_in_progress.

-export_type([id/0]).

-define(LOG_OPTS, #{
    size_pruning_threshold => op_worker:get_env(qos_entry_audit_log_max_size, 10000)
    %% defaults are used for age pruning and expiration; @see audit_log.erl
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec report_synchronization_started(id(), file_id:file_guid(), file_meta:path()) ->
    ok | {error, term()}.
report_synchronization_started(Id, FileGuid, FilePath) ->
    audit_log:append(Id, ?LOG_OPTS, #audit_log_append_request{
        severity = ?INFO_AUDIT_LOG_SEVERITY,
        content = #{
            <<"status">> => <<"scheduled">>,
            <<"fileId">> => file_guid_to_object_id(FileGuid),
            <<"description">> => <<"Remote replica differs, reconciliation started.">>,
            <<"path">> => FilePath
        }
    }).


-spec report_file_synchronized(id(), file_id:file_guid(), file_meta:path()) -> 
    ok | {error, term()}.
report_file_synchronized(Id, FileGuid, FilePath) ->
    audit_log:append(Id, ?LOG_OPTS, #audit_log_append_request{
        severity = ?INFO_AUDIT_LOG_SEVERITY,
        content = #{
            <<"status">> => <<"completed">>,
            <<"fileId">> => file_guid_to_object_id(FileGuid),
            <<"description">> => <<"Local replica reconciled.">>,
            <<"path">> => FilePath
        }
    }).


-spec report_file_synchronization_skipped(id(), file_id:file_guid(), file_meta:path(), skip_reason()) ->
    ok | {error, term()}.
report_file_synchronization_skipped(Id, FileGuid, FilePath, Reason) ->
    audit_log:append(Id, ?LOG_OPTS, #audit_log_append_request{
        severity = ?INFO_AUDIT_LOG_SEVERITY,
        content = #{
            <<"status">> => <<"skipped">>,
            <<"fileId">> => file_guid_to_object_id(FileGuid),
            <<"description">> => skip_reason_to_description(Reason),
            <<"path">> => FilePath
        }
    }).


-spec report_file_synchronization_failed(id(), file_id:file_guid(), file_meta:path(), {error, term()}) -> 
    ok | {error, term()}.
report_file_synchronization_failed(Id, FileGuid, FilePath, Error) ->
    ErrorJson = errors:to_json(Error),

    audit_log:append(Id, ?LOG_OPTS, #audit_log_append_request{
        severity = ?ERROR_AUDIT_LOG_SEVERITY,
        content = #{
            <<"status">> => <<"failed">>,
            <<"fileId">> => file_guid_to_object_id(FileGuid),
            <<"description">> => str_utils:format_bin(
                "Failed to reconcile local replica: ~ts",
                [maps:get(<<"description">>, ErrorJson)]
            ),
            <<"reason">> => ErrorJson,
            <<"path">> => FilePath
        }
    }).


-spec destroy(id()) -> ok | {error, term()}.
destroy(Id) ->
    audit_log:delete(Id).


-spec browse_content(id(), audit_log_browse_opts:opts()) ->
    {ok, audit_log:browse_result()} | errors:error().
browse_content(Id, Opts) ->
    audit_log:browse(Id, Opts).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec file_guid_to_object_id(file_id:file_guid()) -> file_id:objectid().
file_guid_to_object_id(FileGuid) ->
    {ok, ObjectId} = file_id:guid_to_objectid(FileGuid),
    ObjectId.


%% @private
-spec skip_reason_to_description(skip_reason()) -> binary().
skip_reason_to_description(file_deleted_locally) ->
    <<"Remote replica differs, ignoring since the file has been deleted locally.">>;
skip_reason_to_description(reconciliation_already_in_progress) ->
    <<"Remote replica differs, reconciliation already in progress.">>.
