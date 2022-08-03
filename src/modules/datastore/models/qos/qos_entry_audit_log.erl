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
    create/1,
    report_synchronization_started/2,
    report_file_synchronized/2,
    report_file_synchronization_skipped/3,
    report_file_synchronization_failed/3,
    destroy/1,
    browse_content/2
]).

-type id() :: qos_entry:id().
-type skip_reason() :: file_deleted_locally | reconciliation_already_in_progress.

-export_type([id/0]).

-define(LOG_MAX_SIZE, op_worker:get_env(qos_entry_audit_log_max_size, 10000)).
-define(LOG_EXPIRATION, op_worker:get_env(qos_entry_audit_log_expiration_seconds, 1209600)). % 14 days

%%%===================================================================
%%% API
%%%===================================================================

-spec create(id()) -> ok | {error, term()}.
create(Id) ->
    audit_log:create(Id, #{
        size_pruning_threshold => ?LOG_MAX_SIZE,
        age_pruning_threshold => ?LOG_EXPIRATION
    }).


-spec report_synchronization_started(id(), file_id:file_guid()) -> ok | {error, term()}.
report_synchronization_started(Id, FileGuid) ->
    audit_log:append(Id, #audit_log_append_request{
        severity = ?INFO_AUDIT_LOG_SEVERITY,
        content = #{
            <<"status">> => <<"scheduled">>,
            <<"fileId">> => file_guid_to_object_id(FileGuid),
            <<"description">> => <<"Remote replica differs, reconciliation started.">>
        }
    }).


-spec report_file_synchronized(id(), file_id:file_guid()) -> ok | {error, term()}.
report_file_synchronized(Id, FileGuid) ->
    audit_log:append(Id, #audit_log_append_request{
        severity = ?INFO_AUDIT_LOG_SEVERITY,
        content = #{
            <<"status">> => <<"completed">>,
            <<"fileId">> => file_guid_to_object_id(FileGuid),
            <<"description">> => <<"Local replica reconciled.">>
        }
    }).


-spec report_file_synchronization_skipped(id(), file_id:file_guid(), skip_reason()) ->
    ok | {error, term()}.
report_file_synchronization_skipped(Id, FileGuid, Reason) ->
    audit_log:append(Id, #audit_log_append_request{
        severity = ?INFO_AUDIT_LOG_SEVERITY,
        content = #{
            <<"status">> => <<"skipped">>,
            <<"fileId">> => file_guid_to_object_id(FileGuid),
            <<"description">> => skip_reason_to_description(Reason)
        }
    }).


-spec report_file_synchronization_failed(id(), file_id:file_guid(), {error, term()}) -> ok | {error, term()}.
report_file_synchronization_failed(Id, FileGuid, Error) ->
    ErrorJson = errors:to_json(Error),

    audit_log:append(Id, #audit_log_append_request{
        severity = ?ERROR_AUDIT_LOG_SEVERITY,
        content = #{
            <<"status">> => <<"failed">>,
            <<"fileId">> => file_guid_to_object_id(FileGuid),
            <<"description">> => str_utils:format_bin(
                "Failed to reconcile local replica: ~s",
                [maps:get(<<"description">>, ErrorJson)]
            ),
            <<"reason">> => ErrorJson
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
