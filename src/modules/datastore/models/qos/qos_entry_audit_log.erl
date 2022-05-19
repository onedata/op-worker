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

-export_type([id/0]).

-define(LOG_MAX_SIZE, op_worker:get_env(qos_entry_audit_log_max_size, 10000)).
-define(LOG_EXPIRATION, op_worker:get_env(qos_entry_audit_log_expiration_seconds, 1209600)). % 14 days

%%%===================================================================
%%% API
%%%===================================================================

-spec create(id()) -> ok | {error, term()}.
create(Id) ->
    json_infinite_log_model:create(Id, #{
        size_pruning_threshold => ?LOG_MAX_SIZE,
        age_pruning_threshold => ?LOG_EXPIRATION
    }).


-spec report_synchronization_started(id(), file_id:file_guid()) -> ok | {error, term()}.
report_synchronization_started(Id, FileGuid) ->
    json_infinite_log_model:append(Id, #{
        <<"status">> => <<"synchronization started">>,
        <<"severity">> => <<"info">>,
        <<"fileId">> => file_guid_to_object_id(FileGuid)
    }).


-spec report_file_synchronized(id(), file_id:file_guid()) -> ok | {error, term()}.
report_file_synchronized(Id, FileGuid) ->
    json_infinite_log_model:append(Id, #{
        <<"status">> => <<"synchronized">>,
        <<"severity">> => <<"info">>,
        <<"fileId">> => file_guid_to_object_id(FileGuid)
    }).


-spec report_file_synchronization_skipped(id(), file_id:file_guid(), Reason :: binary()) ->
    ok | {error, term()}.
report_file_synchronization_skipped(Id, FileGuid, Reason) ->
    json_infinite_log_model:append(Id, #{
        <<"status">> => <<"synchronization skipped">>,
        <<"reason">> => Reason,
        <<"severity">> => <<"info">>,
        <<"fileId">> => file_guid_to_object_id(FileGuid)
    }).


-spec report_file_synchronization_failed(id(), file_id:file_guid(), {error, term()}) -> ok | {error, term()}.
report_file_synchronization_failed(Id, FileGuid, Error) ->
    json_infinite_log_model:append(Id, #{
        <<"status">> => <<"synchronization failed">>,
        <<"severity">> => <<"error">>,
        <<"fileId">> => file_guid_to_object_id(FileGuid),
        <<"reason">> => errors:to_json(Error)
    }).


-spec destroy(id()) -> ok | {error, term()}.
destroy(Id) ->
    json_infinite_log_model:destroy(Id).


-spec browse_content(id(), json_infinite_log_model:listing_opts()) ->
    {ok, json_infinite_log_model:browse_result()} | {error, term()}.
browse_content(Id, Opts) ->
    json_infinite_log_model:browse_content(Id, Opts#{direction => ?FORWARD}).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec file_guid_to_object_id(file_id:file_guid()) -> file_id:objectid().
file_guid_to_object_id(FileGuid) ->
    {ok, ObjectId} = file_id:guid_to_objectid(FileGuid),
    ObjectId.
