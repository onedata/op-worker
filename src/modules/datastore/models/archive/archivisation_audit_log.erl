%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc 
%%% This module is responsible for storing audit log of archivisation process. 
%%% @end
%%%-------------------------------------------------------------------
-module(archivisation_audit_log).
-author("Michal Stanisz").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("cluster_worker/include/audit_log.hrl").
-include_lib("cluster_worker/include/modules/datastore/infinite_log.hrl").

%% API
-export([
    create/1, destroy/1, browse/2
]).

-export([
    report_file_archivisation_finished/5,
    report_file_archivisation_failed/6,
    report_file_verification_failed/4
]).

-type id() :: archive:id().

-export_type([id/0]).

-define(LOG_MAX_SIZE, op_worker:get_env(archivisation_audit_log_max_size, 10000)).
-define(LOG_EXPIRATION, op_worker:get_env(archivisation_audit_log_expiration_seconds, 1209600)). % 14 days

%%%===================================================================
%%% API
%%%===================================================================

-spec create(id()) -> ok | {error, term()}.
create(Id) ->
    audit_log:create(Id, #{
        size_pruning_threshold => ?LOG_MAX_SIZE,
        age_pruning_threshold => ?LOG_EXPIRATION
    }).


-spec destroy(id()) -> ok | {error, term()}.
destroy(Id) ->
    audit_log:delete(Id).


-spec browse(id(), audit_log_browse_opts:opts()) ->
    {ok, audit_log:browse_result()} | errors:error().
browse(Id, Opts) ->
    audit_log:browse(Id, Opts).


-spec report_file_archivisation_finished(id(), file_id:file_guid(), file_meta:path(), 
    file_meta:type(), time:millis()) -> ok | {error, term()}.
report_file_archivisation_finished(Id, FileGuid, FilePath, FileType, StartTimestamp) ->
    DescriptionPrefix = string:titlecase(type_to_human_readable_str(FileType)),
    
    audit_log:append(Id, #audit_log_append_request{
        severity = ?INFO_AUDIT_LOG_SEVERITY,
        content = #{
            <<"description">> => <<DescriptionPrefix/binary, " archivisation finished.">>,
            <<"fileId">> => file_guid_to_object_id(FileGuid),
            <<"startTimestamp">> => StartTimestamp,
            <<"path">> => FilePath,
            <<"fileType">> => type_to_binary(FileType)
        }
    }).


-spec report_file_archivisation_failed(id(), file_id:file_guid(), file_meta:path(), file_meta:type(), 
    time:millis(), {error, term()}) -> ok | {error, term()}.
report_file_archivisation_failed(Id, FileGuid, FilePath, FileType, StartTimestamp, Error) ->
    ErrorJson = errors:to_json(Error),
    DescriptionPrefix = string:titlecase(type_to_human_readable_str(FileType)),
    
    audit_log:append(Id, #audit_log_append_request{
        severity = ?ERROR_AUDIT_LOG_SEVERITY,
        content = #{
            <<"description">> => <<DescriptionPrefix/binary, " archivisation failed.">>,
            <<"fileId">> => file_guid_to_object_id(FileGuid),
            <<"startTimestamp">> => StartTimestamp,
            <<"reason">> => ErrorJson,
            <<"path">> => FilePath,
            <<"fileType">> => type_to_binary(FileType)
        }
    }).


-spec report_file_verification_failed(id(), file_id:file_guid(), file_meta:path(), file_meta:type()) ->
    ok | {error, term()}.
report_file_verification_failed(Id, FileGuid, FilePath, FileType) ->
    Description = <<"Verification of the archived ", (type_to_human_readable_str(FileType))/binary, " failed.">>,
    
    audit_log:append(Id, #audit_log_append_request{
        severity = ?ERROR_AUDIT_LOG_SEVERITY,
        content = #{
            <<"description">> => Description,
            <<"fileId">> => file_guid_to_object_id(FileGuid),
            <<"path">> => FilePath,
            <<"fileType">> => type_to_binary(FileType)
        }
    }).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec file_guid_to_object_id(file_id:file_guid()) -> file_id:objectid().
file_guid_to_object_id(FileGuid) ->
    {ok, ObjectId} = file_id:guid_to_objectid(FileGuid),
    ObjectId.


%% @private
-spec type_to_human_readable_str(file_meta:type()) -> binary().
type_to_human_readable_str(?REGULAR_FILE_TYPE) -> <<"regular file">>;
type_to_human_readable_str(?DIRECTORY_TYPE) -> <<"directory">>;
type_to_human_readable_str(?SYMLINK_TYPE) -> <<"symbolic link">>.


%% @private
-spec type_to_binary(file_meta:type()) -> binary().
type_to_binary(?REGULAR_FILE_TYPE) -> <<"REG">>;
type_to_binary(?DIRECTORY_TYPE) -> <<"DIR">>;
type_to_binary(?SYMLINK_TYPE) -> <<"SYMLNK">>.
