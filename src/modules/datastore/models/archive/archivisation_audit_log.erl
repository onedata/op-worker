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
    report_file_archivisation_finished/4,
    report_file_archivisation_failed/5,
    report_file_verification_failed/3
]).

-export([
    destroy/1,
    browse/2
]).

-type id() :: archive:id().

-export_type([id/0]).

-define(LOG_OPTS, #{
    size_pruning_threshold => op_worker:get_env(archivisation_audit_log_max_size, 10000)
    %% defaults are used for age pruning and expiration; @see audit_log.erl
}).


%%%===================================================================
%%% API
%%%===================================================================

-spec report_file_archivisation_finished(id(), file_meta:path(),
    onedata_file:type(), time:millis()) -> ok | {error, term()}.
report_file_archivisation_finished(Id, FilePath, FileType, StartTimestamp) ->
    DescriptionPrefix = string:titlecase(type_to_human_readable_str(FileType)),

    audit_log:append(Id, ?LOG_OPTS, #audit_log_append_request{
        severity = ?INFO_AUDIT_LOG_SEVERITY,
        content = #{
            <<"description">> => <<DescriptionPrefix/binary, " archivisation finished.">>,
            <<"startTimestamp">> => StartTimestamp,
            <<"path">> => FilePath,
            <<"fileType">> => type_to_binary(FileType)
        }
    }).


-spec report_file_archivisation_failed(id(), file_meta:path(), onedata_file:type(),
    time:millis(), {error, term()}) -> ok | {error, term()}.
report_file_archivisation_failed(Id, FilePath, FileType, StartTimestamp, Error) ->
    ErrorJson = errors:to_json(Error),
    DescriptionPrefix = string:titlecase(type_to_human_readable_str(FileType)),

    audit_log:append(Id, ?LOG_OPTS, #audit_log_append_request{
        severity = ?ERROR_AUDIT_LOG_SEVERITY,
        content = #{
            <<"description">> => <<DescriptionPrefix/binary, " archivisation failed.">>,
            <<"startTimestamp">> => StartTimestamp,
            <<"reason">> => ErrorJson,
            <<"path">> => FilePath,
            <<"fileType">> => type_to_binary(FileType)
        }
    }).


-spec report_file_verification_failed(id(), file_meta:path(), onedata_file:type()) ->
    ok | {error, term()}.
report_file_verification_failed(Id, FilePath, FileType) ->
    Description = <<"Verification of the archived ", (type_to_human_readable_str(FileType))/binary, " failed.">>,

    audit_log:append(Id, ?LOG_OPTS, #audit_log_append_request{
        severity = ?ERROR_AUDIT_LOG_SEVERITY,
        content = #{
            <<"description">> => Description,
            <<"path">> => FilePath,
            <<"fileType">> => type_to_binary(FileType)
        }
    }).


-spec destroy(id()) -> ok | {error, term()}.
destroy(Id) ->
    audit_log:delete(Id).


-spec browse(id(), audit_log_browse_opts:opts()) ->
    {ok, audit_log:browse_result()} | errors:error().
browse(Id, Opts) ->
    audit_log:browse(Id, Opts).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec type_to_human_readable_str(onedata_file:type()) -> binary().
type_to_human_readable_str(?REGULAR_FILE_TYPE) -> <<"regular file">>;
type_to_human_readable_str(?DIRECTORY_TYPE) -> <<"directory">>;
type_to_human_readable_str(?SYMLINK_TYPE) -> <<"symbolic link">>.


%% @private
-spec type_to_binary(onedata_file:type()) -> binary().
type_to_binary(?REGULAR_FILE_TYPE) -> <<"REG">>;
type_to_binary(?DIRECTORY_TYPE) -> <<"DIR">>;
type_to_binary(?SYMLINK_TYPE) -> <<"SYMLNK">>.
