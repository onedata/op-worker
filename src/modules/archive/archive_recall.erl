%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% API module for performing operations on archive recalls.
%%% Archive recall is a copy of an archive content. If an archive contained nested archive its 
%%% content is also copied in a place of a symbolic link. 
%%% Archive recall uses synced (`archive_recall_details`) and local (`archive_recall_progress`) 
%%% models. Copying is performed using traverse (`archive_recall_traverse`).
%%% @end
%%%-------------------------------------------------------------------
-module(archive_recall).
-author("Michal Stanisz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("cluster_worker/include/modules/datastore/infinite_log.hrl").
-include_lib("ctool/include/time_series/common.hrl").


%% API
-export([create_docs/2, delete_synced_docs/1, delete_local_docs/1]).
-export([report_started/1, report_finished/2,
    report_bytes_copied/2, report_file_finished/1, report_file_failed/4]).
-export([get_details/1, get_stats/3, get_progress/1, browse_log/2]).
-export([get_effective_recall/1]).

-type id() :: file_meta:uuid().
-type record() :: archive_recall_details:record().
-type recall_progress_map() :: archive_recall_progress:recall_progress_map().
-export_type([id/0, record/0, recall_progress_map/0]).


%%%===================================================================
%%% API functions
%%%===================================================================

-spec create_docs(id(), archive:doc()) -> ok | {error, term()}.
create_docs(Id, ArchiveDoc) ->
    try
        ok = archive_recall_details:create(Id, ArchiveDoc),
        ok = archive_recall_progress:create(Id),
        {ok, SpaceId} = archive:get_space_id(ArchiveDoc),
        ok = archive_recall_cache:invalidate_on_all_nodes(SpaceId)
    catch _:{badmatch, Error} ->
        delete_synced_docs(Id),
        delete_local_docs(Id),
        Error
    end.


-spec delete_synced_docs(id()) -> ok | {error, term()}.
delete_synced_docs(Id) ->
    % no need to invalidate archive_recall_cache, as whole subtree has been already deleted
    archive_recall_details:delete(Id).


-spec delete_local_docs(id()) -> ok | {error, term()}.
delete_local_docs(Id) ->
    % no need to invalidate archive_recall_cache, as whole subtree has been already deleted
    archive_recall_progress:delete(Id).


-spec report_started(id()) -> ok | {error, term()}.
report_started(Id) ->
    archive_recall_details:report_started(Id).


-spec report_bytes_copied(id(), non_neg_integer()) -> ok | {error, term()}.
report_bytes_copied(Id, Bytes) ->
    archive_recall_progress:report_bytes_copied(Id, Bytes).


-spec report_finished(id(), od_space:id()) -> ok | {error, term()}.
report_finished(Id, SpaceId) ->
    case archive_recall_details:report_finished(Id) of
        ok ->
            archive_recall_cache:invalidate_on_all_nodes(SpaceId);
        {error, _} = Error ->
            Error
    end.


-spec report_file_finished(id()) -> ok | {error, term()}.
report_file_finished(Id) ->
    archive_recall_progress:report_file_finished(Id).


-spec report_file_failed(id(), file_id:file_guid(), file_meta:path(), {error, term()}) -> 
    ok | {error, term()}.
report_file_failed(Id, FileGuid, RelativePath, Error) ->
    {ok, ObjectId} = file_id:guid_to_objectid(FileGuid),
    ErrorJson = #{
        <<"fileId">> => ObjectId,
        <<"relativePath">> => RelativePath,
        <<"reason">> => errors:to_json(Error)
    },
    % save error in synchronized model (so remote providers have knowledge about failure).
    archive_recall_details:report_error(Id, ErrorJson),
    % save error in local infinite log.
    archive_recall_progress:report_error(Id, ErrorJson).


-spec get_details(id()) -> {ok, record()} | {error, term()}.
get_details(Id) ->
    archive_recall_details:get(Id).


-spec browse_log(id(), audit_log_browse_opts:opts()) ->
    {ok, audit_log:browse_result()} | {error, term()}.
browse_log(Id, Options) ->
    archive_recall_progress:browse_error_log(Id, Options).


-spec get_stats(id(), time_series_collection:layout(), ts_metric:list_options()) ->
    {ok, time_series_collection:slice()} | {error, term()}.
get_stats(Id, SliceLayout, ListWindowsOptions) ->
    archive_recall_progress:get_stats(Id, SliceLayout, ListWindowsOptions).


-spec get_progress(id()) -> {ok, recall_progress_map()}.
get_progress(Id) ->
    archive_recall_progress:get(Id).


-spec get_effective_recall(file_meta:doc()) -> {ok, id() | undefined} | {error, term()}.
get_effective_recall(#document{scope = SpaceId, key = Id} = FileMetaDoc) ->
    case archive_recall_cache:get(SpaceId, FileMetaDoc) of
        {ok, {finished, Id}} -> {ok, Id};
        {ok, {finished, _}} -> {ok, undefined};
        {ok, {ongoing, AncestorId}} -> {ok, AncestorId};
        Other -> Other
    end.
