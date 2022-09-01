%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module for archive model.
%%% It encapsulates record used for storing statistic of
%%% archivisation job.
%%% @end
%%%-------------------------------------------------------------------
-module(archive_stats).
-author("Jakub Kudzia").

-behaviour(persistent_record).

-include("modules/dataset/archive.hrl").


%% API
-export([empty/0, new/3, to_json/1, sum/2,
    mark_file_archived/2, mark_file_failed/1,
    get_archived_bytes/1, get_archived_files/1
]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).

-type record() :: #archive_stats{}.
-export_type([record/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec empty() -> record().
empty() ->
    #archive_stats{}.


-spec new(non_neg_integer(), non_neg_integer(), non_neg_integer()) -> record().
new(FilesArchived, FilesFailed, BytesArchived) ->
    #archive_stats{
        files_archived = FilesArchived,
        files_failed = FilesFailed,
        bytes_archived = BytesArchived
    }.


-spec to_json(archive_stats:record()) -> json_utils:json_map().
to_json(#archive_stats{
    files_archived = FilesArchived,
    files_failed = FilesFailed,
    bytes_archived = BytesArchived
}) ->
    #{
        <<"filesArchived">> => FilesArchived,
        <<"filesFailed">> => FilesFailed,
        <<"bytesArchived">> => BytesArchived
    }.

%% @private
-spec from_json(json_utils:json_map()) -> archive_stats:record().
from_json(#{
    <<"filesArchived">> := FilesArchived,
    <<"filesFailed">> := FilesFailed,
    <<"bytesArchived">> := BytesArchived
}) ->
    #archive_stats{
        files_archived = FilesArchived,
        files_failed = FilesFailed,
        bytes_archived = BytesArchived
    }.


-spec sum(record(), record()) -> record().
sum(Stats1, Stats2) ->
    #archive_stats{
        files_archived = Stats1#archive_stats.files_archived + Stats2#archive_stats.files_archived,
        files_failed = Stats1#archive_stats.files_failed + Stats2#archive_stats.files_failed,
        bytes_archived = Stats1#archive_stats.bytes_archived + Stats2#archive_stats.bytes_archived
    }.


-spec mark_file_archived(record(), non_neg_integer()) -> record().
mark_file_archived(Stats, FileSize) ->
    Stats#archive_stats{
        files_archived = Stats#archive_stats.files_archived + 1,
        bytes_archived = Stats#archive_stats.bytes_archived + FileSize
    }.


-spec mark_file_failed(record()) -> record().
mark_file_failed(Stats) ->
    Stats#archive_stats{
        files_failed = Stats#archive_stats.files_failed + 1
    }.


-spec get_archived_bytes(record()) -> non_neg_integer().
get_archived_bytes(#archive_stats{bytes_archived = Bytes}) ->
    Bytes.


-spec get_archived_files(record()) -> non_neg_integer().
get_archived_files(#archive_stats{files_archived = FilesArchived}) ->
    FilesArchived.

%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================

-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(ArchiveStats, _NestedRecordEncoder) ->
    to_json(ArchiveStats).


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(ArchiveStatsJson, _NestedRecordDecoder) ->
    from_json(ArchiveStatsJson).