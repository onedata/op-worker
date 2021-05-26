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

-include("modules/dataset/archive.hrl").


%% API
-export([empty/0, new/3, to_json/1, sum/2,
    mark_file_archived/2, mark_file_failed/1
]).

-type stats() :: #archive_stats{}.
-export_type([stats/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec empty() -> stats().
empty() ->
    #archive_stats{}.


-spec new(non_neg_integer(), non_neg_integer(), non_neg_integer()) -> stats().
new(FilesArchived, FilesFailed, BytesArchived) ->
    #archive_stats{
        files_archived = FilesArchived,
        files_failed = FilesFailed,
        bytes_archived = BytesArchived
    }.


-spec to_json(archive_stats:stats()) -> json_utils:json_map().
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


-spec sum(stats(), stats()) -> stats().
sum(Stats1, Stats2) ->
    #archive_stats{
        files_archived = Stats1#archive_stats.files_archived + Stats2#archive_stats.files_archived,
        files_failed = Stats1#archive_stats.files_failed + Stats2#archive_stats.files_failed,
        bytes_archived = Stats1#archive_stats.bytes_archived + Stats2#archive_stats.bytes_archived
    }.


-spec mark_file_archived(stats(), non_neg_integer()) -> stats().
mark_file_archived(Stats, FileSize) ->
    Stats#archive_stats{
        files_archived = Stats#archive_stats.files_archived + 1,
        bytes_archived = Stats#archive_stats.bytes_archived + FileSize
    }.


-spec mark_file_failed(stats()) -> stats().
mark_file_failed(Stats) ->
    Stats#archive_stats{
        files_failed = Stats#archive_stats.files_failed + 1
    }.