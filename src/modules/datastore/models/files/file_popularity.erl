%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model tracking popularity of files
%%% @end
%%%-------------------------------------------------------------------
-module(file_popularity).
-author("Tomasz Lichon").

-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([increment_open/1, get/1, get_or_default/1]).

%% datastore_model callbacks
-export([get_record_struct/1]).

-type id() :: file_meta:uuid().
-type file_popularity() :: #file_popularity{}.
-type doc() :: datastore_doc:doc(file_popularity()).
-export_type([id/0]).

-define(CTX, #{model => ?MODULE}).

-define(HOUR_TIME_WINDOW, 1).
-define(DAY_TIME_WINDOW, 24).
-define(MONTH_TIME_WINDOW, 720). % 30*24

-define(HOUR_HISTOGRAM_SIZE, 24).
-define(DAY_HISTOGRAM_SIZE, 30).
-define(MONTH_HISTOGRAM_SIZE, 12). % 30*24

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Updates file's popularity with information about open.
%% @end
%%--------------------------------------------------------------------
-spec increment_open(FileCtx :: file_ctx:ctx()) -> ok | {error, term()}.
increment_open(FileCtx) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    case space_storage:is_cleanup_enabled(SpaceId) of
        true ->
            SpaceId = file_ctx:get_space_id_const(FileCtx),
            FileUuid = file_ctx:get_uuid_const(FileCtx),
            Diff = fun(FilePopularity) ->
                {ok, increase_popularity(FileCtx, FilePopularity)}
            end,
            DefaultFilePopularity = empty_file_popularity(FileCtx),
            Default = #document{
                key = FileUuid,
                value = increase_popularity(FileCtx, DefaultFilePopularity)
            },
            case datastore_model:update(?CTX, FileUuid, Diff, Default) of
                {ok, _} -> ok;
                Error -> Error
            end;
        false ->
            ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns file_popularity doc.
%% @end
%%--------------------------------------------------------------------
-spec get(file_meta:uuid()) -> {ok, doc()} | {error, term()}.
get(FileUuid) ->
    datastore_model:get(?CTX, FileUuid).

%%--------------------------------------------------------------------
%% @doc
%% Returns file_popularity doc, or default doc if its not present.
%% @end
%%--------------------------------------------------------------------
-spec get_or_default(file_ctx:ctx()) -> {ok, doc()} | {error, term()}.
get_or_default(FileCtx) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    case file_popularity:get(FileUuid) of
        {ok, Doc} ->
            {ok, Doc};
        {error, not_found} ->
            {ok, #document{
                key = FileUuid,
                value = empty_file_popularity(FileCtx)
            }};
        Error ->
            Error
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns file_popularity record with zero popularity.
%% @end
%%--------------------------------------------------------------------
-spec empty_file_popularity(file_ctx:ctx()) -> file_popularity().
empty_file_popularity(FileCtx) ->
    HourlyHistogram = time_slot_histogram:new(?HOUR_TIME_WINDOW, ?HOUR_HISTOGRAM_SIZE),
    DailyHistogram = time_slot_histogram:new(?DAY_TIME_WINDOW, ?DAY_HISTOGRAM_SIZE),
    MonthlyHistogram = time_slot_histogram:new(?MONTH_TIME_WINDOW, ?MONTH_HISTOGRAM_SIZE),
    histograms_to_file_popularity(HourlyHistogram, DailyHistogram, MonthlyHistogram, FileCtx).

%%--------------------------------------------------------------------
%% @doc
%% Returns file_popularity record with popularity increased by one open
%% @end
%%--------------------------------------------------------------------
-spec increase_popularity(file_ctx:ctx(), file_popularity()) -> file_popularity().
increase_popularity(FileCtx, FilePopularity) ->
    {HourlyHistogram, DailyHistogram, MonthlyHistogram} =
        file_popularity_to_histograms(FilePopularity),
    CurrentTimestampHours = utils:system_time_seconds() div 3600,
    histograms_to_file_popularity(
        time_slot_histogram:increment(HourlyHistogram, CurrentTimestampHours),
        time_slot_histogram:increment(DailyHistogram, CurrentTimestampHours),
        time_slot_histogram:increment(MonthlyHistogram, CurrentTimestampHours),
        FileCtx
    ).

%%--------------------------------------------------------------------
%% @doc
%% Converts given histograms into file_popularity record
%% @end
%%--------------------------------------------------------------------
-spec histograms_to_file_popularity(
    HourlyHistogram :: time_slot_histogram:histogram(),
    DailyHistogram :: time_slot_histogram:histogram(),
    MonthlyHistogram :: time_slot_histogram:histogram(),
    file_ctx:ctx()) -> file_popularity().
histograms_to_file_popularity(HourlyHistogram, DailyHistogram, MonthlyHistogram, FileCtx) ->
    {LocalSize, _FileCtx2} = file_ctx:get_local_storage_file_size(FileCtx),
    #file_popularity{
        file_uuid = file_ctx:get_uuid_const(FileCtx),
        space_id = file_ctx:get_space_id_const(FileCtx),
        size = LocalSize,
        open_count = time_slot_histogram:get_sum(MonthlyHistogram),
        last_open = time_slot_histogram:get_last_update(HourlyHistogram),
        hr_hist = time_slot_histogram:get_histogram_values(HourlyHistogram),
        dy_hist = time_slot_histogram:get_histogram_values(DailyHistogram),
        mth_hist = time_slot_histogram:get_histogram_values(MonthlyHistogram),
        hr_mov_avg = time_slot_histogram:get_average(HourlyHistogram),
        dy_mov_avg = time_slot_histogram:get_average(DailyHistogram),
        mth_mov_avg = time_slot_histogram:get_average(MonthlyHistogram)
    }.

%%--------------------------------------------------------------------
%% @doc
%% Converts file_popularity record into histograms
%% @end
%%--------------------------------------------------------------------
-spec file_popularity_to_histograms(file_popularity()) ->
    {
        HourlyHistogram :: time_slot_histogram:histogram(),
        DailyHistogram :: time_slot_histogram:histogram(),
        MonthlyHistogram :: time_slot_histogram:histogram()
    }.
file_popularity_to_histograms(#file_popularity{
    last_open = LastUpdate,
    hr_hist = HourlyHistogram,
    dy_hist = DailyHistogram,
    mth_hist = MonthlyHistogram
}) ->
    {
        time_slot_histogram:new(LastUpdate, ?HOUR_TIME_WINDOW, HourlyHistogram),
        time_slot_histogram:new(LastUpdate, ?DAY_TIME_WINDOW, DailyHistogram),
        time_slot_histogram:new(LastUpdate, ?MONTH_TIME_WINDOW, MonthlyHistogram)
    }.


%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {file_uuid, string},
        {space_id, string},
        {size, integer},
        {open_count, integer},
        {last_open, integer},
        {hr_hist, [integer]},
        {dy_hist, [integer]},
        {mth_hist, [integer]},
        {hr_mov_avg, integer},
        {dy_mov_avg, integer},
        {mth_mov_avg, integer}
    ]}.