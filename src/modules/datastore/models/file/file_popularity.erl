%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model tracking popularity of files.
%%% Note: this module operates on referenced uuids - all operations on hardlinks
%%% are treated as operations on original file. Thus, popularity of all hardlinks
%%% pointing particular file ale counted together.
%%% @end
%%%-------------------------------------------------------------------
-module(file_popularity).
-author("Tomasz Lichon").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([increment_open/1, get_or_default/1, update_size/2, update_size/1, delete/1,
    get/1]).
%% Test API
-export([update/2]).

%% datastore_model callbacks
-export([get_record_struct/1, get_ctx/0, get_record_version/0, upgrade_record/2]).

-type id() :: file_meta:uuid().
-type record() :: #file_popularity{}.
-type doc() :: datastore_doc:doc(record()).
-export_type([id/0, record/0]).

-define(CTX, #{model => ?MODULE}).

-define(HOUR_TIME_WINDOW, 1).
-define(DAY_TIME_WINDOW, 24).
-define(MONTH_TIME_WINDOW, 720). % 30*24

-define(HOUR_HISTOGRAM_SIZE, 24).
-define(DAY_HISTOGRAM_SIZE, 30).
-define(MONTH_HISTOGRAM_SIZE, 12).

%%%===================================================================
%%% API
%%%===================================================================

-spec update_size(file_ctx:ctx(), non_neg_integer()) -> ok | {error, term()}.
update_size(FileCtx, NewSize) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    case file_popularity_api:is_enabled(SpaceId) of
        true ->
            FileUuid = file_ctx:get_referenced_uuid_const(FileCtx),
            DefaultFilePopularity = empty_file_popularity(FileCtx, NewSize),
            DefaultToCreate = #document{
                key = FileUuid,
                value = DefaultFilePopularity#file_popularity{size = NewSize},
                scope = SpaceId
            },
            case
                datastore_model:update(?CTX, FileUuid, fun(FilePopularity) ->
                    {ok, FilePopularity#file_popularity{size = NewSize}}
                end, DefaultToCreate)
            of
                {ok, _} -> ok;
                Error -> Error
            end;
        false ->
            ok
    end.


-spec update_size(file_ctx:ctx()) -> ok | {error, term()}.
update_size(FileCtx) ->
    {LocalSize, FileCtx2} = file_ctx:get_local_storage_file_size(FileCtx),
    update_size(FileCtx2, LocalSize).


-spec increment_open(FileCtx :: file_ctx:ctx()) -> ok | {error, term()}.
increment_open(FileCtx) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    case file_popularity_api:is_enabled(SpaceId) of
        true ->
            FileUuid = file_ctx:get_referenced_uuid_const(FileCtx),
            {LocalSize, FileCtx2} = file_ctx:get_local_storage_file_size(FileCtx),
            Diff = fun(FilePopularity) ->
                {ok, increase_popularity(FileCtx, LocalSize, FilePopularity)}
            end,
            DefaultFilePopularity = empty_file_popularity(FileCtx2, LocalSize),
            Default = #document{
                key = FileUuid,
                value = increase_popularity(FileCtx2, LocalSize, DefaultFilePopularity),
                scope = SpaceId
            },
            case datastore_model:update(?CTX, FileUuid, Diff, Default) of
                {ok, _} -> ok;
                Error -> Error
            end;
        false ->
            ok
    end.


-spec get(file_meta:uuid()) -> {ok, doc()} | {error, term()}.
get(FileUuid) ->
    datastore_model:get(?CTX, fslogic_uuid:ensure_referenced_uuid(FileUuid)).


-spec delete(file_meta:uuid()) -> ok | {error, term()}.
delete(FileUuid) ->
    datastore_model:delete(?CTX, fslogic_uuid:ensure_referenced_uuid(FileUuid)).


-spec get_or_default(file_ctx:ctx()) -> {ok, doc()} | {error, term()}.
get_or_default(FileCtx) ->
    FileUuid = file_ctx:get_referenced_uuid_const(FileCtx),
    case file_popularity:get(FileUuid) of
        {ok, Doc} ->
            {ok, Doc};
        {error, not_found} ->
            {LocalSize, FileCtx2} = file_ctx:get_local_storage_file_size(FileCtx),
            {ok, #document{
                key = FileUuid,
                value = empty_file_popularity(FileCtx2, LocalSize)
            }};
        Error ->
            Error
    end.


%%%===================================================================
%%% Test API
%%%===================================================================

-spec update(file_meta:uuid(), datastore_model:diff()) -> {ok, record()} | {error, term()}.
update(FileUuid, Diff) ->
    datastore_model:update(?CTX, fslogic_uuid:ensure_referenced_uuid(FileUuid), Diff).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns file_popularity record with zero popularity.
%% @end
%%--------------------------------------------------------------------
-spec empty_file_popularity(file_ctx:ctx(), non_neg_integer()) -> record().
empty_file_popularity(FileCtx, LocalSize) ->
    HourlyHistogram = time_slot_histogram:new(?HOUR_TIME_WINDOW, ?HOUR_HISTOGRAM_SIZE),
    DailyHistogram = time_slot_histogram:new(?DAY_TIME_WINDOW, ?DAY_HISTOGRAM_SIZE),
    MonthlyHistogram = time_slot_histogram:new(?MONTH_TIME_WINDOW, ?MONTH_HISTOGRAM_SIZE),
    histograms_to_file_popularity(HourlyHistogram, DailyHistogram, MonthlyHistogram, LocalSize, FileCtx).

%%--------------------------------------------------------------------
%% @doc
%% Returns file_popularity record with popularity increased by one open
%% @end
%%--------------------------------------------------------------------
-spec increase_popularity(file_ctx:ctx(), non_neg_integer(), record()) -> record().
increase_popularity(FileCtx, LocalSize, FilePopularity) ->
    {HourlyHistogram, DailyHistogram, MonthlyHistogram} =
        file_popularity_to_histograms(FilePopularity),
    CurrentTimestampHours = global_clock:timestamp_hours(),
    histograms_to_file_popularity(
        increment_histogram(HourlyHistogram, CurrentTimestampHours),
        increment_histogram(DailyHistogram, CurrentTimestampHours),
        increment_histogram(MonthlyHistogram, CurrentTimestampHours),
        LocalSize,
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
    LocalSize :: non_neg_integer(),
    file_ctx:ctx()) -> record().
histograms_to_file_popularity(HourlyHistogram, DailyHistogram, MonthlyHistogram, LocalSize, FileCtx) ->
    #file_popularity{
        file_uuid = file_ctx:get_referenced_uuid_const(FileCtx),
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
-spec file_popularity_to_histograms(record()) ->
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
%%% Internal functions
%%%===================================================================

%% @private
-spec increment_histogram(time_slot_histogram:histogram(), time:hours()) ->
    time_slot_histogram:histogram().
increment_histogram(Histogram, CurrentTimeHours) ->
    MonotonicTime = time_slot_histogram:ensure_monotonic_timestamp(Histogram, CurrentTimeHours),
    time_slot_histogram:increment(Histogram, MonotonicTime).

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
    ]};
get_record_struct(2) ->
    {record, [
        {file_uuid, string},
        {space_id, string},
        {size, integer},
        {open_count, integer},
        {last_open, integer},
        {hr_hist, [integer]},
        {dy_hist, [integer]},
        {mth_hist, [integer]},
        {hr_mov_avg, float},
        {dy_mov_avg, float},
        {mth_mov_avg, float}
    ]}.

%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    2.

%%--------------------------------------------------------------------
%% @doc
%% Upgrades model's record from provided version to the next one.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, FilePopularity) ->
    {2, FilePopularity}.