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
-behaviour(model_behaviour).

-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_model.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([increment_open/1, get_or_default/1, initialize/1, update_size/2]).

%% model_behaviour callbacks
-export([save/1, get/1, exists/1, delete/1, update/2, create/1, create_or_update/2,
    model_init/0, 'after'/5, before/4]).
-export([record_struct/1]).

-define(HOUR_TIME_WINDOW, 1).
-define(DAY_TIME_WINDOW, 24).
-define(MONTH_TIME_WINDOW, 720). % 30*24

-define(HOUR_HISTOGRAM_SIZE, 24).
-define(DAY_HISTOGRAM_SIZE, 30).
-define(MONTH_HISTOGRAM_SIZE, 12).

-type id() :: file_meta:uuid().
-type file_popularity() :: #file_popularity{}.
-type doc() :: #document{value :: file_popularity()}.

-export_type([id/0]).

%%--------------------------------------------------------------------
%% @doc
%% Returns structure of the record in specified version.
%% @end
%%--------------------------------------------------------------------
-spec record_struct(datastore_json:record_version()) -> datastore_json:record_struct().
record_struct(1) ->
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

%%%===================================================================
%%% API
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% Creates file popularity view if it is enabled.
%% @end
%%-------------------------------------------------------------------
-spec initialize(od_space:id()) -> ok | {error, term()}.
initialize(SpaceId) ->
    case space_storage:is_file_popularity_enabled(SpaceId) of
        true ->
            file_popularity_view:create(SpaceId);
        false ->
            ok
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Updated file's size
%% @end
%%-------------------------------------------------------------------
update_size(FileCtx, NewSize) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    case space_storage:is_file_popularity_enabled(SpaceId) of
        true ->
            FileUuid = file_ctx:get_uuid_const(FileCtx),
            DefaultFilePopularity = empty_file_popularity(FileCtx),
            ToCreate = #document{
                key = FileUuid,
                value = DefaultFilePopularity#file_popularity{size=NewSize}
            },
            case
                create_or_update(ToCreate, fun(FilePopularity) ->
                    {ok, FilePopularity#file_popularity{size=NewSize}}
                end)
            of
                {ok, _} ->
                    ok;
                Error -> Error
            end;
        false ->
            ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% Updates file's popularity with information about open.
%% @end
%%--------------------------------------------------------------------
-spec increment_open(FileCtx :: file_ctx:ctx()) -> ok | datastore:generic_error().
increment_open(FileCtx) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    case space_storage:is_file_popularity_enabled(SpaceId) of
        true ->
            FileUuid = file_ctx:get_uuid_const(FileCtx),
            DefaultFilePopularity = empty_file_popularity(FileCtx),
            ToCreate = #document{
                key = FileUuid,
                value = increase_popularity(FileCtx, DefaultFilePopularity)
            },
            case
                create_or_update(ToCreate, fun(FilePopularity) ->
                    {ok, increase_popularity(FileCtx, FilePopularity)}
                end)
            of
                {ok, _} ->
                    ok;
                Error -> Error
            end;
        false ->
             ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns file_popularity doc, or default doc if its not present.
%% @end
%%--------------------------------------------------------------------
-spec get_or_default(file_ctx:ctx()) ->
    {ok, doc()} | datastore:get_error().
get_or_default(FileCtx) ->
    FileUuid= file_ctx:get_uuid_const(FileCtx),
    case get(FileUuid) of
        {ok, Doc} ->
            {ok, Doc};
        {error, {not_found, _}} ->
            {ok, #document{
                key = FileUuid,
                value = empty_file_popularity(FileCtx)
            }};
        Error ->
            Error
    end.

%%%===================================================================
%%% model_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback save/1.
%% @end
%%--------------------------------------------------------------------
-spec save(datastore:document()) -> {ok, datastore:key()} | datastore:generic_error().
save(Document) ->
    model:execute_with_default_context(?MODULE, save, [Document]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:key()} | datastore:update_error().
update(Key, Diff) ->
    model:execute_with_default_context(?MODULE, update, [Key, Diff]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) -> {ok, datastore:key()} | datastore:create_error().
create(Document) ->
    model:execute_with_default_context(?MODULE, create, [Document]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:key()) -> {ok, datastore:document()} | datastore:get_error().
get(Key) ->
    model:execute_with_default_context(?MODULE, get, [Key]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:key()) -> ok | datastore:generic_error().
delete(Key) ->
    model:execute_with_default_context(?MODULE, delete, [Key]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1.
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:key()) -> datastore:exists_return().
exists(Key) ->
    ?RESPONSE(model:execute_with_default_context(?MODULE, exists, [Key])).

%%--------------------------------------------------------------------
%% @doc
%% Updates document with using ID from document. If such object does not exist,
%% it initialises the object with the document.
%% @end
%%--------------------------------------------------------------------
-spec create_or_update(datastore:document(), Diff :: datastore:document_diff()) ->
    {ok, datastore:key()} | datastore:generic_error().
create_or_update(Doc, Diff) ->
    model:execute_with_default_context(?MODULE, create_or_update, [Doc, Diff]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0.
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    Config = ?MODEL_CONFIG(file_popularity_bucket, [], ?GLOBALLY_CACHED_LEVEL),
    Config#model_config{version = 1}.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback 'after'/5.
%% @end
%%--------------------------------------------------------------------
-spec 'after'(ModelName :: model_behaviour:model_type(), Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term(),
    ReturnValue :: term()) -> ok.
'after'(_ModelName, _Method, _Level, _Context, _ReturnValue) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback before/4.
%% @end
%%--------------------------------------------------------------------
-spec before(ModelName :: model_behaviour:model_type(), Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term()) -> ok | datastore:generic_error().
before(_ModelName, _Method, _Level, _Context) ->
    ok.

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
    CurrentTimestampHours = time_utils:cluster_time_seconds() div 3600,
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