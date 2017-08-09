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
-export([increment_open/1, get_or_default/2]).

%% model_behaviour callbacks
-export([save/1, get/1, exists/1, delete/1, update/2, create/1, create_or_update/2,
    model_init/0, 'after'/5, before/4]).
-export([record_struct/1]).

-define(HOUR_TIME_WINDOW, 1).
-define(DAY_TIME_WINDOW, 24).
-define(MONTH_TIME_WINDOW, 720). % 30*24

-define(HOUR_HISTOGRAM_SIZE, 24).
-define(DAY_HISTOGRAM_SIZE, 30).
-define(MONTH_HISTOGRAM_SIZE, 12). % 30*24

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
        {total_open_count, integer},
        {last_open, integer},
        {hourly_histogram, [integer]},
        {daily_histogram, [integer]},
        {monthly_histogram, [integer]},
        {hourly_moving_average, integer},
        {daily_moving_average, integer},
        {monthly_moving_average, integer}
    ]}.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Updates file's popularity with information about open.
%% @end
%%--------------------------------------------------------------------
-spec increment_open(FileCtx :: file_ctx:ctx()) -> ok | datastore:generic_error().
increment_open(FileCtx) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    case space_storage:is_cleanup_enabled(SpaceId) of
        true ->
            SpaceId = file_ctx:get_space_id_const(FileCtx),
            FileUuid = file_ctx:get_uuid_const(FileCtx),
            DefaultFilePopularity = empty_file_popularity(FileUuid, SpaceId),
            ToCreate = #document{
                key = FileUuid,
                value = increase_popularity(DefaultFilePopularity)
            },
            case
                create_or_update(ToCreate, fun(FilePopularity) ->
                    {ok, increase_popularity(FilePopularity)}
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
-spec get_or_default(file_meta:uuid(), od_space:id()) ->
    {ok, doc()} | datastore:get_error().
get_or_default(FileUuid, SpaceId) ->
    case get(FileUuid) of
        {ok, Doc} ->
            {ok, Doc};
        {error, {not_found, _}} ->
            {ok, #document{
                key = FileUuid,
                value = empty_file_popularity(FileUuid, SpaceId)
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
-spec empty_file_popularity(file_meta:uuid(), od_space:id()) -> file_popularity().
empty_file_popularity(FileUuid, SpaceId) ->
    HourlyHistogram = time_slot_histogram:new(?HOUR_TIME_WINDOW, ?HOUR_HISTOGRAM_SIZE),
    DailyHistogram = time_slot_histogram:new(?DAY_TIME_WINDOW, ?DAY_HISTOGRAM_SIZE),
    MonthlyHistogram = time_slot_histogram:new(?MONTH_TIME_WINDOW, ?MONTH_HISTOGRAM_SIZE),
    histograms_to_file_popularity(HourlyHistogram, DailyHistogram, MonthlyHistogram, FileUuid, SpaceId).

%%--------------------------------------------------------------------
%% @doc
%% Returns file_popularity record with popularity increased by one open
%% @end
%%--------------------------------------------------------------------
-spec increase_popularity(file_popularity()) -> file_popularity().
increase_popularity(FilePopularity = #file_popularity{
    file_uuid = FileUuid,
    space_id = SpaceId
}) ->
    {HourlyHistogram, DailyHistogram, MonthlyHistogram} =
        file_popularity_to_histograms(FilePopularity),
    CurrentTimestampHours = utils:system_time_seconds() div 3600,
    histograms_to_file_popularity(
        time_slot_histogram:increment(HourlyHistogram, CurrentTimestampHours),
        time_slot_histogram:increment(DailyHistogram, CurrentTimestampHours),
        time_slot_histogram:increment(MonthlyHistogram, CurrentTimestampHours),
        FileUuid,
        SpaceId
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
    file_meta:uuid(), od_space:id()) -> file_popularity().
histograms_to_file_popularity(HourlyHistogram, DailyHistogram, MonthlyHistogram, FileUuid, SpaceId) ->
    #file_popularity{
        file_uuid = FileUuid,
        space_id = SpaceId,
        total_open_count = time_slot_histogram:get_sum(MonthlyHistogram),
        last_open = time_slot_histogram:get_last_update(HourlyHistogram),
        hourly_histogram = time_slot_histogram:get_histogram_values(HourlyHistogram),
        daily_histogram = time_slot_histogram:get_histogram_values(DailyHistogram),
        monthly_histogram = time_slot_histogram:get_histogram_values(MonthlyHistogram),
        hourly_moving_average = time_slot_histogram:get_average(HourlyHistogram),
        daily_moving_average = time_slot_histogram:get_average(DailyHistogram),
        monthly_moving_average = time_slot_histogram:get_average(MonthlyHistogram)
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
    hourly_histogram = HourlyHistogram,
    daily_histogram = DailyHistogram,
    monthly_histogram = MonthlyHistogram
}) ->
    {
        time_slot_histogram:new(LastUpdate, ?HOUR_TIME_WINDOW, HourlyHistogram),
        time_slot_histogram:new(LastUpdate, ?DAY_TIME_WINDOW, DailyHistogram),
        time_slot_histogram:new(LastUpdate, ?MONTH_TIME_WINDOW, MonthlyHistogram)
    }.