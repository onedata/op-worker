%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This model stores file-popularity configuration.
%%% @end
%%%-------------------------------------------------------------------
-module(file_popularity_config).
-author("Jakub Kudzia").

-include("modules/datastore/datastore_runner.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/file_popularity_config.hrl").
-include_lib("ctool/include/logging.hrl").

-type id() :: od_space:id().
-type record() :: #file_popularity_config{}.
-type diff() :: datastore_doc:diff(record()).
-type doc() :: datastore_doc:doc(record()).
-type error() :: {error, term()}.

-export_type([id/0]).

%% API
-export([is_enabled/1, delete/1, get/1,
    get_last_open_hour_weight/1, get_avg_open_count_per_day_weight/1,
    maybe_create_or_update/2, get_max_avg_open_count_per_day/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0, upgrade_record/2]).

-define(CTX, #{
    model => ?MODULE,
    generated_key => false
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec get(id()) -> {ok, doc()} | error().
get(SpaceId) ->
    datastore_model:get(?CTX, SpaceId).

-spec is_enabled(doc() | record() | id()) -> boolean().
is_enabled(#document{value = FPC}) ->
    is_enabled(FPC);
is_enabled(#file_popularity_config{enabled = Enabled}) ->
    Enabled;
is_enabled(SpaceId) ->
    case file_popularity_config:get(SpaceId) of
        {ok, #document{value = FPC}} ->
            FPC#file_popularity_config.enabled;
        _ ->
            false
    end.

-spec delete(id()) -> ok.
delete(SpaceId) ->
    datastore_model:delete(?CTX, SpaceId).

-spec get_last_open_hour_weight(record() | doc()) -> number().
get_last_open_hour_weight(FPC = #file_popularity_config{}) ->
    FPC#file_popularity_config.last_open_hour_weight;
get_last_open_hour_weight(#document{value = FPC}) ->
    get_last_open_hour_weight(FPC).

-spec get_avg_open_count_per_day_weight(record() | doc()) -> number().
get_avg_open_count_per_day_weight(FPC = #file_popularity_config{}) ->
    FPC#file_popularity_config.avg_open_count_per_day_weight;
get_avg_open_count_per_day_weight(#document{value = FPC}) ->
    get_avg_open_count_per_day_weight(FPC).

-spec get_max_avg_open_count_per_day(record() | doc()) -> number().
get_max_avg_open_count_per_day(FPC = #file_popularity_config{}) ->
    FPC#file_popularity_config.max_avg_open_count_per_day;
get_max_avg_open_count_per_day(#document{value = FPC}) ->
    get_max_avg_open_count_per_day(FPC).

-spec maybe_create_or_update(od_space:id(), maps:map()) -> {ok, doc()} | error().
maybe_create_or_update(SpaceId, NewConfiguration) ->
    Diff = fun(FP) ->
        case apply_configuration(FP, NewConfiguration) of
            FP -> {error, not_changed};
            NewFP -> {ok, NewFP}
        end
    end,
    InitialRecord = apply_configuration(default_record(), NewConfiguration),
    InitialDoc = default_doc(SpaceId, InitialRecord),
    create_or_update(SpaceId, Diff, InitialDoc).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec default_record() -> record().
default_record() ->
    #file_popularity_config{
        last_open_hour_weight = ?DEFAULT_LAST_OPEN_HOUR_WEIGHT,
        avg_open_count_per_day_weight = ?DEFAULT_AVG_OPEN_COUNT_PER_DAY_WEIGHT,
        max_avg_open_count_per_day = ?DEFAULT_MAX_AVG_OPEN_COUNT_PER_DAY
    }.

-spec default_doc(od_space:id(), record()) -> doc().
default_doc(SpaceId, FPC) ->
    #document{
        key = SpaceId,
        value = FPC,
        scope = SpaceId
    }.

-spec create_or_update(id(), diff(), doc()) -> {ok, doc()} | error().
create_or_update(SpaceId, UpdateFun, DefaultDoc) ->
    datastore_model:update(?CTX, SpaceId, UpdateFun, DefaultDoc).

-spec apply_configuration(record(), maps:map()) -> record().
apply_configuration(FP = #file_popularity_config{
    enabled = Enabled,
    last_open_hour_weight = LastOpenHourWeight,
    avg_open_count_per_day_weight = AvgOpenCountPerDayWeight,
    max_avg_open_count_per_day = MaxAvgOpenCountPerDay
}, Configuration) ->
    FP#file_popularity_config{
        enabled =
            maps:get(enabled, Configuration, Enabled),
        last_open_hour_weight =
            maps:get(last_open_hour_weight, Configuration, LastOpenHourWeight),
        avg_open_count_per_day_weight =
            maps:get(avg_open_count_per_day_weight, Configuration, AvgOpenCountPerDayWeight),
        max_avg_open_count_per_day =
            maps:get(max_avg_open_count_per_day, Configuration, MaxAvgOpenCountPerDay)

    }.

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
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
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [{enabled, boolean}]};
get_record_struct(2) ->
    {record, [
        {enabled, boolean},
        {last_open_hour_weight, float},
        {avg_open_count_per_day_weight, float},
        {max_avg_open_count_per_day, float}
    ]}.

%%--------------------------------------------------------------------
%% @doc
%% Upgrades model's record from provided version to the next one.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, {?MODULE, Enabled}) ->
    {2, {?MODULE, Enabled, ?DEFAULT_LAST_OPEN_HOUR_WEIGHT,
        ?DEFAULT_AVG_OPEN_COUNT_PER_DAY_WEIGHT, ?DEFAULT_MAX_AVG_OPEN_COUNT_PER_DAY
    }}.