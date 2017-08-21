%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Creating and querying popularity view
%%% @end
%%%--------------------------------------------------------------------
-module(file_popularity_view).
-author("Tomasz Lichon").

-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/logging.hrl").

-define(VIEW_NAME(SpaceId), <<"file-popularity-", SpaceId/binary>>).

%% API
-export([create/1, get_unpopular_files/7]).

-define(INFINITY, 100000000000000000). % 100PB

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates view on space files capable of ordering files by their popularity
%% @end
%%--------------------------------------------------------------------
-spec create(od_space:id()) -> ok | {error, term()}.
create(SpaceId) ->
    ViewFunction =
        <<"function (doc, meta) {"
        "   if(doc['_record'] == 'file_popularity' && doc['space_id'] == '", SpaceId/binary , "') { "
        "      emit("
        "         ["
        "             doc['size'],",
        "             doc['last_open'],",
        "             doc['open_count'],",
        "             doc['hr_mov_avg'],",
        "             doc['dy_mov_avg'],",
        "             doc['mth_mov_avg']",
        "         ],"
        "         null"
        "      );"
        "   }"
        "}">>,
    Ctx = model:make_disk_ctx(file_popularity:model_init()),
    couchbase_driver:save_spatial_view_doc(Ctx, ?VIEW_NAME(SpaceId), ViewFunction).

%%--------------------------------------------------------------------
%% @doc
%% Finds unpopular files in space
%% @end
%%--------------------------------------------------------------------
-spec get_unpopular_files(od_space:id(), SizeLowerLimit :: null | non_neg_integer(),
    HoursSinceLastOpen :: null | non_neg_integer(), TotalOpenLimit :: null | non_neg_integer(),
    HourAverageLimit :: null | non_neg_integer(), DayAverageLimit :: null | non_neg_integer(),
    MonthAverageLimit :: null | non_neg_integer()) -> [file_ctx:ctx()].
get_unpopular_files(SpaceId, SizeLowerLimit, HoursSinceLastOpenLimit,
    TotalOpenLimit, HourAverageLimit, DayAverageLimit, MonthAverageLimit
) ->
    Ctx = model:make_disk_ctx(file_popularity:model_init()),
    CurrentTimeInHours = utils:system_time_seconds() div 3600,
    HoursTimestampLimit = case HoursSinceLastOpenLimit of
        null ->
            null;
        _ ->
            CurrentTimeInHours - HoursSinceLastOpenLimit
    end,
    Options = [
        {spatial, true},
        {stale, false},
        {start_range, [SizeLowerLimit, 0, 0, 0, 0, 0]},
        {end_range, [
            ?INFINITY,
            HoursTimestampLimit,
            TotalOpenLimit,
            HourAverageLimit,
            DayAverageLimit,
            MonthAverageLimit
        ]}
    ],
    {ok, {Rows}} = query([Ctx, ?VIEW_NAME(SpaceId), ?VIEW_NAME(SpaceId), Options]),
    lists:map(fun(Row) ->
        {<<"id">>, <<"file_popularity-", FileUuid/binary>>} =
            lists:keyfind(<<"id">>, 1, Row),
        file_ctx:new_by_guid(fslogic_uuid:uuid_to_guid(FileUuid, SpaceId))
    end, Rows).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc This function silences dialyzer "no local return" errors.
%% @equiv apply(fun couchbase_driver:query_view/4, Args).
%% @end
%%--------------------------------------------------------------------
-spec query(list()) -> {ok, datastore_json2:ejson()} | {error, term()}.
query(Args) ->
    apply(fun couchbase_driver:query_view/4, Args).