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

%% API
-export([create/1, get_unpopular_files/6]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
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
        "             doc['last_open'],",
        "             doc['total_open_count'],",
        "             doc['hourly_moving_average'],",
        "             doc['daily_moving_average'],",
        "             doc['monthly_moving_average']",
        "         ],"
        "         [doc['file_uuid'], doc['space_id']]"
        "      );"
        "   }"
        "}">>,
    Ctx = model:make_disk_ctx(file_popularity:model_init()),
    couchbase_driver:save_spatial_view_doc(Ctx, SpaceId, ViewFunction).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Finds unpopular files in space
%% @end
%%--------------------------------------------------------------------
-spec get_unpopular_files(od_space:id(), HoursSinceLastOpen :: Limit,
    TotalOpenLimit :: Limit, HourAverageLimit :: Limit,
    DayAverageLimit :: Limit, MonthAverageLimit :: Limit) -> [file_ctx:ctx()] when
    Limit :: null | non_neg_integer().
get_unpopular_files(SpaceId, HoursSinceLastOpenLimit, TotalOpenLimit,
    HourAverageLimit, DayAverageLimit, MonthAverageLimit
) ->
    Ctx = model:make_disk_ctx(file_popularity:model_init()),
    CurrentTimeInHours = erlang:system_time(seconds) div 3600,
    HoursTimestampLimit = case HoursSinceLastOpenLimit of
        null ->
            null;
        _ ->
            CurrentTimeInHours - HoursSinceLastOpenLimit
    end,
    Options = [
        {spatial, true},
        {stale, false},
        {start_range, [0, 0, 0, 0, 0]},
        {end_range, [
            HoursTimestampLimit,
            TotalOpenLimit,
            HourAverageLimit,
            DayAverageLimit,
            MonthAverageLimit
        ]}
    ],
    {ok, {Rows}} = couchbase_driver:query_view(Ctx, SpaceId, SpaceId, Options),
    lists:map(fun(Row) ->
        {<<"value">>, [FileUuid, SpaceId]} = lists:keyfind(<<"value">>, 1, Row),
        file_ctx:new_by_guid(fslogic_uuid:uuid_to_guid(FileUuid, SpaceId))
    end, Rows).

%%%===================================================================
%%% Internal functions
%%%===================================================================