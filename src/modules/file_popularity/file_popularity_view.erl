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

-include("modules/datastore/datastore_models.hrl").
-include("modules/file_popularity/file_popularity_view.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([create/4, example_query/1, delete/1, modify/4]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates view on space files capable of ordering files by their popularity
%% @end
%%--------------------------------------------------------------------
-spec create(od_space:id(), number(), number(), number()) -> ok | {error, term()}.
create(SpaceId, TimestampWeight, AvgOpenCountPerDayWeight, MaxAvgOpenCountPerDay) ->
    ViewFunction =
        <<"function (doc, meta) {
            'use strict';
            // function used to calculate the popularity of a file
            ", (popularity_function(TimestampWeight, AvgOpenCountPerDayWeight, MaxAvgOpenCountPerDay))/binary,"

            // code for building file_id
            ", (index:build_cdmi_object_id_in_js())/binary,"

           if(doc['_record'] == 'file_popularity'
               && doc['space_id'] == '", SpaceId/binary, "'
               && doc['_deleted'] == false)
           {
               emit(
                   popularity(doc['last_open'], doc['dy_mov_avg']),
                   buildObjectId(doc['file_uuid'], doc['space_id'])
               );
           }
        }">>,

    Ctx = datastore_model_default:get_ctx(file_popularity),
    DiscCtx = maps:get(disc_driver_ctx, Ctx),
    ok = couchbase_driver:save_view_doc(DiscCtx, ?FILE_POPULARITY_VIEW(SpaceId), ViewFunction).

-spec delete(od_space:id()) -> ok | {error, term()}.
delete(SpaceId) ->
    Ctx = datastore_model_default:get_ctx(file_popularity),
    DiscCtx = maps:get(disc_driver_ctx, Ctx),
    couchbase_driver:delete_design_doc(DiscCtx, ?FILE_POPULARITY_VIEW(SpaceId)).

-spec modify(od_space:id(), number(), number(), number()) -> ok | {error, term()}.
modify(SpaceId, LastOpenWeight, AvgOpenCountPerDayWeight, MaxAvgOpenCountPerDay) ->
    case delete(SpaceId) of
        ok ->
            create(SpaceId, LastOpenWeight, AvgOpenCountPerDayWeight, MaxAvgOpenCountPerDay);
        {error, {<<"not_found">>, _}} ->
            create(SpaceId, LastOpenWeight, AvgOpenCountPerDayWeight, MaxAvgOpenCountPerDay);
        Error ->
            Error
    end.

%%-------------------------------------------------------------------
%% @doc
%% Returns example curl which can be used to query 10 least popular
%% in the space.
%% @end
%%-------------------------------------------------------------------
-spec example_query(od_space:id()) -> binary().
example_query(SpaceId) ->
    str_utils:format_bin(
        "curl -sS -k -H \"X-Auth-Token:$TOKEN\" -X GET https://~ts/api/v3/oneprovider/spaces/~ts/views/file-popularity/query?limit=10",
        [oneprovider:get_domain(), SpaceId]
    ).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% JS code of the popularity function.
%% The function is defined as:
%% P(lastOpenHour, avgOpenCountPerDay) =
%%  = w1 * lastOpenHour + w2 * min(avgOpenCountPerDay, MAX_AVG_OPEN_COUNT_PER_DAY)
%%
%% where:
%%  * lastOpenHour - parameter which is equal to timestamp (in hours since 01.01.1970)
%%    of last open operation on given file
%%  * w1 - weight of lastOpenHour parameter
%%  * avgOpenCountPerDay - parameter equal to moving average of number of open
%%    operations on given file per day. Value is calculated over last 30 days.
%%  * w2 - weight of avgOpenCountPerDay parameter
%%  * MAX_AVG_OPEN_COUNT_PER_DAY - upper boundary for avgOpenCountPerDay parameter
%% @end
%%-------------------------------------------------------------------
-spec popularity_function(number(), number(), number()) -> binary().
popularity_function(LastOpenWeight, AvgOpenCountPerMonthWeight, MaxAvgOpenCountPerDay) ->
    TW = str_utils:to_binary(LastOpenWeight),
    OW = str_utils:to_binary(AvgOpenCountPerMonthWeight),
    MA = str_utils:to_binary(MaxAvgOpenCountPerDay),
    <<"function popularity(last_open, dy_mov_avg) {
        return ", TW/binary, " * last_open + ", OW/binary," * Math.min(dy_mov_avg, ", MA/binary,");
    }">>.
