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
-include("modules/fslogic/file_popularity_view.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([create/4, example_query/1, query/3, delete/1, modify/4]).

-type  index_token() :: #index_token{}.

-export_type([index_token/0]).

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
    ok = couchbase_driver:save_view_doc(DiscCtx, ?VIEW_NAME(SpaceId), ViewFunction).

-spec delete(od_space:id()) -> ok | {error, term()}.
delete(SpaceId) ->
    Ctx = datastore_model_default:get_ctx(file_popularity),
    DiscCtx = maps:get(disc_driver_ctx, Ctx),
    couchbase_driver:delete_design_doc(DiscCtx, ?VIEW_NAME(SpaceId)).

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

-spec query(od_space:id(), undefined | index_token(), non_neg_integer()) ->
    {[file_id:objectid()], undefined | index_token()} | {error, term()}.
query(SpaceId, IndexToken, Limit) ->
    Options = token_to_opts(IndexToken, Limit),
    Ctx = datastore_model_default:get_ctx(file_popularity),
    DiscCtx = maps:get(disc_driver_ctx, Ctx),
    ViewName = ?VIEW_NAME(SpaceId),
    case query([DiscCtx, ViewName, ViewName, Options]) of
        {ok, {[]}} ->
            {[], IndexToken};
        {ok, {Rows}} ->
            TokenDefined = utils:ensure_defined(IndexToken, undefined, #index_token{}),
            {RevertedFileIds, NewToken} = lists:foldl(fun(Row, {RevertedFileIdsIn, TokenIn}) ->
                {<<"key">>, Key} = lists:keyfind(<<"key">>, 1, Row),
                {<<"value">>, FileId} = lists:keyfind(<<"value">>, 1, Row),
                {<<"id">>, DocId} = lists:keyfind(<<"id">>, 1, Row),
                {[FileId | RevertedFileIdsIn], TokenIn#index_token{
                    start_key = Key,
                    last_doc_id = DocId
                }}
            end, {[], TokenDefined}, Rows),
            {lists:reverse(RevertedFileIds), NewToken};
        Error = {error, Reason} ->
            ?error("Querying file_popularity_view ~p failed due to ~p", [ViewName, Reason]),
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
        "curl -sS -k -H \"X-Auth-Token:$TOKEN\" -X GET https://~s/api/v3/oneprovider/spaces/~s/indices/file-popularity/query?limit=10",
        [oneprovider:get_domain(), SpaceId]
    ).


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec token_to_opts(undefined | index_token(), non_neg_integer()) -> [couchbase_driver:view_opt()].
token_to_opts(undefined, Limit) ->
    [
        {stale, false},
        {limit, Limit}
    ];
token_to_opts(#index_token{
last_doc_id = undefined,
    start_key = undefined
}, Limit) -> [
        {stale, false},
        {limit, Limit}
    ];
token_to_opts(#index_token{
    last_doc_id = LastDocId,
    start_key = LastKey
}, Limit) ->
    [
        {stale, false},
        {startkey, LastKey},
        {startkey_docid, LastDocId},
        {limit, Limit},
        {skip, 1}
    ].

%%--------------------------------------------------------------------
%% @private
%% @doc This function silences dialyzer "no local return" errors.
%% @equiv apply(fun couchbase_driver:query_view/4, Args).
%% @end
%%--------------------------------------------------------------------
-spec query(list()) -> {ok, datastore_json2:ejson()} | {error, term()}.
query(Args) ->
    apply(fun couchbase_driver:query_view/4, Args).

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
