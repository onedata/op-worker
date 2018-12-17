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
-export([create/3, rest_url/1, query/3, delete/1]).

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
-spec create(od_space:id(), number(), number()) -> ok | {error, term()}.
create(SpaceId, TimestampWeight, AvgOpenCountPerDayWeight) ->
    ViewFunction =
        <<"function (doc, meta) {
            'use strict';
            // function used to calculate the popularity of a file
            ", (popularity_function(TimestampWeight, AvgOpenCountPerDayWeight))/binary,"

            // code for building cdmi_id
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

    ?critical("ViewFunction: ~p", [ViewFunction]),
    Ctx = datastore_model_default:get_ctx(file_popularity),
    DiscCtx = maps:get(disc_driver_ctx, Ctx),
    ok = couchbase_driver:save_view_doc(DiscCtx, ?VIEW_NAME(SpaceId), ViewFunction).

-spec delete(od_space:id()) -> ok | {error, term()}.
delete(SpaceId) ->
    Ctx = datastore_model_default:get_ctx(file_popularity),
    DiscCtx = maps:get(disc_driver_ctx, Ctx),
    couchbase_driver:delete_design_doc(DiscCtx, ?VIEW_NAME(SpaceId)).


-spec query(od_space:id(), undefined | index_token(), non_neg_integer()) ->
    {[cdmi_id:objectid()], undefined | index_token()} | {error, term()}.
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
%% Returns rest url endpoint for querying file popularity in given space.
%% @end
%%-------------------------------------------------------------------
-spec rest_url(od_space:id()) -> binary().
rest_url(SpaceId) ->
    Endpoint = oneprovider:get_rest_endpoint(str_utils:format("spaces/~s/indexes/file-popularity/query", [SpaceId])),
    list_to_binary(Endpoint).


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

popularity_function(LastOpenWeight, AvgOpenCountPerMonthWeight) ->
    TW = str_utils:to_binary(LastOpenWeight),
    OW = str_utils:to_binary(AvgOpenCountPerMonthWeight),
    <<"function popularity(last_open, dy_mov_avg) {
        return ", TW/binary, " * last_open + ", OW/binary," * dy_mov_avg;
    }">>.
