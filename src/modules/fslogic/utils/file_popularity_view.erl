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
-include_lib("ctool/include/logging.hrl").

-define(VIEW_NAME(SpaceId), <<"file-popularity-", SpaceId/binary>>).

%% API
-export([create/1, get_unpopular_files/3, rest_url/1, initial_token/2]).

-record(token, {
    last_doc_id :: undefined | binary(),
    start_key :: binary(),
    end_key :: binary()
}).

-type  token() :: #token{}.

-export_type([token/0]).

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
        "   if(doc['_record'] == 'file_popularity' "
        "       && doc['space_id'] == '", SpaceId/binary , "' "
        "       && doc['_deleted'] == false) "
        "{ "
        "      emit("
        "         ["
        "             doc['open_count'],",
        "             doc['last_open'],",
        "             doc['size'],",
        "             doc['hr_mov_avg'],",
        "             doc['dy_mov_avg'],",
        "             doc['mth_mov_avg']"
        "         ],"
        "         doc['_key']"
        "      );"
        "   }"
        "}">>,
    Ctx = datastore_model_default:get_ctx(file_popularity),
    DiscCtx = maps:get(disc_driver_ctx, Ctx),
    couchbase_driver:save_view_doc(DiscCtx, ?VIEW_NAME(SpaceId), ViewFunction).

%%-------------------------------------------------------------------
%% @doc
%% Converts StartKey and EndKey to token that can be used to
%% query the view.
%% @end
%%-------------------------------------------------------------------
-spec initial_token(binary(), binary()) -> token().
initial_token(StartKey, EndKey) ->
    #token{
        last_doc_id = undefined,
        start_key = StartKey,
        end_key = EndKey
    }.

%%--------------------------------------------------------------------
%% @doc
%% Finds unpopular files in space
%% @end
%%--------------------------------------------------------------------
-spec get_unpopular_files(od_space:id(), token(), non_neg_integer()) ->
    {[file_ctx:ctx()], token()}.
get_unpopular_files(SpaceId, Token = #token{
    last_doc_id = undefined,
    start_key = LastKey,
    end_key = EndKey
}, Limit) ->
    Options = [
        {stale, false},
        {descending, true},
        {startkey, LastKey},
        {endkey, EndKey},
        {limit, Limit}
    ],
    query(SpaceId, Options, Token);
get_unpopular_files(SpaceId, Token = #token{
    last_doc_id = LastDocId,
    start_key = LastKey,
    end_key = EndKey
},  Limit) ->
    Options = [
        {stale, false},
        {descending, true},
        {startkey, LastKey},
        {startkey_docid, LastDocId},
        {endkey, EndKey},
        {limit, Limit},
        {skip, 1}
    ],
    query(SpaceId, Options, Token).

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

-spec query(od_space:id(), [couchbase_driver:view_opt()], token()) -> {[file_ctx:ctx()], token()}.
query(SpaceId, Options, Token) ->
    Ctx = datastore_model_default:get_ctx(file_popularity),
    DiscCtx = maps:get(disc_driver_ctx, Ctx),
    {ok, {Rows}} = query([DiscCtx, ?VIEW_NAME(SpaceId), ?VIEW_NAME(SpaceId), Options]),
    ?critical("Rows num: ~p", [length(Rows)]),
    {RevertedFileCtxs, NewToken} = lists:foldl(fun(Row, {RevertedFileCtxsIn, TokenIn}) ->
        {<<"key">>, Key} = lists:keyfind(<<"key">>, 1, Row),
        {<<"value">>, FileUuid} = lists:keyfind(<<"value">>, 1, Row),
        {<<"id">>, DocId} = lists:keyfind(<<"id">>, 1, Row),
        FileCtx = file_ctx:new_by_guid(fslogic_uuid:uuid_to_guid(FileUuid, SpaceId)),
        {[FileCtx | RevertedFileCtxsIn], TokenIn#token{
            start_key = json_utils:encode(Key),
            last_doc_id = DocId
        }}
    end, {[], Token}, Rows),
    {lists:reverse(RevertedFileCtxs), NewToken}.

%%--------------------------------------------------------------------
%% @private
%% @doc This function silences dialyzer "no local return" errors.
%% @equiv apply(fun couchbase_driver:query_view/4, Args).
%% @end
%%--------------------------------------------------------------------
-spec query(list()) -> {ok, datastore_json2:ejson()} | {error, term()}.
query(Args) ->
    apply(fun couchbase_driver:query_view/4, Args).

