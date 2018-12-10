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
-export([create/1, rest_url/1, query/3, initial_index_token/2, delete/1]).

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
-spec create(od_space:id()) -> ok | {error, term()}.
create(SpaceId) ->
    ViewFunction =
        <<"function (doc, meta) {"
        "   if(doc['_record'] == 'file_popularity' "
        "       && doc['space_id'] == '", SpaceId/binary, "' "
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

-spec delete(od_space:id()) -> ok | {error, term()}.
delete(SpaceId) ->
    Ctx = datastore_model_default:get_ctx(file_popularity),
    DiscCtx = maps:get(disc_driver_ctx, Ctx),
    couchbase_driver:delete_design_doc(DiscCtx, ?VIEW_NAME(SpaceId)).

%%-------------------------------------------------------------------
%% @doc
%% Converts StartKey and EndKey to index token that can be used to
%% query the view.
%% @end
%%-------------------------------------------------------------------
-spec initial_index_token(undefined | [non_neg_integer()],
    undefined | [non_neg_integer()]) -> index_token().
initial_index_token(StartKey, EndKey) ->
    #index_token{
        last_doc_id = undefined,
        start_key = StartKey,
        end_key = EndKey
    }.

-spec query(od_space:id(), undefined | index_token(), non_neg_integer()) ->
    {[file_ctx:ctx()], undefined | index_token()} | {error, term()}.
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
            {RevertedFileCtxs, NewToken} = lists:foldl(fun(Row, {RevertedFileCtxsIn, TokenIn}) ->
                {<<"key">>, Key} = lists:keyfind(<<"key">>, 1, Row),
                {<<"value">>, FileUuid} = lists:keyfind(<<"value">>, 1, Row),
                {<<"id">>, DocId} = lists:keyfind(<<"id">>, 1, Row),
                FileCtx = file_ctx:new_by_guid(fslogic_uuid:uuid_to_guid(FileUuid, SpaceId)),
                {[FileCtx | RevertedFileCtxsIn], TokenIn#index_token{
                    start_key = Key,
                    last_doc_id = DocId
                }}
            end, {[], TokenDefined}, Rows),
            {lists:reverse(RevertedFileCtxs), NewToken};
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
    filter_undefined_opt([
        {stale, false},
        {limit, Limit}
    ]);
token_to_opts(#index_token{
    last_doc_id = undefined,
    start_key = LastKey,
    end_key = EndKey
}, Limit) ->
    filter_undefined_opt([
        {stale, false},
        {startkey, LastKey},
        {endkey, EndKey},
        {limit, Limit}
    ]);
token_to_opts(#index_token{
    last_doc_id = LastDocId,
    start_key = LastKey,
    end_key = EndKey
}, Limit) ->
    filter_undefined_opt([
        {stale, false},
        {startkey, LastKey},
        {startkey_docid, LastDocId},
        {endkey, EndKey},
        {limit, Limit},
        {skip, 1}
    ]).

-spec filter_undefined_opt([couchbase_driver:opt()]) -> [couchbase_driver:opt()].
filter_undefined_opt(Opts) ->
    [Opt || Opt = {_OptName, OptValue} <- Opts, OptValue =/= undefined].

%%--------------------------------------------------------------------
%% @private
%% @doc This function silences dialyzer "no local return" errors.
%% @equiv apply(fun couchbase_driver:query_view/4, Args).
%% @end
%%--------------------------------------------------------------------
-spec query(list()) -> {ok, datastore_json2:ejson()} | {error, term()}.
query(Args) ->
    apply(fun couchbase_driver:query_view/4, Args).