%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model storing aggregated statistics about transfers
%%% featuring given space and target provider.
%%% @end
%%%-------------------------------------------------------------------
-module(index).
-author("Bartosz Walkowicz").
-author("Jakub Kudzia").

-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([delete/2, list/1, save/6, save_db_view/5, query_view_and_filter_values/3,
    query_view/3, get_json/2]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0]).

-type name() :: binary().
-type key() :: datastore_utils:key().
-type index() :: #index{}.
-type index_function() :: binary().
-type options() :: proplists:proplist().
-type query_options() :: [couchbase_driver:view_opt()].
-type providers() :: all | [od_provider:id()].
-type doc() :: datastore_doc:doc(index()).

-export_type([name/0, index/0, index_function/0, doc/0, options/0, providers/0, query_options/0]).

-define(CTX, #{
    model => ?MODULE,
    sync_enabled => true,
    mutator => oneprovider:get_id_or_undefined()
}).
-define(DISC_CTX, #{bucket => <<"onedata">>}).

%%%===================================================================
%%% API
%%%===================================================================

% TODO jakie ma być id?

-spec save(od_space:id(), name(), index_function(), options(), boolean(), [od_provider:id()]) -> ok.
save(SpaceId, Name, MapFunction, Options, Spatial, Providers) ->
    Id = id(Name, SpaceId),
    EscapedMapFunction = escape_js_function(MapFunction),
    Doc = #document{
        key = Id,
        value = #index{
            name = Name,
            space_id = SpaceId,
            spatial = Spatial,
            map_function = EscapedMapFunction,
            index_options = Options,
            providers = Providers
        },
        scope = SpaceId
    },
    {ok, _} = datastore_model:save(?CTX, Doc),
    ok = save_db_view(Id, SpaceId, EscapedMapFunction, Spatial, Options),
    ok = index_links:add_link(Name, SpaceId).

-spec get_json(od_space:id(), binary()) ->
    #{binary() => term()} | {error, term()}.
get_json(SpaceId, IndexName) ->
    Id = id(IndexName, SpaceId),
    case datastore_model:get(?CTX, Id) of
        {ok, #document{value = #index{
            spatial = Spatial,
            map_function = MapFunction,
            index_options = Options,
            providers = Providers
        }}} ->
            #{
                <<"indexOptions">> => Options,
                <<"providers">> => Providers,
                <<"mapFunction">> => MapFunction,
                % <<"reduceFunction">> => ReduceFunction, todo
                <<"spatial">> => Spatial
            };
        Error -> Error
    end.

-spec delete(od_space:id(), binary()) -> ok | {error, term()}.
delete(SpaceId, IndexName) ->
    Id = id(IndexName, SpaceId),
    datastore_model:delete(?CTX, Id),
    index_links:delete_links(IndexName, SpaceId).

-spec list(od_space:id()) -> {ok, [index:name()]}.
list(SpaceId) ->
    list(SpaceId, undefined, 0, all).

-spec list(od_space:id(), undefined | name(), integer(), non_neg_integer() | all) -> {ok, [index:name()]}.
list(SpaceId, StartId, Offset, Limit) ->
    {ok, index_links:list(SpaceId, StartId, Offset, Limit)}.

-spec id(name(), od_space:id()) -> key().
id(IndexName, SpaceId) ->
    datastore_utils:gen_key(IndexName, SpaceId).

%%--------------------------------------------------------------------
%% @doc
%% Add view to db, Function should be a valid, escaped javascript string
%% with one argument function.
%% @end
%%--------------------------------------------------------------------
-spec save_db_view(key(), od_space:id(), index_function(), boolean(), options()) -> ok.
save_db_view(IndexId, SpaceId, Function, Spatial, Options) ->
    RecordName = custom_metadata,
    RecordNameBin = atom_to_binary(RecordName, utf8),
    ViewFunction = <<"function (doc, meta) { 'use strict'; if(doc['_record'] == '", RecordNameBin/binary, "' && doc['space_id'] == '", SpaceId/binary, "') { "
    "var key = eval.call(null, '(", Function/binary, ")'); ",
        "var key_to_emit = key(doc['value']); if(key_to_emit) { emit(key_to_emit, doc['_key']); } } }">>,
    ok = case Spatial of
        true ->
            couchbase_driver:save_spatial_view_doc(?DISC_CTX, IndexId, ViewFunction, Options);
        _ ->
            couchbase_driver:save_view_doc(?DISC_CTX, IndexId, ViewFunction, Options)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Get view result.
%% @end
%%--------------------------------------------------------------------
-spec query_view_and_filter_values(od_space:id(), name(), list()) -> {ok, [file_meta:uuid()]}.
query_view_and_filter_values(SpaceId, IndexName, Options) ->
    Id = id(IndexName, SpaceId),
    {ok, {Rows}} = couchbase_driver:query_view(?DISC_CTX, Id, Id, Options),
    FileUuids = lists:map(fun(Row) ->
        {<<"value">>, FileUuid} = lists:keyfind(<<"value">>, 1, Row),
        FileUuid
    end, Rows),
    {ok, lists:filtermap(fun(Uuid) ->
        try
            {true, fslogic_uuid:uuid_to_guid(Uuid)}
        catch
            _:Error ->
                ?error("Cannot resolve uuid of file ~p in index ~p, error ~p", [Uuid, Id, Error]),
                false
        end
    end, FileUuids)}.

query_view(SpaceId, IndexName, Options) ->
    Id = id(IndexName, SpaceId),
    couchbase_driver:query_view(?DISC_CTX, Id, Id, Options).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc escapes characters: \ " ' \n \t \v \0 \f \r
%%--------------------------------------------------------------------
-spec escape_js_function(Function :: binary()) -> binary().
escape_js_function(undefined) ->
    undefined;
escape_js_function(Function) ->
    escape_js_function(Function, [{<<"\\\\">>, <<"\\\\\\\\">>}, {<<"'">>, <<"\\\\'">>},
        {<<"\"">>, <<"\\\\\"">>}, {<<"\\n">>, <<"\\\\n">>}, {<<"\\t">>, <<"\\\\t">>},
        {<<"\\v">>, <<"\\\\v">>}, {<<"\\0">>, <<"\\\\0">>}, {<<"\\f">>, <<"\\\\f">>},
        {<<"\\r">>, <<"\\\\r">>}]).

%%--------------------------------------------------------------------
%% @doc Escapes characters given as proplists, in provided Function.
%%--------------------------------------------------------------------
-spec escape_js_function(Function :: binary(), [{binary(), binary()}]) -> binary().
escape_js_function(Function, []) ->
    Function;
escape_js_function(Function, [{Pattern, Replacement} | Rest]) ->
    EscapedFunction = re:replace(Function, Pattern, Replacement, [{return, binary}, global]),
    escape_js_function(EscapedFunction, Rest).

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
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
    1.


%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {name, string},
        {space_id, string},
        {spatial, atom},
        {map_function, string},
        {reduce_function, string},
        {index_options, [{atom, term}]},
        {providers, [string]}
    ]}.
