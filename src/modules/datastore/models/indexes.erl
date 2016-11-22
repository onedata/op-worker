%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Model for holding database views.
%%% @end
%%%-------------------------------------------------------------------
-module(indexes).
-author("Tomasz Lichon").
-behaviour(model_behaviour).

-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_model.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([add_index/5, get_index/2, query_view/2, get_all_indexes/1, change_index_function/3]).

%% model_behaviour callbacks
-export([save/1, get/1, exists/1, delete/1, update/2, create/1, model_init/0,
    'after'/5, before/4, create_or_update/2]).
-export([record_struct/1]).

-type view_name() :: binary().
-type index_id() :: binary().
-type view_function() :: binary().
-type spatial() :: boolean().
-type index() :: #{name => indexes:view_name(), space_id => od_space:id(), function => indexes:view_function(), spatial => indexes:spatial()}.

-export_type([view_name/0, index_id/0, view_function/0, spatial/0]).

%%--------------------------------------------------------------------
%% @doc
%% Returns structure of the record in specified version.
%% @end
%%--------------------------------------------------------------------
-spec record_struct(datastore_json:record_version()) -> datastore_json:record_struct().
record_struct(1) ->
    {record, [
        {value, #{string => {custom_type, index, index_encoder}}}
    ]}.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @equiv add_index(UserId, ViewName, ViewFunction, SpaceId, datastore_utils:gen_uuid()).
%%--------------------------------------------------------------------
-spec add_index(od_user:id(), view_name(), view_function(), od_space:id(), spatial()) ->
    {ok, index_id()} | {error, any()}.
add_index(UserId, ViewName, ViewFunction, SpaceId, Spatial) ->
    add_index(UserId, ViewName, ViewFunction, SpaceId, Spatial, datastore_utils:gen_uuid()).

%%--------------------------------------------------------------------
%% @doc
%% Add view defined by given function.
%% @end
%%--------------------------------------------------------------------
-spec add_index(od_user:id(), view_name() | undefined, view_function(),
    od_space:id() | undefined, spatial() | undefined, index_id()) -> {ok, index_id()} | {error, any()}.
add_index(UserId, ViewName, ViewFunction, SpaceId, Spatial, IndexId) ->
    EscapedViewFunction = escape_js_function(ViewFunction),
    critical_section:run([?MODEL_NAME, UserId], fun() ->
        case indexes:get(UserId) of
            {ok, Doc = #document{value = IndexesDoc = #indexes{value = Indexes}}} ->
                NewMap =
                    case maps:get(IndexId, Indexes, undefined) of
                        undefined ->
                            add_db_view(IndexId, SpaceId, EscapedViewFunction, Spatial),
                            maps:put(IndexId, #{name => ViewName, space_id => SpaceId, function => EscapedViewFunction, spatial => Spatial}, Indexes);
                        OldMap = #{space_id := SId, spatial := DefinedSpatial} when SpaceId =:= undefined ->
                            add_db_view(IndexId, SId, EscapedViewFunction, DefinedSpatial),
                            maps:put(IndexId, OldMap#{function => EscapedViewFunction}, Indexes);
                        OldMap = #{spatial := DefinedSpatial} ->
                            add_db_view(IndexId, SpaceId, EscapedViewFunction, DefinedSpatial),
                            maps:put(IndexId, OldMap#{function => EscapedViewFunction, space_id => SpaceId}, Indexes)
                    end,
                case save(Doc#document{value = IndexesDoc#indexes{value = NewMap}}) of
                    {ok, _} ->
                        {ok, IndexId};
                    Error ->
                        Error
                end;
            {error, {not_found, indexes}} ->
                add_db_view(IndexId, SpaceId, EscapedViewFunction, Spatial),
                Map = maps:put(IndexId, #{name => ViewName, space_id => SpaceId, function => EscapedViewFunction, spatial => Spatial}, #{}),
                case create(#document{key = UserId, value = #indexes{value = Map}}) of
                    {ok, _} ->
                        {ok, IndexId};
                    Error ->
                        Error
                end;
            Error ->
                Error
        end
    end).

%%--------------------------------------------------------------------
%% @doc
%% Get index from db.
%% @end
%%--------------------------------------------------------------------
-spec get_index(od_user:id(), index_id()) -> {ok, index()} | {error, any()}.
get_index(UserId, IndexId) ->
    case indexes:get(UserId) of
        {ok, #document{value = #indexes{value = Indexes}}} ->
            case maps:get(IndexId, Indexes, undefined) of
                undefined ->
                    {error, ?ENOENT};
                Val ->
                    {ok, Val#{id => IndexId}}
            end;
        {error, {not_found, indexes}} ->
            {error, ?ENOENT};
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Change index js function.
%% @end
%%--------------------------------------------------------------------
-spec change_index_function(od_user:id(), index_id(), view_function()) -> {ok, index_id()} | {error, any()}.
change_index_function(UserId, IndexId, Function) ->
    add_index(UserId, undefined, Function, undefined, undefined, IndexId).

%%--------------------------------------------------------------------
%% @doc
%% Get all indexes from db, with given space id.
%% @end
%%--------------------------------------------------------------------
-spec get_all_indexes(od_user:id()) -> {ok, maps:map()} | {error, any()}.
get_all_indexes(UserId) ->
    case indexes:get(UserId) of
        {ok, #document{value = #indexes{value = Indexes}}} ->
            {ok, Indexes};
        {error, {not_found, indexes}} ->
            {ok, #{}};
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Get view result.
%% @end
%%--------------------------------------------------------------------
-spec query_view(index_id(), list()) -> {ok, [file_meta:uuid()]}.
query_view(Id, Options) ->
    {ok, FileUuids} = couchdb_datastore_driver:query_view(custom_metadata, Id, Options),
    {ok, lists:filtermap(fun(Uuid) ->
        try
            {true, fslogic_uuid:uuid_to_guid(Uuid)}
        catch
            _:_  ->
                ?error("Cannot resolve uuid of file ~p in index ~p",[Uuid, Id]),
                false
        end
    end, FileUuids)}.

%%%===================================================================
%%% model_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback save/1.
%% @end
%%--------------------------------------------------------------------
-spec save(datastore:document()) ->
    {ok, datastore:key()} | datastore:generic_error().
save(Document) ->
    datastore:save(?STORE_LEVEL, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:key()} | datastore:update_error().
update(Key, Diff) ->
    datastore:update(?STORE_LEVEL, ?MODULE, Key, Diff).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) ->
    {ok, datastore:key()} | datastore:create_error().
create(Document) ->
    datastore:create(?STORE_LEVEL, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:key()) -> {ok, datastore:document()} | datastore:get_error().
get(Key) ->
    datastore:get(?STORE_LEVEL, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:key()) -> ok | datastore:generic_error().
delete(Key) ->
    datastore:delete(?STORE_LEVEL, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1.
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:key()) -> datastore:exists_return().
exists(Key) ->
    ?RESPONSE(datastore:exists(?STORE_LEVEL, ?MODULE, Key)).

%%--------------------------------------------------------------------
%% @doc
%% Updates document with using ID from document. If such object does not exist,
%% it initialises the object with the document.
%% @end
%%--------------------------------------------------------------------
-spec create_or_update(datastore:document(), Diff :: datastore:document_diff()) ->
    {ok, datastore:ext_key()} | datastore:update_error().
create_or_update(Doc, Diff) ->
    datastore:create_or_update(?STORE_LEVEL, Doc, Diff).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0.
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    ?MODEL_CONFIG(index_bucket, [], ?GLOBALLY_CACHED_LEVEL).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback 'after'/5.
%% @end
%%--------------------------------------------------------------------
-spec 'after'(ModelName :: model_behaviour:model_type(),
    Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term(),
    ReturnValue :: term()) -> ok.
'after'(_ModelName, _Method, _Level, _Context, _ReturnValue) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback before/4.
%% @end
%%--------------------------------------------------------------------
-spec before(ModelName :: model_behaviour:model_type(),
    Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term()) ->
    ok | datastore:generic_error().
before(_ModelName, _Method, _Level, _Context) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%==================================================================

%%--------------------------------------------------------------------
%% @doc
%% Add view to db, Function should be a valid, escaped javascript string
%% with one argument function.
%% @end
%%--------------------------------------------------------------------
-spec add_db_view(index_id(), od_space:id(), view_function(), spatial()) -> ok.
add_db_view(IndexId, SpaceId, Function, Spatial) ->
    RecordName = custom_metadata,
    RecordNameBin = atom_to_binary(RecordName, utf8),
    DbViewFunction = <<"function (doc, meta) { 'use strict'; if(doc['<record_type>'] == '", RecordNameBin/binary, "' && doc['space_id'] == '", SpaceId/binary , "') { "
        "var key = eval.call(null, '(", Function/binary, ")'); ",
        "var key_to_emit = key(doc['value']); if(key_to_emit) { emit(key_to_emit, null); } } }">>,
    ok = couchdb_datastore_driver:add_view(RecordName, IndexId, DbViewFunction, Spatial).

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