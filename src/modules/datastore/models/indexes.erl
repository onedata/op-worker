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

%% API
-export([add_index/4, get_index/2, query_view/2, get_all_indexes/1, change_index_function/3]).

%% model_behaviour callbacks
-export([save/1, get/1, exists/1, delete/1, update/2, create/1, model_init/0,
    'after'/5, before/4]).

-type view_name() :: binary().
-type index_id() :: binary().
-type view_function() :: binary().
-type index() :: #{name => indexes:view_name(), space_id => space_info:id(), function => indexes:view_function()}.

-export_type([view_name/0, index_id/0, view_function/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @equiv add_index(UserId, ViewName, ViewFunction, SpaceId, datastore_utils:gen_uuid()).
%%--------------------------------------------------------------------
-spec add_index(onedata_user:id(), view_name(), view_function(), space_info:id()) -> {ok, index_id()}.
add_index(UserId, ViewName, ViewFunction, SpaceId) ->
    add_index(UserId, ViewName, ViewFunction, SpaceId, datastore_utils:gen_uuid()).

%%--------------------------------------------------------------------
%% @doc
%% Add view defined by given function.
%% @end
%%--------------------------------------------------------------------
-spec add_index(onedata_user:id(), view_name(), view_function(), space_info:id(), index_id()) -> {ok, index_id()}.
add_index(UserId, ViewName, ViewFunction, SpaceId, IndexId) ->
    RecordName = <<"custom_metadata">>,
    DbViewFunction = <<"function (doc, meta) { if(doc['RECORD::'] == '", RecordName/binary, "' && doc['ATOM::space_id'] == '", SpaceId/binary , "') { var key = ",
        ViewFunction/binary,
        "; var key_to_emit = key(doc['ATOM::value']); if(key_to_emit) { emit(key_to_emit, null); } } }">>,
    ok = couchdb_datastore_driver:add_view(IndexId, DbViewFunction),
    case indexes:get(UserId) of
        {ok, Doc = #document{value = IndexesDoc = #indexes{value = Indexes}}} ->
            NewMap =
                case maps:get(IndexId, Indexes, undefined) of
                    undefined ->
                        maps:put(IndexId, #{name => ViewName, space_id => SpaceId, function => ViewFunction}, Indexes);
                    OldMap ->
                        maps:put(IndexId, OldMap#{function => ViewFunction}, Indexes)
                end,
            {ok, _} = save(Doc#document{value = IndexesDoc#indexes{value = NewMap}}),
            {ok, IndexId};
        {error, {not_found, indexes}} ->
            Map = maps:put(IndexId, #{name => ViewName, space_id => SpaceId, function => ViewFunction}, #{}),
            {ok, _} = create(#document{key = UserId, value = #indexes{value = Map}}),
            {ok, IndexId};
        Error ->
            ok = couchdb_datastore_driver:delete_view(IndexId),
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Get index from db.
%% @end
%%--------------------------------------------------------------------
-spec get_index(onedata_user:id(), index_id()) -> {ok, index()} | {error, ?ENOENT}.
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
-spec change_index_function(onedata_user:id(), index_id(), view_function()) -> ok | {error, ?ENOENT}.
change_index_function(UserId, IndexId, Function) ->
    add_index(UserId, undefined, Function, undefined, IndexId).

%%--------------------------------------------------------------------
%% @doc
%% Get all indexes from db, with given space id.
%% @end
%%--------------------------------------------------------------------
-spec get_all_indexes(onedata_user:id()) -> {ok, [index()]} | {error, ?ENOENT}.
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
-spec query_view(index_id(), binary()) -> {ok, [file_meta:uuid()]}.
query_view(Id, Key) ->
    {ok, FileUuids} = couchdb_datastore_driver:get_view(Id, Key),
    {ok, lists:map(fun(Uuid) -> fslogic_uuid:to_file_guid(Uuid) end, FileUuids)}.

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
