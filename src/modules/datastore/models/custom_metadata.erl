%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Model for holding files' custom metadata.
%%% @end
%%%-------------------------------------------------------------------
-module(custom_metadata).
-author("Tomasz Lichon").
-behaviour(model_behaviour).

-include("modules/datastore/datastore_specific_models_def.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_model.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/errors.hrl").

%% API
-export([get_json_metadata/1, get_json_metadata/2, set_json_metadata/2, set_json_metadata/3]).

%% model_behaviour callbacks
-export([save/1, get/1, exists/1, delete/1, update/2, create/1, model_init/0,
    'after'/5, before/4]).

-type type() :: binary().
-type name() :: binary().
-type names() :: [name()].
-type metadata() :: #metadata{}.

-export_type([type/0, name/0, names/0, metadata/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @equiv get_json_metadata(FileUuid, []).
%%--------------------------------------------------------------------
-spec get_json_metadata(file_meta:uuid()) ->
    {ok, datastore:document()} | datastore:get_error().
get_json_metadata(FileUuid) ->
    get_json_metadata(FileUuid, []).

%%--------------------------------------------------------------------
%% @doc Gets json metadata subtree
%%--------------------------------------------------------------------
-spec get_json_metadata(file_meta:uuid(), [binary()]) ->
    {ok, #{}} | datastore:get_error().
get_json_metadata(FileUuid, Names) ->
    case custom_metadata:get(FileUuid) of
        {ok, #document{value = #custom_metadata{json = Json}}} ->
            {ok, custom_meta_manipulation:find(Json, Names)};
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @equiv set_json_metadata(FileUuid, Json, []).
%%--------------------------------------------------------------------
-spec set_json_metadata(file_meta:uuid(), #{}) ->
    {ok, datastore:document()} | datastore:get_error().
set_json_metadata(FileUuid, Json) ->
    set_json_metadata(FileUuid, Json, []).

%%--------------------------------------------------------------------
%% @doc Gets extended attribute with given name
%%--------------------------------------------------------------------
-spec set_json_metadata(file_meta:uuid(), #{}, [binary()]) ->
    {ok, datastore:document()} | datastore:get_error().
set_json_metadata(FileUuid, JsonToInsert, Names) ->
    case custom_metadata:get(FileUuid) of %todo use create or update
        {ok, Doc = #document{value = Meta = #custom_metadata{json = Json}}} ->
            NewJson = custom_meta_manipulation:insert(Json, JsonToInsert, Names),
            save(Doc#document{value = Meta#custom_metadata{json = NewJson}});
        {error, {not_found, custom_metadata}} ->
            create(#document{key = FileUuid, value = #custom_metadata{json = custom_meta_manipulation:insert(undefined, JsonToInsert, Names)}});
        Error ->
            Error
    end.

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
    ?MODEL_CONFIG(custom_metadata_bucket, [], ?GLOBALLY_CACHED_LEVEL).

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
%%%===================================================================
