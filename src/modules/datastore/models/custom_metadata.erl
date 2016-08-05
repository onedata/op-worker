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
-export([get_json_metadata/1, get_json_metadata/2, set_json_metadata/2, set_json_metadata/3,
    get_xattr_metadata/2, list_xattr_metadata/1, remove_xattr_metadata/2, set_xattr_metadata/3,
    exists_xattr_metadata/2, get_rdf_metadata/1, set_rdf_metadata/2]).

%% model_behaviour callbacks
-export([save/1, get/1, exists/1, delete/1, update/2, create/1, model_init/0,
    'after'/5, before/4]).

-type type() :: binary().
-type name() :: binary().
-type names() :: [name()].
-type metadata() :: #metadata{}.
-type rdf() :: binary().
-type view_id() :: binary().

-export_type([type/0, name/0, names/0, metadata/0, rdf/0, view_id/0]).

-define(JSON_PREFIX, <<"onedata_json">>).
-define(RDF_PREFIX, <<"onedata_rdf">>).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @equiv get_json_metadata(FileUuid, []).
%%--------------------------------------------------------------------
-spec get_json_metadata(file_meta:uuid()) ->
    {ok, #{}} | datastore:get_error().
get_json_metadata(FileUuid) ->
    get_json_metadata(FileUuid, []).

%%--------------------------------------------------------------------
%% @doc
%% Gets json metadata subtree
%% e. g. for meta:
%%
%% {'l1': {'l2': 'value'}}
%%
%% get_json_metadata(FileUuid, [<<"l1">>, <<"l2">>]) -> {ok, <<"value">>}
%% get_json_metadata(FileUuid, [<<"l1">>]) -> {ok, #{<<"l2">> => <<"value">>}}
%% get_json_metadata(FileUuid, []) -> {ok, #{<<"l1">> => {<<"l2">> => <<"value">>}}}
%%
%% @end
%%--------------------------------------------------------------------
-spec get_json_metadata(file_meta:uuid(), [binary()]) ->
    {ok, #{}} | datastore:get_error().
get_json_metadata(FileUuid, Names) ->
    case get(FileUuid) of
        {ok, #document{value = #custom_metadata{value = Meta}}} ->
            Json = maps:get(?JSON_PREFIX, Meta, #{}),
            {ok, custom_meta_manipulation:find(Json, Names)};
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @equiv set_json_metadata(FileUuid, Json, []).
%%--------------------------------------------------------------------
-spec set_json_metadata(file_meta:uuid(), #{}) ->
    {ok, file_meta:uuid()} | datastore:get_error().
set_json_metadata(FileUuid, Json) ->
    set_json_metadata(FileUuid, Json, []).

%%--------------------------------------------------------------------
%% @doc Set json metadata subtree
%% e. g. for meta:
%%
%% {'l1': {'l2': 'value'}}
%%
%% set_json_metadata(FileUuid, <<"new_value">> [<<"l1">>, <<"l2">>])
%%    meta: {'l1': {'l2': 'new_value'}}
%% set_json_metadata(FileUuid, [<<"l1">>])
%%    meta: {'l1': 'new_value'}
%% set_json_metadata(FileUuid, []) -> {ok, #{<<"l1">> => {<<"l2">> => <<"value">>}}}
%%    meta: 'new_value'
%%--------------------------------------------------------------------
-spec set_json_metadata(file_meta:uuid(), #{}, [binary()]) ->
    {ok, file_meta:uuid()} | datastore:get_error().
set_json_metadata(FileUuid, JsonToInsert, Names) ->
    case get(FileUuid) of %todo use create or update
        {ok, Doc = #document{value = Meta = #custom_metadata{value = MetaValue}}} ->
            Json = maps:get(?JSON_PREFIX, MetaValue, #{}),
            NewJson = custom_meta_manipulation:insert(Json, JsonToInsert, Names),
            save(Doc#document{value = Meta#custom_metadata{value = MetaValue#{?JSON_PREFIX => NewJson}}});
        {error, {not_found, custom_metadata}} ->
            create(#document{key = FileUuid, value = #custom_metadata{
                space_id = get_space_id(FileUuid),
                value = #{?JSON_PREFIX => custom_meta_manipulation:insert(undefined, JsonToInsert, Names)}
            }});
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc Gets file's rdf metadata
%% @equiv get_xattr_metadata(FileUuid, ?RDF_PREFIX).
%%--------------------------------------------------------------------
-spec get_rdf_metadata(file_meta:uuid()) -> {ok, rdf()} | datastore:get_error().
get_rdf_metadata(FileUuid) ->
    get_xattr_metadata(FileUuid, ?RDF_PREFIX).

%%--------------------------------------------------------------------
%% @doc Gets file's rdf metadata
%% @equiv get_xattr_metadata(FileUuid, ?RDF_PREFIX).
%%--------------------------------------------------------------------
-spec set_rdf_metadata(file_meta:uuid(), rdf()) ->
    {ok, file_meta:uuid()} | datastore:generic_error().
set_rdf_metadata(FileUuid, Value) ->
    set_xattr_metadata(FileUuid, ?RDF_PREFIX, Value).

%%--------------------------------------------------------------------
%% @doc Get extended attribute metadata
%%--------------------------------------------------------------------
-spec get_xattr_metadata(file_meta:uuid(), xattr:name()) ->
    {ok, xattr:value()} | datastore:get_error().
get_xattr_metadata(FileUuid, Name) ->
    case get(FileUuid) of
        {ok, #document{value = #custom_metadata{value = Meta}}} ->
            case maps:get(Name, Meta, undefined) of
                undefined ->
                    {error, {not_found, custom_metadata}};
                Value ->
                    {ok, Value}
            end;
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc List extended attribute metadata names
%%--------------------------------------------------------------------
-spec list_xattr_metadata(file_meta:uuid()) ->
    {ok, [xattr:name()]} | datastore:generic_error().
list_xattr_metadata(FileUuid) ->
    case get(FileUuid) of
        {ok, #document{value = #custom_metadata{value = Meta}}} ->
            Keys = maps:keys(Meta),
            {ok, Keys};
        {error, {not_found, custom_metadata}} ->
            {ok, []};
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc Remove extended attribute metadata
%%--------------------------------------------------------------------
-spec remove_xattr_metadata(file_meta:uuid(), xattr:name()) ->
    ok | datastore:generic_error().
remove_xattr_metadata(FileUuid, Name) ->
    case get(FileUuid) of
        {ok, Doc = #document{value = Meta = #custom_metadata{value = MetaValue}}} ->
            NewMetaValue = maps:remove(Name, MetaValue),
            case save(Doc#document{value = Meta#custom_metadata{value = NewMetaValue}}) of
                {ok, _} ->
                    ok;
                Error ->
                    Error
            end;
        {error, {not_found, custom_metadata}} ->
            ok;
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc Checks if extended attribute metadata exists
%%--------------------------------------------------------------------
-spec exists_xattr_metadata(file_meta:uuid(), xattr:name()) ->
    datastore:exists_return().
exists_xattr_metadata(FileUuid, Name) ->
    case get(FileUuid) of
        {ok, #document{value = #custom_metadata{value = MetaValue}}} ->
            maps:is_key(Name, MetaValue);
        {error, {not_found, custom_metadata}} ->
            false
    end.

%%--------------------------------------------------------------------
%% @doc Set extended attribute metadata
%%--------------------------------------------------------------------
-spec set_xattr_metadata(file_meta:uuid(), xattr:name(), xattr:value()) ->
    {ok, file_meta:uuid()} | datastore:generic_error().
set_xattr_metadata(FileUuid, Name, Value) ->
    case get(FileUuid) of
        {ok, Doc = #document{value = Meta = #custom_metadata{value = MetaValue}}} ->
            NewMetaValue = maps:put(Name, Value, MetaValue),
            save(Doc#document{value = Meta#custom_metadata{value = NewMetaValue}});
        {error, {not_found, custom_metadata}} ->
            Map = maps:put(Name,Value, #{}),
            create(#document{key = FileUuid, value = #custom_metadata{
                space_id = get_space_id(FileUuid),
                value = Map}});
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
    ?MODEL_CONFIG(custom_metadata_bucket, [{file_meta, delete}], ?DISK_ONLY_LEVEL).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback 'after'/5.
%% @end
%%--------------------------------------------------------------------
-spec 'after'(ModelName :: model_behaviour:model_type(),
    Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term(),
    ReturnValue :: term()) -> ok.
'after'(file_meta, delete, ?GLOBAL_ONLY_LEVEL, [Key, _], ok) ->
    delete(Key).

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
%% Get space id of file.
%% @end
%%--------------------------------------------------------------------
-spec get_space_id(file_meta:uuid()) -> space_info:id().
get_space_id(FileUuid) ->
    {ok, #document{key = SpaceUUID}} = file_meta:get_scope({uuid, FileUuid}),
    fslogic_uuid:space_dir_uuid_to_spaceid(SpaceUUID).