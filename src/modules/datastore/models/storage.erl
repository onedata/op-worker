%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Model for holding storage configuration.
%%%      @todo: rewrite without "ROOT_STORAGE" when implementation of persistent_store:list will be ready
%%% @end
%%%-------------------------------------------------------------------
-module(storage).
-author("Rafal Slota").
-behaviour(model_behaviour).

-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_model.hrl").

%% ID of root storage which links to all registered storage to simplify list/1 operation
-define(ROOT_STORAGE, <<"root_storage">>).

%% Resource ID used to sync all operations on this model
-define(STORAGE_LOCK_ID, <<"storage_res_id">>).


-type id() :: datastore:uuid().
-type name() :: binary().

-export_type([id/0, name/0]).

%% API
-export([id/1, name/1, helpers/1, get_by_name/1]).

%% model_behaviour callbacks
-export([save/1, get/1, exists/1, delete/1, update/2, create/1, model_init/0,
    'after'/5, before/4, list/0]).

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
create(#storage{} = S) ->
    create(#document{value = S});
create(#document{value = #storage{name = Name}} = Document) ->
    datastore:run_transaction(?MODEL_NAME, ?STORAGE_LOCK_ID, fun() ->
        case datastore:fetch_link(?LINK_STORE_LEVEL, ?ROOT_STORAGE, ?MODEL_NAME, Name) of
            {ok, _} ->
                {error, aleady_exists};
            {error, link_not_found} ->
                _ = datastore:create(?STORE_LEVEL, #document{key = ?ROOT_STORAGE, value = #storage{}}),
                case datastore:create(?STORE_LEVEL, Document) of
                    {error, Reason} ->
                        {error, Reason};
                    {ok, Key} ->
                        ok = datastore:add_links(?LINK_STORE_LEVEL, ?ROOT_STORAGE, ?MODEL_NAME, {Name, {Key, ?MODEL_NAME}}),
                        {ok, Key}
                end
        end
                                                              end).

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
    ?MODEL_CONFIG(system_config_bucket, [], ?GLOBALLY_CACHED_LEVEL).

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

%%--------------------------------------------------------------------
%% @doc
%% Returns list of all records.
%% @end
%%--------------------------------------------------------------------
-spec list() -> {ok, [datastore:document()]} | datastore:generic_error() | no_return().
list() ->
    datastore:foreach_link(?LINK_STORE_LEVEL, ?ROOT_STORAGE, ?MODEL_NAME,
        fun(_LinkName, {Key, storage}, AccIn) ->
            {ok, Doc} = get(Key),
            [Doc | AccIn]
        end, []).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Returns storage ID.
%%--------------------------------------------------------------------
-spec id(Doc :: #document{}) -> StorageId :: id().
id(#document{key = StorageId, value = #storage{}}) ->
    StorageId.

%%--------------------------------------------------------------------
%% @doc Returns storage name.
%%--------------------------------------------------------------------
-spec name(Storage :: #document{} | #storage{}) -> Name :: name().
name(#document{value = #storage{} = Storage}) ->
    name(Storage);
name(#storage{name = Name}) ->
    Name.

%%--------------------------------------------------------------------
%% @doc Returns list of storage helpers.
%%--------------------------------------------------------------------
-spec helpers(Storage :: #document{} | #storage{}) -> Helpers :: [helpers:init()].
helpers(#document{value = #storage{} = Storage}) ->
    helpers(Storage);
helpers(#storage{helpers = Helpers}) ->
    Helpers.

%%--------------------------------------------------------------------
%% @doc Returns storage document by storage name.
%%--------------------------------------------------------------------
-spec get_by_name(Name :: name()) -> {ok, datastore:document()} | datastore:get_error().
get_by_name(Name) ->
    {ok, Docs} = list(),
    get_by_name(Name, Docs).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns storage document by storage name.
%% @end
%%--------------------------------------------------------------------
-spec get_by_name(Name :: name(), Docs :: [#document{}]) ->
    {ok, datastore:document()} | datastore:get_error().
get_by_name(_, []) ->
    {error, {not_found, ?MODULE}};
get_by_name(Name, [#document{value = #storage{name = Name}} = Doc | _]) ->
    {ok, Doc};
get_by_name(Name, [_ | Docs]) ->
    get_by_name(Name, Docs).