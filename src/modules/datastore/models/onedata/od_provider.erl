%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Cache for space details fetched from Global Registry.
%%% @end
%%%-------------------------------------------------------------------
-module(od_provider).
-author("Michal Zmuda").
-behaviour(model_behaviour).

-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_model.hrl").
-include_lib("ctool/include/oz/oz_providers.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([create_or_update/2, get_or_fetch/1, fetch/1]).

%% model_behaviour callbacks
-export([save/1, get/1, list/0, exists/1, delete/1, update/2, create/1, model_init/0,
    'after'/5, before/4]).
-export([record_struct/1]).
-export([record_upgrade/2]).

-type id() :: binary().

-export_type([id/0]).

%%--------------------------------------------------------------------
%% @doc
%% Returns structure of the record in specified version.
%% @end
%%--------------------------------------------------------------------
-spec record_struct(datastore_json:record_version()) -> datastore_json:record_struct().
record_struct(1) ->
    {record, [
        {client_name, string},
        {urls, [string]},
        {spaces, [string]},
        {public_only, boolean},
        {revision_history, [term]}
    ]};
record_struct(2) ->
    {record, [
        {client_name, string},
        {urls, [string]},
        {latitude, float},
        {longitude, float},
        {spaces, [string]},
        {public_only, boolean},
        {revision_history, [term]}
    ]}.

%%%===================================================================
%%% model_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback save/1.
%% @end
%%--------------------------------------------------------------------
-spec save(datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:generic_error().
save(Document) ->
    model:execute_with_default_context(?MODULE, save, [Document]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:ext_key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:ext_key()} | datastore:update_error().
update(Key, Diff) ->
    model:execute_with_default_context(?MODULE, update, [Key, Diff]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:create_error().
create(Document) ->
    model:execute_with_default_context(?MODULE, create, [Document]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:ext_key()) -> {ok, datastore:document()} | datastore:get_error().
get(Key) ->
    model:execute_with_default_context(?MODULE, get, [Key]).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of all records.
%% @end
%%--------------------------------------------------------------------
-spec list() -> {ok, [datastore:document()]} | datastore:generic_error() | no_return().
list() ->
    model:execute_with_default_context(?MODULE, list, [?GET_ALL, []]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:ext_key()) -> ok | datastore:generic_error().
delete(Key) ->
    model:execute_with_default_context(?MODULE, delete, [Key]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1.
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:ext_key()) -> datastore:exists_return().
exists(Key) ->
    ?RESPONSE(model:execute_with_default_context(?MODULE, exists, [Key])).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0.
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    Config = ?MODEL_CONFIG(provider_info_bucket, [], ?GLOBALLY_CACHED_LEVEL),
    Config#model_config{
        version = 2
    }.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback 'after'/5.
%% @end
%%--------------------------------------------------------------------
-spec 'after'(ModelName :: model_behaviour:model_type(), Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term(),
    ReturnValue :: term()) -> ok.
'after'(_ModelName, _Method, _Level, _Context, _ReturnValue) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback before/4.
%% @end
%%--------------------------------------------------------------------
-spec before(ModelName :: model_behaviour:model_type(), Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term()) -> ok | datastore:generic_error().
before(_ModelName, _Method, _Level, _Context) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Upgrades record from specified version.
%% @end
%%--------------------------------------------------------------------
-spec record_upgrade(datastore_json:record_version(), tuple()) ->
    {datastore_json:record_version(), tuple()}.
record_upgrade(1, Provider) ->
    {
        od_provider,
        ClientName,
        Urls,

        Spaces,

        PublicOnly,
        RevisionHistory
    } = Provider,
    {2, #od_provider{
        client_name = ClientName,
        urls = Urls,
        latitude = 0.0,
        longitude = 0.0,

        spaces = Spaces,

        public_only = PublicOnly,
        revision_history = RevisionHistory
    }}.


%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Updates document with using ID from document. If such object does not exist,
%% it initialises the object with the document.
%% @end
%%--------------------------------------------------------------------
-spec create_or_update(datastore:document(), Diff :: datastore:document_diff()) ->
    {ok, datastore:ext_key()} | datastore:generic_error().
create_or_update(Doc, Diff) ->
    model:execute_with_default_context(?MODULE, create_or_update, [Doc, Diff]).


%%--------------------------------------------------------------------
%% @doc
%% Get provider from cache or fetch from OZ and save in cache.
%% @end
%%--------------------------------------------------------------------
-spec get_or_fetch(ProviderId :: id()) ->
    {ok, datastore:document()} | datastore:get_error().
get_or_fetch(ProviderId) ->
    case od_provider:get(ProviderId) of
        {ok, Doc} -> {ok, Doc};
        {error, {not_found, _}} -> fetch(ProviderId);
        Error -> Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Fetch provider from OZ and save it in cache.
%% @end
%%--------------------------------------------------------------------
-spec fetch(ProviderId :: id()) ->
    {ok, datastore:document()} | {error, Reason :: term()}.
fetch(ProviderId) ->
    try
        {ok, #provider_details{
            name = Name, urls = URLs,
            latitude = Latitude, longitude = Longitude
        }} = oz_providers:get_details(provider, ProviderId),
        {PublicOnly, SpaceIDs} = case oz_providers:get_spaces(provider) of
            {ok, SIDs} -> {false, SIDs};
            {error, Res} ->
                ?warning("Unable to fetch public info for provider ~p due to ~p", [
                    ProviderId, Res]),
                {true, []}
        end,

        Doc = #document{key = ProviderId, value = #od_provider{
            client_name = Name,
            urls = URLs,
            latitude = Latitude,
            longitude = Longitude,
            spaces = SpaceIDs,
            public_only = PublicOnly
        }},

        case od_provider:create(Doc) of
            {ok, _} -> ok;
            {error, already_exists} -> ok
        end,
        {ok, Doc}
    catch
        _:Reason ->
            {error, Reason}
    end.