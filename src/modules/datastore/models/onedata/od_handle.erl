%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% @doc Cache for handle details fetched from OZ.
%%% @end
%%%-------------------------------------------------------------------
-module(od_handle).
-author("Lukasz Opiola").
-behaviour(model_behaviour).

-include("modules/datastore/datastore_specific_models_def.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_model.hrl").
-include_lib("ctool/include/oz/oz_handles.hrl").
-include_lib("ctool/include/logging.hrl").

-type doc() :: datastore:document().
-type info() :: #od_handle{}.
-type id() :: binary().
-type resource_type() :: binary().
-type resource_id() :: binary().
-type public_handle() :: binary().
-type metadata() :: binary().
-type timestamp() :: calendar:datetime().

-export_type([doc/0, info/0, id/0]).
-export_type([resource_type/0, resource_id/0, public_handle/0, metadata/0,
    timestamp/0]).

%% API
-export([actual_timestamp/0]).

%% model_behaviour callbacks
-export([save/1, get/1, get_or_fetch/2, list/0, exists/1, delete/1, update/2,
    create/1, model_init/0, 'after'/5, before/4]).
-export([create_or_update/2]).
-export([record_struct/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns structure of the record in specified version.
%% @end
%%--------------------------------------------------------------------
-spec record_struct(datastore_json:record_version()) -> datastore_json:record_struct().
record_struct(1) ->
    {record, [
        {public_handle, string},
        {resource_type, string},
        {resource_id, string},
        {metadata, string},
        {timestamp, {{integer, integer, integer}, {integer, integer, integer}}},
        {handle_service, string},
        {users, [{string, [atom]}]},
        {groups, [{string, [atom]}]},
        {eff_users, [{string, [atom]}]},
        {eff_groups, [{string, [atom]}]},
        {revision_history, [term]}
    ]}.

%%--------------------------------------------------------------------
%% @equiv universaltime().
%%--------------------------------------------------------------------
-spec actual_timestamp() -> timestamp().
actual_timestamp() ->
    erlang:universaltime().

%%%===================================================================
%%% model_behaviour callbacks
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
%% Gets space info from the database in user context. If space info is not found
%% fetches it from onezone and stores it in the database.
%% @end
%%--------------------------------------------------------------------
-spec get_or_fetch(Auth :: oz_endpoint:auth(), HandleId :: id()) ->
    {ok, datastore:document()} | datastore:get_error().
get_or_fetch(Auth, HandleId) ->
    case ?MODULE:get(HandleId) of
        {ok, Doc} -> {ok, Doc};
        {error, {not_found, _}} -> fetch(Auth, HandleId);
        {error, Reason} -> {error, Reason}
    end.

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
    StoreLevel = ?DISK_ONLY_LEVEL,
    ?MODEL_CONFIG(handle_info_bucket, [], StoreLevel).

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

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Fetches space info from onezone and stores it in the database.
%% @end
%%--------------------------------------------------------------------
-spec fetch(Auth :: oz_endpoint:auth(), HandleId :: id()) ->
    {ok, datastore:document()} | datastore:get_error().
fetch(?GUEST_SESS_ID = Auth, HandleId) ->
    {ok, #handle_details{
        public_handle = PublicHandle,
        metadata = Metadata
    }} = oz_handles:get_public_details(Auth, HandleId),

    Doc = #document{key = HandleId, value = #od_handle{
        public_handle = PublicHandle,
        metadata = Metadata
    }},

    case create(Doc) of
        {ok, _} -> ok;
        {error, already_exists} -> ok
    end,

    {ok, Doc};

fetch(Auth, HandleId) ->
    {ok, #handle_details{
        handle_service = HandleService,
        public_handle = PublicHandle,
        resource_type = ResourceType,
        resource_id = ResourceId,
        metadata = Metadata,
        timestamp = Timestamp
    }} = oz_handles:get_details(Auth, HandleId),

    Doc = #document{key = HandleId, value = #od_handle{
        handle_service = HandleService,
        public_handle = PublicHandle,
        resource_type = ResourceType,
        resource_id = ResourceId,
        metadata = Metadata,
        timestamp = Timestamp
    }},

    case create(Doc) of
        {ok, _} -> ok;
        {error, already_exists} -> ok
    end,

    {ok, Doc}.
