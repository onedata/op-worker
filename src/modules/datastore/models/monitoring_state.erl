%%%-------------------------------------------------------------------
%%% @author Michal Wrona
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Persistent state of Monitoring worker.
%%% @end
%%%-------------------------------------------------------------------
-module(monitoring_state).
-author("Michal Wrona").
-behaviour(model_behaviour).

-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_model.hrl").

%% model_behaviour callbacks
-export([save/1, get/1, list/0, exists/1, delete/1, update/2, create/1,
    model_init/0, 'after'/5, before/4]).

-export([save/4, get/3, get/4, decoded_list/0, exists/3, delete/3, create/4,
    decode_id/1]).

%%%===================================================================
%%% model_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback save/1.
%% @end
%%--------------------------------------------------------------------
-spec save(datastore:document()) -> {ok, datastore:ext_key()} | datastore:generic_error().
save(#document{} = Document) ->
    datastore:save(?STORE_LEVEL, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:ext_key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:ext_key()} | datastore:update_error().
update(Key, Diff) ->
    datastore:update(?STORE_LEVEL, ?MODULE, Key, Diff).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) -> {ok, datastore:ext_key()} | datastore:create_error().
create(#document{} = Document) ->
    datastore:create(?STORE_LEVEL, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% Sets access time to current time for user session and returns old value.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:ext_key()) -> {ok, datastore:document()} | datastore:get_error().
get(Key) ->
    datastore:get(?STORE_LEVEL, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of all records.
%% @end
%%--------------------------------------------------------------------
-spec list() -> {ok, [datastore:document()]} | datastore:generic_error() | no_return().
list() ->
    datastore:list(?STORE_LEVEL, ?MODEL_NAME, ?GET_ALL, []).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:ext_key()) -> ok | datastore:generic_error().
delete(Key) ->
    datastore:delete(?STORE_LEVEL, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1.
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:ext_key()) -> datastore:exists_return().
exists(Key) ->
    ?RESPONSE(datastore:exists(?STORE_LEVEL, ?MODULE, Key)).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0.
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    ?MODEL_CONFIG(monitoring_state_bucket, [], ?GLOBALLY_CACHED_LEVEL).

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
%% Saves Monitoring State for given SubjectType, SubjectId and MetricType.
%% @end
%%--------------------------------------------------------------------
-spec save(atom(), datastore:id(), atom(), #monitoring_state{}) ->
    {ok, datastore:ext_key()} | datastore:create_error().
save(SubjectType, SubjectId, MetricType, MonitoringState) ->
    monitoring_state:save(#document{
        key = encode_id(SubjectType, SubjectId, MetricType),
        value = MonitoringState
    }).

%%--------------------------------------------------------------------
%% @doc
%% Returns Monitoring State for given SubjectType, SubjectId and MetricType.
%% @end
%%--------------------------------------------------------------------
-spec get(atom(), datastore:id(), atom()) -> {ok, #monitoring_state{}} | datastore:get_error().
get(SubjectType, SubjectId, MetricType) ->
    case monitoring_state:get(encode_id(SubjectType, SubjectId, MetricType)) of
        {ok, #document{value = MonitoringState}} ->
            {ok, MonitoringState};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns Monitoring State for given SubjectType, SubjectId and MetricType.
%% @end
%%--------------------------------------------------------------------
-spec get(atom(), datastore:id(), atom(), oneprovider:id()) ->
    {ok, #monitoring_state{}} | datastore:get_error().
get(SubjectType, SubjectId, MetricType, ProviderId) ->
    case monitoring_state:get(encode_id(SubjectType, SubjectId, MetricType, ProviderId)) of
        {ok, #document{value = MonitoringState}} ->
            {ok, MonitoringState};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns Monitoring State with Id decoded to SubjectType,
%% SubjectId and MetricType.
%% @end
%%--------------------------------------------------------------------
-spec decoded_list() -> {ok, [{atom(), datastore:id(), atom(),
    #monitoring_state{}}]} | datastore:generic_error() | no_return().
decoded_list() ->
    case monitoring_state:list() of
        {ok, Docs} ->
            DecodedDocs = lists:map(fun(#document{key = Key, value = Value}) ->
                {SubjectType, SubjectId, MetricType, ProviderId} = decode_id(Key),
                {SubjectType, SubjectId, MetricType, ProviderId, Value}
            end, Docs),
            {ok, DecodedDocs};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns true when monitoring state for given SubjectType,
%% SubjectId and MetricType exists, false otherwise.
%% @end
%%--------------------------------------------------------------------
-spec exists(atom(), datastore:id(), atom()) -> boolean() | datastore:generic_error().
exists(SubjectType, SubjectId, MetricType) ->
    monitoring_state:exists(encode_id(SubjectType, SubjectId, MetricType)).

%%--------------------------------------------------------------------
%% @doc
%% Deletes document for given SubjectType, SubjectId and MetricType.
%% @end
%%--------------------------------------------------------------------
-spec delete(atom(), datastore:id(), atom()) -> ok | datastore:create_error().
delete(SubjectType, SubjectId, MetricType) ->
    monitoring_state:delete(encode_id(SubjectType, SubjectId, MetricType)).

%%--------------------------------------------------------------------
%% @doc
%% Creates document with Monitoring State for given SubjectType,
%% SubjectId and MetricType.
%% @end
%%--------------------------------------------------------------------
-spec create(atom(), datastore:id(), atom(), #monitoring_state{}) ->
    {ok, datastore:ext_key()} | datastore:create_error().
create(SubjectType, SubjectId, MetricType, MonitoringState) ->
    monitoring_state:create(#document{
        key = encode_id(SubjectType, SubjectId, MetricType),
        value = MonitoringState
    }).

%%--------------------------------------------------------------------
%% @doc
%% Decodes Monitoring State Id to tuple containing SubjectType, SubjectId,
%% MetricType and ProviderId
%% @end
%%--------------------------------------------------------------------
-spec decode_id(datastore:id()) -> {SubjectType :: atom(),
    SubjectId :: datastore:id(), MetricType :: atom(), oneprovider:id()}.
decode_id(Id) ->
    binary_to_term(Id).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @private
%% Encodes tuple containing SubjectType, SubjectId, MetricType
%% and ProviderId to Monitoring State Id.
%% @end
%%--------------------------------------------------------------------
-spec encode_id(SubjectType :: atom(), SubjectId :: datastore:id(),
    MetricType :: atom(), oneprovider:id()) -> datastore:id().
encode_id(SubjectType, SubjectId, MetricType) ->
    term_to_binary({SubjectType, SubjectId, MetricType, oneprovider:get_provider_id()}).

%%--------------------------------------------------------------------
%% @doc
%% @private
%% Encodes tuple containing SubjectType, SubjectId, MetricType to
%% Monitoring State Id using this provider ID.
%% @end
%%--------------------------------------------------------------------
-spec encode_id(SubjectType :: atom(), SubjectId :: datastore:id(),
    MetricType :: atom()) -> datastore:id().
encode_id(SubjectType, SubjectId, MetricType, ProviderId) ->
    term_to_binary({SubjectType, SubjectId, MetricType, ProviderId}).

