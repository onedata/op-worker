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

-export([decoded_list/0, decode_id/1, encode_id/1]).

%%%===================================================================
%%% model_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback save/1.
%% @end
%%--------------------------------------------------------------------
-spec save(datastore:document()) -> {ok, datastore:ext_key()} | datastore:generic_error().
save(#document{key = #monitoring_id{} = MonitoringIdRecord} = Document) ->
    monitoring_state:save(Document#document{key = encode_id(MonitoringIdRecord)});
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
create(#document{key = #monitoring_id{} = MonitoringIdRecord} = Document) ->
    monitoring_state:create(Document#document{key = encode_id(MonitoringIdRecord)});
create(#document{} = Document) ->
    datastore:create(?STORE_LEVEL, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% Sets access time to current time for user session and returns old value.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:ext_key()) -> {ok, datastore:document()} | datastore:get_error().
get(#monitoring_id{} = MonitoringIdRecord) ->
    monitoring_state:get(encode_id(MonitoringIdRecord));
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
delete(#monitoring_id{} = MonitoringIdRecord) ->
    monitoring_state:delete(encode_id(MonitoringIdRecord));
delete(Key) ->
    datastore:delete(?STORE_LEVEL, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1.
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:ext_key()) -> datastore:exists_return().
exists(#monitoring_id{} = MonitoringIdRecord) ->
    monitoring_state:exists(encode_id(MonitoringIdRecord));
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
%% Returns Monitoring State with Id decoded to SubjectType,
%% SubjectId and MetricType.
%% @end
%%--------------------------------------------------------------------
-spec decoded_list() -> {ok, [{#monitoring_id{}, #monitoring_state{}}]} |
    datastore:generic_error() | no_return().
decoded_list() ->
    case monitoring_state:list() of
        {ok, Docs} ->
            DecodedDocs = lists:map(fun(#document{key = Key, value = Value}) ->
                MonitoringId = decode_id(Key),
                {MonitoringId, Value}
            end, Docs),
            {ok, DecodedDocs};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Decodes monitoring datastore id to monitoring_id record.
%% @end
%%--------------------------------------------------------------------
-spec decode_id(datastore:id()) -> #monitoring_id{}.
decode_id(MonitoringIdBinary) ->
    binary_to_term(MonitoringIdBinary).

%%--------------------------------------------------------------------
%% @doc
%% Encodes monitoring_id record to datastore id.
%% @end
%%--------------------------------------------------------------------
-spec encode_id(#monitoring_id{}) -> datastore:id().
encode_id(MonitoringIdRecord) ->
    term_to_binary(MonitoringIdRecord).


%%%===================================================================
%%% Internal functions
%%%===================================================================