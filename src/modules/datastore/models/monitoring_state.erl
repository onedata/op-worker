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
    model_init/0, 'after'/5, before/4, run_transaction/2]).

-export([encode_id/1]).

%%%===================================================================
%%% model_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback save/1.
%% @end
%%--------------------------------------------------------------------
-spec save(datastore:document() | #monitoring_id{}) ->
    {ok, datastore:ext_key()} | datastore:generic_error().
save(#document{key = #monitoring_id{} = MonitoringIdRecord} = Document) ->
    monitoring_state:save(Document#document{key = encode_id(MonitoringIdRecord)});
save(#document{} = Document) ->
    datastore:save(?STORE_LEVEL, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:ext_key() | #monitoring_id{},
    Diff :: datastore:document_diff()) ->
    {ok, datastore:ext_key()} | datastore:update_error().
update(#monitoring_id{} = MonitoringIdRecord, Diff) ->
    monitoring_state:update(encode_id(MonitoringIdRecord), Diff);
update(Key, Diff) ->
    datastore:update(?STORE_LEVEL, ?MODULE, Key, Diff).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document() | #monitoring_id{}) ->
    {ok, datastore:ext_key()} | datastore:create_error().
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
-spec get(datastore:ext_key() | #monitoring_id{}) ->
    {ok, datastore:document()} | datastore:get_error().
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
-spec exists(datastore:ext_key() | #monitoring_id{}) -> datastore:exists_return().
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
    ?MODEL_CONFIG(monitoring_state_bucket, [], ?DISK_ONLY_LEVEL).

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
%% Runs given function within locked ResourceId. This function makes sure
%% that 2 funs with same ResourceId won't run at the same time.
%% @end
%%--------------------------------------------------------------------
-spec run_transaction(ResourceId :: binary() | #monitoring_id{},
    Fun :: fun(() -> Result :: term())) -> Result :: term().
% TODO remove after update of monitoring worker
run_transaction(#monitoring_id{} = MonitoringIdRecord, Fun) ->
    monitoring_state:run_transaction(encode_id(MonitoringIdRecord), Fun);
run_transaction(ResourceId, Fun) ->
    critical_section:run([?MODEL_NAME, ResourceId], Fun).

%%--------------------------------------------------------------------
%% @doc
%% Encodes monitoring_id record to datastore id.
%% @end
%%--------------------------------------------------------------------
-spec encode_id(#monitoring_id{}) -> datastore:id().
encode_id(MonitoringIdRecord) ->
    http_utils:base64url_encode(crypto:hash(md5, term_to_binary(MonitoringIdRecord))).


%%%===================================================================
%%% Internal functions
%%%===================================================================