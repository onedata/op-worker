%%%-------------------------------------------------------------------
%%% @author Michal Wrona
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Lightweight version of monitoring state needed for worker init.
%%% @end
%%%-------------------------------------------------------------------
-module(monitoring_init_state).
-author("Michal Wrona").
-behaviour(model_behaviour).

-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_model.hrl").

%% model_behaviour callbacks
-export([save/1, get/1, list/0, exists/1, delete/1, update/2, create/1,
    model_init/0, 'after'/5, before/4, run_synchronized/2]).

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
    monitoring_init_state:save(Document#document{key =
        monitoring_state:encode_id(MonitoringIdRecord)});
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
    monitoring_init_state:update(monitoring_state:encode_id(MonitoringIdRecord), Diff);
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
    monitoring_init_state:create(Document#document{key =
        monitoring_state:encode_id(MonitoringIdRecord)});
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
    monitoring_init_state:get(monitoring_state:encode_id(MonitoringIdRecord));
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
    monitoring_init_state:delete(monitoring_state:encode_id(MonitoringIdRecord));
delete(Key) ->
    datastore:delete(?STORE_LEVEL, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1.
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:ext_key() | #monitoring_id{}) -> datastore:exists_return().
exists(#monitoring_id{} = MonitoringIdRecord) ->
    monitoring_init_state:exists(monitoring_state:encode_id(MonitoringIdRecord));
exists(Key) ->
    ?RESPONSE(datastore:exists(?STORE_LEVEL, ?MODULE, Key)).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0.
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    ?MODEL_CONFIG(monitoring_init_state_bucket, [], ?DISK_ONLY_LEVEL).

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
-spec run_synchronized(ResourceId :: binary() | #monitoring_id{},
    Fun :: fun(() -> Result :: term())) -> Result :: term().
run_synchronized(#monitoring_id{} = MonitoringIdRecord, Fun) ->
    monitoring_init_state:run_synchronized(
        monitoring_state:encode_id(MonitoringIdRecord), Fun);
run_synchronized(ResourceId, Fun) ->
    datastore:run_synchronized(?MODEL_NAME, ResourceId, Fun).


%%%===================================================================
%%% Internal functions
%%%===================================================================