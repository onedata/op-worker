%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Information about tasks to be done.
%%% @end
%%%-------------------------------------------------------------------
-module(task_pool).
-author("Michał Wrzeszcz").
-behaviour(model_behaviour).

-include_lib("cluster_worker/include/cluster/worker/elements/task_manager/task_manager.hrl").
-include_lib("cluster_worker/include/cluster/worker/modules/datastore/datastore_model.hrl").

%% model_behaviour callbacks
-export([save/1, get/1, list/0, list/1, list_failed/1, exists/1, delete/1, delete/2, update/2, update/3,
    create/1, create/2, model_init/0, 'after'/5, before/4]).

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
%% Same as {@link model_behaviour} callback update/2 but allows
%% choice of task level.
%% @end
%%--------------------------------------------------------------------
-spec update(Level :: task_manager:level(), datastore:key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:key()} | datastore:update_error().
update(?NON_LEVEL, _Key, _Diff) ->
    {ok, non};

update(Level, Key, Diff) ->
    datastore:update(task_to_db_level(Level), ?MODULE, Key, Diff).

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
%% Same as {@link model_behaviour} callback create/1 but allows
%% choice of task level.
%% @end
%%--------------------------------------------------------------------
-spec create(Level :: task_manager:level(), datastore:document()) ->
    {ok, datastore:key()} | datastore:create_error().
create(?NON_LEVEL, _Document) ->
    {ok, non};

create(Level, Document) ->
    datastore:create(task_to_db_level(Level), Document).

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
%% Returns list of all records.
%% @end
%%--------------------------------------------------------------------
-spec list() -> {ok, [datastore:document()]} | datastore:generic_error() | no_return().
list() ->
    datastore:list(?STORE_LEVEL, ?MODEL_NAME, ?GET_ALL, []).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of all records.
%% @end
%%--------------------------------------------------------------------
-spec list(Level :: task_manager:level()) ->
    {ok, [datastore:document()]} | datastore:generic_error() | no_return().
list(?NON_LEVEL) ->
    {ok, []};

list(Level) ->
    datastore:list(task_to_db_level(Level), ?MODEL_NAME, ?GET_ALL, []).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of tasks that failed.
%% @end
%%--------------------------------------------------------------------
-spec list_failed(Level :: task_manager:level()) ->
    {ok, [datastore:document()]} | datastore:generic_error() | no_return().
list_failed(?NON_LEVEL) ->
    {ok, []};

list_failed(Level) ->
    Filter = fun
        ('$end_of_table', Acc) ->
            {abort, Acc};
        (#document{value = V} = Doc, Acc) ->
            N = node(),
            case (V#task_pool.node =/= N) orelse is_process_alive(V#task_pool.owner) of
                false ->
                    {next, [Doc | Acc]};
                _ ->
                    {next, Acc}
            end
    end,
    datastore:list(task_to_db_level(Level), ?MODEL_NAME, Filter, []).

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
%% Same as {@link model_behaviour} callback delete/1 but allows
%% choice of task level.
%% @end
%%--------------------------------------------------------------------
-spec delete(Level :: task_manager:level(), datastore:key()) -> ok | datastore:generic_error().
delete(?NON_LEVEL, _Key) ->
    ok;

delete(Level, Key) ->
    datastore:delete(task_to_db_level(Level), ?MODULE, Key).

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
    ?MODEL_CONFIG(test_bucket, [], ?LOCAL_ONLY_LEVEL).

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

%%--------------------------------------------------------------------
%% @doc
%% Translates task level to store level.
%% @end
%%--------------------------------------------------------------------
-spec task_to_db_level(Level :: task_manager:level()) -> datastore:store_level().
task_to_db_level(?NODE_LEVEL) ->
    ?LOCAL_ONLY_LEVEL;

task_to_db_level(?CLUSTER_LEVEL) ->
    ?GLOBAL_ONLY_LEVEL;

task_to_db_level(?PERSISTENT_LEVEL) ->
    ?DISK_ONLY_LEVEL.