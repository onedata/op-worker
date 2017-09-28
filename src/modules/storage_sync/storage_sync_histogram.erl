%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Model for storing storage_sync monitoring data.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_sync_histogram).
-author("Jakub Kudzia").
-behaviour(model_behaviour).

-include("global_definitions.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_model.hrl").
-include_lib("ctool/include/logging.hrl").


-type key() :: exometer_report:metric().
-type value() :: integer().
-type values() :: [value()].
-type timestamp() :: calendar:datetime().
-type length() :: non_neg_integer().

-export_type([key/0, value/0, values/0, timestamp/0, length/0]).


%% API
-export([new/1, add/2, get_histogram/1, remove/1]).


%% model_behaviour callbacks
-export([save/1, get/1, exists/1, delete/1, update/2, create/1,
    model_init/0, 'after'/5, before/4]).

-define(RESOLUTION, application:get_env(?APP_NAME, storage_sync_histogram_length, 12)).

%%%===================================================================
%%% API functions
%%%===================================================================

new(Metric) ->
    Key = term_to_binary(Metric),
    NewDoc = #document{
        key = Key,
        value = #storage_sync_histogram{
            values = [0 || _ <- lists:seq(1, ?RESOLUTION)],
            timestamp = calendar:local_time()
        }
    },
    {ok, Key} = save(NewDoc).

%%-------------------------------------------------------------------
%% @doc
%% Adds value to histogram associated with given metric.
%% @end
%%-------------------------------------------------------------------
-spec add(key(), value()) -> {ok ,key()}.
add(Metric, NewValue) ->
    {ok, _} = update(term_to_binary(Metric), fun(Old = #storage_sync_histogram{
        values = OldValues
    }) ->
        NewLength = length(OldValues) + 1,
        MaxLength = ?RESOLUTION,
        NewValues = lists:sublist(OldValues ++ [NewValue], NewLength - MaxLength + 1, MaxLength),
        {ok, Old#storage_sync_histogram{
            values = NewValues,
            timestamp = calendar:local_time()
        }}
    end).

%%-------------------------------------------------------------------
%% @doc
%% Returns saved values for given Metric.
%% @end
%%-------------------------------------------------------------------
-spec get_histogram(key()) -> {values(), timestamp()} | undefined.
get_histogram(Metric) ->
    case  get(term_to_binary(Metric)) of
        {ok, #document{value = #storage_sync_histogram{
            values = Values,
            timestamp = Timestamp
        }}}  ->
            {Values, Timestamp};
        _ ->
            undefined
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Removes entry for given Metric.
%% @end
%%-------------------------------------------------------------------
-spec remove(key()) -> ok.
remove(Metric) ->
    ok = delete(term_to_binary(Metric)).


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
    ?MODEL_CONFIG(storage_sync_histogram_bucket, [], ?LOCAL_ONLY_LEVEL).

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