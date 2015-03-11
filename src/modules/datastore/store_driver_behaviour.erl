%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Behaviour for datastore drivers for databases and memory stores.
%%% @end
%%%-------------------------------------------------------------------
-module(store_driver_behaviour).
-author("Rafal Slota").

-include("modules/datastore/datastore.hrl").

%%--------------------------------------------------------------------
%% @doc
%% Initializes given bucket locally (this method is executed per-node).
%% @end
%%--------------------------------------------------------------------
-callback init_bucket(Bucket :: datastore:bucket(), Models :: [model_behaviour:model_config()]) -> ok.


%%--------------------------------------------------------------------
%% @doc
%% Saves given #document.
%% @end
%%--------------------------------------------------------------------
-callback save(model_behaviour:model_config(), datastore:document()) -> {ok, datastore:key()} | datastore:generic_error().


%%--------------------------------------------------------------------
%% @doc
%% Updates given by key document by replacing given fields with new values.
%% @end
%%--------------------------------------------------------------------
-callback update(model_behaviour:model_config(), datastore:key(),
                    Diff :: datastore:document_diff()) -> {ok, datastore:key()} | datastore:update_error().


%%--------------------------------------------------------------------
%% @doc
%% Creates new #document.
%% @end
%%--------------------------------------------------------------------
-callback create(model_behaviour:model_config(), datastore:document()) -> {ok, datastore:key()} | datastore:create_error().


%%--------------------------------------------------------------------
%% @doc
%% Gets #document with given key.
%% @end
%%--------------------------------------------------------------------
-callback get(model_behaviour:model_config(), datastore:key()) -> {ok, datastore:document()} | datastore:get_error().


%%--------------------------------------------------------------------
%% @doc
%% Deletes #document with given key.
%% @end
%%--------------------------------------------------------------------
-callback delete(model_behaviour:model_config(), datastore:key()) -> ok | datastore:generic_error().


%%--------------------------------------------------------------------
%% @doc
%% Checks if #document with given key exists.
%% @end
%%--------------------------------------------------------------------
-callback exists(model_behaviour:model_config(), datastore:key()) -> true | false | datastore:generic_error().


%%--------------------------------------------------------------------
%% @doc
%% Checks driver state.
%% @end
%%--------------------------------------------------------------------
-callback healthcheck(WorkerState :: term()) -> ok | {error, Reason :: term()}.
