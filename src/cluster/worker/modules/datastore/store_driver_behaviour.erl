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

-type driver_action() :: model_behaviour:model_action().

-export_type([driver_action/0]).

%%--------------------------------------------------------------------
%% @doc
%% Initializes given bucket locally (this method is executed per-node).
%% @end
%%--------------------------------------------------------------------
-callback init_bucket(Bucket :: datastore:bucket(), Models :: [model_behaviour:model_config()], NodeToSync :: node()) -> ok.


%%--------------------------------------------------------------------
%% @doc
%% Saves given #document.
%% @end
%%--------------------------------------------------------------------
-callback save(model_behaviour:model_config(), datastore:document()) -> {ok, datastore:ext_key()} | datastore:generic_error().


%%--------------------------------------------------------------------
%% @doc
%% Updates given by key document by replacing given fields with new values.
%% @end
%%--------------------------------------------------------------------
-callback update(model_behaviour:model_config(), datastore:ext_key(),
                    Diff :: datastore:document_diff()) -> {ok, datastore:ext_key()} | datastore:update_error().


%%--------------------------------------------------------------------
%% @doc
%% Creates new #document.
%% @end
%%--------------------------------------------------------------------
-callback create(model_behaviour:model_config(), datastore:document()) -> {ok, datastore:ext_key()} | datastore:create_error().


%%--------------------------------------------------------------------
%% @doc
%% Updates given document by replacing given fields with new values or creates new one if not exists.
%% @end
%%--------------------------------------------------------------------
-callback create_or_update(model_behaviour:model_config(), datastore:document(),
    Diff :: datastore:document_diff()) -> {ok, datastore:ext_key()} | datastore:update_error().


%%--------------------------------------------------------------------
%% @doc
%% Gets #document with given key.
%% @end
%%--------------------------------------------------------------------
-callback get(model_behaviour:model_config(), datastore:ext_key()) -> {ok, datastore:document()} | datastore:get_error().


%%--------------------------------------------------------------------
%% @doc
%% Deletes #document with given key.
%% @end
%%--------------------------------------------------------------------
-callback delete(model_behaviour:model_config(), datastore:ext_key(), datastore:delete_predicate()) -> ok | datastore:generic_error().


%%--------------------------------------------------------------------
%% @doc
%% Checks if #document with given key exists.
%% @end
%%--------------------------------------------------------------------
-callback exists(model_behaviour:model_config(), datastore:ext_key()) -> {ok, boolean()} | datastore:generic_error().


%%--------------------------------------------------------------------
%% @doc
%% Traverses entire or part of table. Acts simililar to erlang:foldl except that it may be interrupted
%% by returning {abort, Acc} from given fun.
%% @end
%%--------------------------------------------------------------------
-callback list(model_behaviour:model_config(), Fun :: datastore:list_fun(), AccIn :: term()) ->
    {ok, Acc :: term()} | datastore:generic_error() | no_return().

%%--------------------------------------------------------------------
%% @doc
%% Checks driver state.
%% @end
%%--------------------------------------------------------------------
-callback healthcheck(WorkerState :: term()) -> ok | {error, Reason :: term()}.


%%--------------------------------------------------------------------
%% @doc
%% Adds given links to the document with given key.
%% @end
%%--------------------------------------------------------------------
-callback add_links(model_behaviour:model_config(), datastore:ext_key(), [datastore:normalized_link_spec()]) ->
    ok | datastore:generic_error() | no_return().


%%--------------------------------------------------------------------
%% @doc
%% Removes links from the document with given key. There is special link name 'all' which removes all links.
%% @end
%%--------------------------------------------------------------------
-callback delete_links(model_behaviour:model_config(), datastore:ext_key(), [datastore:link_name()] | all) ->
    ok | datastore:generic_error() | no_return().


%%--------------------------------------------------------------------
%% @doc
%% Gets specified link from the document given by key.
%% @end
%%--------------------------------------------------------------------
-callback fetch_link(model_behaviour:model_config(), datastore:ext_key(), datastore:link_name()) ->
    {ok, datastore:link_target()} | datastore:link_error() | no_return().


%%--------------------------------------------------------------------
%% @doc
%% "Walks" from link to link and fetches either all encountered documents (for Mode == get_all - not yet implemted),
%% or just last document (for Mode == get_leaf). Starts on the document given by key.
%% @end
%%--------------------------------------------------------------------
-callback foreach_link(model_behaviour:model_config(), Key :: datastore:ext_key(),
    fun((datastore:link_name(), datastore:link_target(), Acc :: term()) -> Acc :: term()), AccIn :: term()) ->
    {ok, Acc :: term()} | datastore:link_error() | no_return().

