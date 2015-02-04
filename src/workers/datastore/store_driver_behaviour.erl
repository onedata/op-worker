%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Mnesia database driver.
%%% @end
%%%-------------------------------------------------------------------
-module(store_driver_behaviour).
-author("Rafal Slota").

-include("workers/datastore/datastore.hrl").


%%--------------------------------------------------------------------
%% @doc
%% Saves the document
%% @end
%%--------------------------------------------------------------------
-callback init_bucket(Bucket :: datastore:bucket(), Models :: [model_behaviour:model_config()]) -> ok.


%%--------------------------------------------------------------------
%% @doc
%% Saves the document
%% @end
%%--------------------------------------------------------------------
-callback save(model_behaviour:model_config(), datastore:document()) -> {ok, datastore:key()} | datastore:generic_error().


%%--------------------------------------------------------------------
%% @doc
%% @todo: Write me!
%% @end
%%--------------------------------------------------------------------
-callback update(model_behaviour:model_config(), datastore:key(),
                    Diff :: datastore:document_diff()) -> {ok, datastore:key()} | datastore:update_error().


%%--------------------------------------------------------------------
%% @doc
%% @todo: Write me!
%% @end
%%--------------------------------------------------------------------
-callback create(model_behaviour:model_config(), datastore:document()) -> {ok, datastore:key()} | datastore:create_error().


%%--------------------------------------------------------------------
%% @doc
%% @todo: Write me!
%% @end
%%--------------------------------------------------------------------
-callback get(model_behaviour:model_config(), datastore:document()) -> {ok, datastore:document()} | datastore:get_error().


%%--------------------------------------------------------------------
%% @doc
%% @todo: Write me!
%% @end
%%--------------------------------------------------------------------
-callback delete(model_behaviour:model_config(), datastore:key()) -> ok | datastore:generic_error().


%%--------------------------------------------------------------------
%% @doc
%% @todo: Write me!
%% @end
%%--------------------------------------------------------------------
-callback exists(model_behaviour:model_config(), datastore:key()) -> true | false | datastore:generic_error().