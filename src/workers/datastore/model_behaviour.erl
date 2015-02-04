%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc @todo: Write me!
%%% @end
%%%-------------------------------------------------------------------
-module(model_behaviour).
-author("Rafal Slota").

-include("workers/datastore/datastore.hrl").

-type model_action() :: save | get | delete | update | create | exists.
-type model_type() :: atom().
-type model_config() :: #model_config{}.

-export_type([model_config/0]).


%%--------------------------------------------------------------------
%% @doc
%% Saves the document
%% @end
%%--------------------------------------------------------------------
-callback save(datastore:document()) -> {ok, datastore:key()} | datastore:generic_error().


%%--------------------------------------------------------------------
%% @doc
%% @todo: Write me!
%% @end
%%--------------------------------------------------------------------
-callback update(datastore:key(), Diff :: datastore:document_diff()) -> {ok, datastore:key()} | datastore:update_error().


%%--------------------------------------------------------------------
%% @doc
%% @todo: Write me!
%% @end
%%--------------------------------------------------------------------
-callback create(datastore:document()) -> {ok, datastore:key()} | datastore:create_error().


%%--------------------------------------------------------------------
%% @doc
%% @todo: Write me!
%% @end
%%--------------------------------------------------------------------
-callback get(datastore:document()) -> {ok, datastore:document()} | datastore:get_error().


%%--------------------------------------------------------------------
%% @doc
%% @todo: Write me!
%% @end
%%--------------------------------------------------------------------
-callback delete(datastore:key()) -> ok | datastore:generic_error().


%%--------------------------------------------------------------------
%% @doc
%% @todo: Write me!
%% @end
%%--------------------------------------------------------------------
-callback exists(datastore:key()) -> true | false | datastore:generic_error().



%%--------------------------------------------------------------------
%% @doc
%% @todo: Write me!
%% @end
%%--------------------------------------------------------------------
-callback model_init() -> model_config().


%%--------------------------------------------------------------------
%% @doc
%% @todo: Write me!
%% @end
%%--------------------------------------------------------------------
-callback 'after'(ModelName :: model_type(), Method :: model_action(),
                    Level :: datastore:store_level(), Context :: term(),
                    ReturnValue :: term()) -> ok.


%%--------------------------------------------------------------------
%% @doc
%% @todo: Write me!
%% @end
%%--------------------------------------------------------------------
-callback before(ModelName :: model_type(), Method :: model_action(),
                    Level :: datastore:store_level(), Context :: term()) -> ok | datastore:generic_error().