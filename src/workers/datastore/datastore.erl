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
-module(datastore).
-author("Rafal Slota").

-include("workers/datastore/datastore.hrl").

-type key() :: term().
-type value() :: #document{}.

%% API
-export_type([key/0, value/0]).
-export([save/2, update/2, create/2, get/3, delete/3, exists/3]).


%%--------------------------------------------------------------------
%% @doc
%% @todo: Write me!
%% @end
%%--------------------------------------------------------------------
save(Level, #document{} = Document) ->
    ModelName = model_name(Document),
    ModelConfig = ModelName:model_init(),
    distributed_cache_driver:save(ModelConfig, Document).


%%--------------------------------------------------------------------
%% @doc
%% @todo: Write me!
%% @end
%%--------------------------------------------------------------------
update(Level, #document{} = Document) ->
    ModelName = model_name(Document),
    ModelConfig = ModelName:model_init(),
    distributed_cache_driver:update(ModelConfig, Document).


%%--------------------------------------------------------------------
%% @doc
%% @todo: Write me!
%% @end
%%--------------------------------------------------------------------
create(Level, #document{} = Document) ->
    ModelName = model_name(Document),
    ModelConfig = ModelName:model_init(),
    distributed_cache_driver:create(ModelConfig, Document).


%%--------------------------------------------------------------------
%% @doc
%% @todo: Write me!
%% @end
%%--------------------------------------------------------------------
get(Level, ModelName, Key) ->
    ModelConfig = ModelName:model_init(),
    distributed_cache_driver:get(ModelConfig, Key).


%%--------------------------------------------------------------------
%% @doc
%% @todo: Write me!
%% @end
%%--------------------------------------------------------------------
delete(Level, ModelName, Key) ->
    ModelConfig = ModelName:model_init(),
    distributed_cache_driver:delete(ModelConfig, Key).


%%--------------------------------------------------------------------
%% @doc
%% @todo: Write me!
%% @end
%%--------------------------------------------------------------------
exists(Level, ModelName, Key) ->
    ModelConfig = ModelName:model_init(),
    distributed_cache_driver:exists(ModelConfig, Key).


%%--------------------------------------------------------------------
%% @doc
%% @todo: Write me!
%% @end
%%--------------------------------------------------------------------
list() -> ok.


model_name(#document{value = Record}) ->
    model_name(Record);
model_name(Record) when is_tuple(Record) ->
    element(1, Record).


run_prehooks(#model_config{}, Method, Context) ->
    ok.

run_posthooks(#model_config{}, Method, Context, Return) ->
    spawn(fun() -> ok end).