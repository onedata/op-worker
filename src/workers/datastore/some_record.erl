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
-module(some_record).
-author("Rafal Slota").
-behaviour(model_behaviour).

-include("workers/datastore/datastore.hrl").


%% API
-export([save/1, get/1, exists/1, delete/1, update/2, create/1, list/0, model_init/0, 'after'/5, before/4]).


save(Document) ->
    datastore:save(all, Document).

update(Key, Diff) ->
    datastore:update(all, ?MODULE, Key, Diff).

create(Document) ->
    datastore:create(all, Document).

exists(Key) ->
    datastore:exists(l_cache, ?MODULE, Key).

get(Key) ->
    datastore:get(l_cache, ?MODULE, Key).

delete(Key) ->
    datastore:delete(l_cache, ?MODULE, Key).

list() ->
    erlang:error(not_implemented).

model_init() ->
    #model_config{
        name = ?MODULE, size = record_info(size, ?MODULE), fields = record_info(fields, ?MODULE), defaults = #?MODULE{},
        bucket = test_bucket, hooks = [{some_record, update}]}.

'after'(ModelName, Method, Level, Context, Return) ->
    ok.

before(ModelName, Method, Level, Context) ->
    ok.
