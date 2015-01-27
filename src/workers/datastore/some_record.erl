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
-export([save/0, get/0, exists/0, delete/0, update/0, create/0, list/0, model_init/0, 'after'/0, before/0]).


save() ->
    erlang:error(not_implemented).

update() ->
    erlang:error(not_implemented).

create() ->
    erlang:error(not_implemented).

exists() ->
    erlang:error(not_implemented).

get() ->
    erlang:error(not_implemented).

delete() ->
    erlang:error(not_implemented).

list() ->
    erlang:error(not_implemented).

model_init() ->
    #model_config{name = ?MODULE, size = record_info(size, ?MODULE), fields = record_info(fields, ?MODULE), defaults = #?MODULE{},
        bucket = test_bucket, hooks = []}.

'after'() ->
    erlang:error(not_implemented).

before() ->
    erlang:error(not_implemented).
