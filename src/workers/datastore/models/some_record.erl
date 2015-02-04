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
-export([save/1, get/1, exists/1, delete/1, update/2, create/1, model_init/0, 'after'/5, before/4]).


%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback save/1. <br/>
%% @end
%%--------------------------------------------------------------------
save(Document) ->
    datastore:save(all, Document).


%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2. <br/>
%% @end
%%--------------------------------------------------------------------
update(Key, Diff) ->
    datastore:update(all, ?MODULE, Key, Diff).


%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1. <br/>
%% @end
%%--------------------------------------------------------------------
create(Document) ->
    datastore:create(all, Document).


%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1. <br/>
%% @end
%%--------------------------------------------------------------------
exists(Key) ->
    datastore:exists(l_cache, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1. <br/>
%% @end
%%--------------------------------------------------------------------
get(Key) ->
    datastore:get(l_cache, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1. <br/>
%% @end
%%--------------------------------------------------------------------
delete(Key) ->
    datastore:delete(l_cache, ?MODULE, Key).


%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0. <br/>
%% @end
%%--------------------------------------------------------------------
model_init() ->
        ?MODEL_CONFIG(test_bucket, [{some_record, update}]).


%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback 'after'/5. <br/>
%% @end
%%--------------------------------------------------------------------
'after'(_ModelName, _Method, _Level, _Context, _Return) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback before/4. <br/>
%% @end
%%--------------------------------------------------------------------
before(_ModelName, _Method, _Level, _Context) ->
    ok.
