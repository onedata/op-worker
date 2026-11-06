%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Cache of workflow_engine:streamed_task_data().
%%% @end
%%%-------------------------------------------------------------------
-module(workflow_cached_task_data).
-author("Michal Wrzeszcz").

-include("modules/datastore/datastore_models.hrl").

%% API
-export([put/1, take/1, delete/1]).

-type id() :: datastore:key().
-export_type([id/0]).


-define(CTX, #{
    model => ?MODULE,
    disc_driver => undefined
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec put(workflow_engine:streamed_task_data()) -> id().
put(Data) ->
    Doc = #document{value = #workflow_cached_task_data{data = Data}},
    {ok, #document{key = Id}} = datastore_model:save(?CTX, Doc),
    Id.


-spec take(id()) -> workflow_engine:streamed_task_data().
take(Id) ->
    {ok, #document{value = #workflow_cached_task_data{data = Data}}} = datastore_model:get(?CTX, Id),
    delete(Id),
    Data.


-spec delete(id()) -> ok.
delete(Id) ->
    ok = datastore_model:delete(?CTX, Id).