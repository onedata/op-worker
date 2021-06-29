%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Cache of results of jobs' processing. Results are processed on pool
%%% so the result is cached until any pool process is ready to process it.
%%% @end
%%%-------------------------------------------------------------------
-module(workflow_cached_async_result).
-author("Michal Wrzeszcz").

-include("workflow_engine.hrl").
-include("modules/datastore/datastore_models.hrl").

%% API
-export([put/1, get_and_delete/1]).

-type internal_id() :: binary().
-type id() :: internal_id() | ?WF_ERROR_MALFORMED_REQUEST | ?WF_ERROR_TIMEOUT.

-export_type([id/0]).

-define(CTX, #{
    model => ?MODULE,
    disc_driver => undefined
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec put(workflow_handler:task_processing_result()) -> id().
put(?WF_ERROR_MALFORMED_REQUEST) ->
    ?WF_ERROR_MALFORMED_REQUEST;
put(?WF_ERROR_TIMEOUT) ->
    ?WF_ERROR_TIMEOUT;
put(ProcessingResult) ->
    Doc = #document{value = #workflow_cached_async_result{result = ProcessingResult}},
    {ok, #document{key = Id}} = datastore_model:save(?CTX, Doc),
    Id.

-spec get_and_delete(id()) -> workflow_handler:task_processing_result().
get_and_delete(?WF_ERROR_MALFORMED_REQUEST) ->
    ?WF_ERROR_MALFORMED_REQUEST;
get_and_delete(?WF_ERROR_TIMEOUT) ->
    ?WF_ERROR_TIMEOUT;
get_and_delete(Id) ->
    {ok, #document{value = #workflow_cached_async_result{result = ProcessingResult}}} = datastore_model:get(?CTX, Id),
    ok = datastore_model:delete(?CTX, Id),
    ProcessingResult.