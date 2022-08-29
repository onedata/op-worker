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
%%% TODO VFS-7919 - improve doc
%%% @end
%%%-------------------------------------------------------------------
-module(workflow_cached_async_result).
-author("Michal Wrzeszcz").

-include("workflow_engine.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([put/1, take/1, delete/1]).

-type id() :: datastore:key().
-type result_ref() :: id() | ?ERROR_MALFORMED_DATA | ?ERROR_TIMEOUT.

-export_type([result_ref/0]).

-define(CTX, #{
    model => ?MODULE,
    disc_driver => undefined
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec put(workflow_handler:async_processing_result()) -> result_ref().
put(?ERROR_MALFORMED_DATA) ->
    ?ERROR_MALFORMED_DATA;
put(?ERROR_TIMEOUT) ->
    ?ERROR_TIMEOUT;
put(ProcessingResult) ->
    Doc = #document{value = #workflow_cached_async_result{result = ProcessingResult}},
    {ok, #document{key = Id}} = datastore_model:save(?CTX, Doc),
    Id.

-spec take(result_ref()) -> workflow_handler:async_processing_result().
take(?ERROR_MALFORMED_DATA) ->
    ?ERROR_MALFORMED_DATA;
take(?ERROR_TIMEOUT) ->
    ?ERROR_TIMEOUT;
take(Id) ->
    {ok, #document{value = #workflow_cached_async_result{result = ProcessingResult}}} = datastore_model:get(?CTX, Id),
    ok = datastore_model:delete(?CTX, Id),
    ProcessingResult.

-spec delete(result_ref()) -> ok.
delete(?ERROR_MALFORMED_DATA) ->
    ?ERROR_MALFORMED_DATA;
delete(?ERROR_TIMEOUT) ->
    ?ERROR_TIMEOUT;
delete(Id) ->
    ok = datastore_model:delete(?CTX, Id).