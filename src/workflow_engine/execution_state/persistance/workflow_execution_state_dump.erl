%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Stores progress information needed to restart workflow.
%%% @end
%%%-------------------------------------------------------------------
-module(workflow_execution_state_dump).
-author("Michal Wrzeszcz").


-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([dump_workflow_execution_state/1, restore_workflow_execution_state_from_dump/1, cleanup/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).


-define(CTX, #{
    model => ?MODULE
}).


%%%===================================================================
%%% API
%%%===================================================================

-spec dump_workflow_execution_state(workflow_engine:execution_id()) -> ok.
dump_workflow_execution_state(_) ->
    ok.


-spec restore_workflow_execution_state_from_dump(workflow_engine:execution_id()) ->
    ok | ?ERROR_NOT_FOUND.
restore_workflow_execution_state_from_dump(ExecutionId) ->
    case datastore_model:get(?CTX, ExecutionId) of
        {ok, #document{value = _Record}} -> ok;
        ?ERROR_NOT_FOUND -> ?ERROR_NOT_FOUND
    end.


-spec cleanup(workflow_engine:execution_id()) -> ok.
cleanup(ExecutionId) ->
    ok = datastore_model:delete(?CTX, ExecutionId).


%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.


-spec get_record_struct(datastore_model:record_version()) -> datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {iterator, {custom, json, {iterator, encode, decode}}},
        {lane_index, integer},
        {item_index, integer}
    ]}.
