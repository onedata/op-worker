%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions operating on automation workflow execution environment.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_env).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").

%% API
-export([get_store_id/2]).

-type record() :: #atm_workflow_execution_env{}.

-export_type([record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec get_store_id(automation:id(), record()) -> atm_store:id() | no_return().
get_store_id(AtmStoreSchemaId, #atm_workflow_execution_env{
    store_registry = AtmStoreRegistry
}) ->
    case maps:get(AtmStoreSchemaId, AtmStoreRegistry, undefined) of
        undefined ->
            throw(?ERROR_ATM_REFERENCED_NONEXISTENT_STORE(AtmStoreSchemaId));
        AtmStoreId ->
            AtmStoreId
    end.
