%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Module operating on collection of discarded automation workflow executions.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_discarded_workflow_executions).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").

%% API
-export([list/2, add/1, delete/1]).

-define(CTX, (atm_workflow_execution:get_ctx())).

-define(FOREST, <<"ATM_DISCARDED_WORKFLOW_EXECUTIONS">>).
-define(TREE_ID, <<"LONE_TREE">>).


%%%===================================================================
%%% API
%%%===================================================================


-spec list(atm_workflow_execution:id(), pos_integer()) ->
    [atm_workflow_execution:id()].
list(StartAtmWorkflowExecutionId, Limit) ->
    ListingOpts = #{
        prev_link_name => StartAtmWorkflowExecutionId,
        size => Limit
    },
    FoldFun = fun(#link{name = AtmWorkflowExecutionId}, Acc) ->
        {ok, [AtmWorkflowExecutionId | Acc]}
    end,
    {ok, AtmWorkflowExecutionIds} = datastore_model:fold_links(
        ?CTX, ?FOREST, ?TREE_ID, FoldFun, [], ListingOpts
    ),
    lists:reverse(AtmWorkflowExecutionIds).


-spec add(atm_workflow_execution:id()) -> ok.
add(AtmWorkflowExecutionId) ->
    Link = {AtmWorkflowExecutionId, <<>>},

    case datastore_model:add_links(?CTX, ?FOREST, ?TREE_ID, Link) of
        {ok, _} -> ok;
        {error, already_exists} -> ok
    end.


-spec delete(atm_workflow_execution:id()) -> ok.
delete(AtmWorkflowExecutionId) ->
    ok = datastore_model:delete_links(?CTX, ?FOREST, ?TREE_ID, AtmWorkflowExecutionId).
