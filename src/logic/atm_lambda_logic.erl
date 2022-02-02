%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Interface for manipulating automation lambdas via Graph Sync.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_lambda_logic).
-author("Michal Stanisz").

-include("graph_sync/provider_graph_sync.hrl").
-include("middleware/middleware.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/automation/automation.hrl").
-include_lib("ctool/include/errors.hrl").

-export([get/2]).
-export([assert_executable_revisions/2]).


%%%===================================================================
%%% API
%%%===================================================================

-spec get(gs_client_worker:client(), od_atm_lambda:id()) ->
    {ok, od_atm_lambda:doc()} | errors:error().
get(SessionId, AtmLambdaId) ->
    gs_client_worker:request(SessionId, #gs_req_graph{
        operation = get,
        gri = #gri{type = od_atm_lambda, id = AtmLambdaId, aspect = instance, scope = private},
        subscribe = true
    }).


%%-------------------------------------------------------------------
%% @doc
%% Checks whether given atm lambda revisions can be executed (not all valid
%% features may be supported by this provider - e.g. OpenFaaS service may
%% not be configured).
%% @end
%%-------------------------------------------------------------------
-spec assert_executable_revisions(
    [atm_lambda_revision:revision_number()],
    od_atm_lambda:record() | od_atm_lambda:doc()
) ->
    ok | no_return().
assert_executable_revisions(RevisionNums, #od_atm_lambda{revision_registry = RevisionRegistry}) ->
    lists:foreach(fun(RevisionNum) ->
        case atm_lambda_revision_registry:has_revision(RevisionNum, RevisionRegistry) of
            true ->
                assert_executable_revision(atm_lambda_revision_registry:get_revision(
                    RevisionNum, RevisionRegistry
                ));
            false ->
                throw(?ERROR_NOT_FOUND)
        end
    end, RevisionNums);

assert_executable_revisions(RevisionNums, #document{value = AtmLambda}) ->
    assert_executable_revisions(RevisionNums, AtmLambda).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec assert_executable_revision(atm_lambda_revision:record()) ->
    ok | no_return().
assert_executable_revision(#atm_lambda_revision{operation_spec = #atm_openfaas_operation_spec{}}) ->
    atm_openfaas_task_executor:assert_openfaas_available().
