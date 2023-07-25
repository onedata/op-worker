%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model storing automation lambda snapshot.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_lambda_snapshot).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").
-include("modules/datastore/datastore_runner.hrl").

%% API
-export([create/2, get/1, get_revision/2, delete/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_version/0, get_record_struct/1]).


-type id() :: binary().
-type record() :: #atm_lambda_snapshot{}.
-type doc() :: datastore_doc:doc(record()).

-export_type([id/0, record/0, doc/0]).


-define(CTX, #{model => ?MODULE}).


%%%===================================================================
%%% API functions
%%%===================================================================


-spec create(atm_workflow_execution:id(), od_atm_lambda:doc()) ->
    {ok, id()} | {error, term()}.
create(AtmWorkflowExecutionId, #document{key = AtmLambdaId, value = #od_atm_lambda{
    revision_registry = RevisionRegistry,
    atm_inventories = AtmInventories
}}) ->
    %% TODO VFS-7685 add ref count and gen snapshot id based on doc revision and lambda revisions
    %% (not used in given workflow execution lambda revisions are removed from revision registry)
    ?extract_key(datastore_model:create(?CTX, #document{
        key = datastore_key:new_from_digest([AtmWorkflowExecutionId, AtmLambdaId]),
        value = #atm_lambda_snapshot{
            lambda_id = AtmLambdaId,
            revision_registry = RevisionRegistry,
            atm_inventories = AtmInventories
        }
    })).


-spec get(id()) -> {ok, doc()} | {error, term()}.
get(AtmLambdaSnapshotId) ->
    datastore_model:get(?CTX, AtmLambdaSnapshotId).


-spec get_revision(atm_lambda_revision:revision_number(), record()) ->
    atm_lambda_revision:record().
get_revision(RevisionNum, #atm_lambda_snapshot{revision_registry = RevisionRegistry}) ->
    atm_lambda_revision_registry:get_revision(RevisionNum, RevisionRegistry).


-spec delete(id()) -> ok | {error, term()}.
delete(AtmLambdaSnapshotId) ->
    datastore_model:delete(?CTX, AtmLambdaSnapshotId).


%%%===================================================================
%%% Datastore callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.


%%--------------------------------------------------------------------
%% @doc
%% Returns model's record version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    1.


%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) -> datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {lambda_id, string},
        {revision_registry, {custom, string, {persistent_record, to_string, from_string, atm_lambda_revision_registry}}},
        {atm_inventories, [string]}
    ]}.
