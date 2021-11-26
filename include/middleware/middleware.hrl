%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% TODO VFS-5621
%%% Common definitions concerning middleware.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(MIDDLEWARE_HRL).
-define(MIDDLEWARE_HRL, 1).

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("cluster_worker/include/graph_sync/graph_sync.hrl").


% Record expressing middleware request
-record(op_req, {
    auth = ?GUEST :: aai:auth(),
    gri :: gri:gri(),
    operation = create :: middleware:operation(),
    data = #{} :: middleware:data(),
    auth_hint = undefined :: undefined | middleware:auth_hint(),
    % applicable for create/get requests - returns the revision of resource
    return_revision = false :: boolean()
}).


-define(throw_on_error(Res), case Res of
    {error, _} = Error -> throw(Error);
    _ -> Res
end).


%%%===================================================================
%%% Available operations in middleware_worker
%%%===================================================================


-record(schedule_atm_workflow_execution, {
    atm_workflow_schema_id :: od_atm_workflow_schema:id(),
    atm_workflow_schema_revision_num :: atm_workflow_schema_revision:revision_number(),
    store_initial_values :: atm_workflow_execution_api:store_initial_values(),
    callback_url :: undefined | http_client:url()
}).

-record(cancel_atm_workflow_execution, {
    atm_workflow_execution_id :: atm_workflow_execution:id()
}).

-record(repeat_atm_workflow_execution, {
    type :: atm_workflow_execution:repeat_type(),
    atm_workflow_execution_id :: atm_workflow_execution:id(),
    atm_lane_run_selector :: atm_lane_execution:lane_run_selector()
}).


-endif.
