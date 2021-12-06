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

-include("modules/dataset/dataset.hrl").
-include("modules/fslogic/acl.hrl").
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


-define(throw_on_error(__EXPR), middleware_utils:throw_if_error(__EXPR)).


%%%===================================================================
%%% Available operations in middleware_worker
%%%===================================================================

%% archives related

-record(list_archives, {
    dataset_id :: dataset:id(),
    opts :: archives_list:opts(),
    mode = ?BASIC_INFO :: archive_api:listing_mode()
}).

-record(archive_dataset, {
    id :: dataset:id(),
    config :: archive:config(),
    preserved_callback :: archive:callback(),
    purged_callback :: archive:callback(),
    description :: archive:description()
}).

-record(get_archive_info, {
    id :: archive:id()
}).

-record(update_archive, {
    id :: archive:id(),
    description :: archive:description() | undefined,
    diff :: archive:diff()
}).

-record(init_archive_purge, {
    id :: archive:id(),
    callback :: archive:callback()
}).

%% automation related

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

%% datasets related

-record(list_top_datasets, {
    state :: dataset:state(),
    opts :: dataset_api:listing_opts(),
    mode = ?BASIC_INFO :: dataset_api:listing_mode()
}).

-record(list_children_datasets, {
    id :: dataset:id(),
    opts :: dataset_api:listing_opts(),
    mode = ?BASIC_INFO :: dataset_api:listing_mode()
}).

-record(establish_dataset, {
    protection_flags = ?no_flags_mask :: data_access_control:bitmask()
}).

-record(get_dataset_info, {
    id :: dataset:id()
}).

-record(update_dataset, {
    id :: dataset:id(),
    state :: undefined | dataset:state(),
    flags_to_set = ?no_flags_mask :: data_access_control:bitmask(),
    flags_to_unset = ?no_flags_mask :: data_access_control:bitmask()
}).

-record(remove_dataset, {
    id :: dataset:id()
}).

-record(get_file_eff_dataset_summary, {}).


-endif.
