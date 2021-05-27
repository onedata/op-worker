%%%-------------------------------------------------------------------
%%% @author Mateusz Paciorek
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Messages used in communication between servers and within server.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(PROVIDER_MESSAGES_HRL).
-define(PROVIDER_MESSAGES_HRL, 1).

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/qos.hrl").
-include("modules/fslogic/acl.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("modules/fslogic/file_attr.hrl").
-include("modules/dataset/dataset.hrl").

-record(get_parent, {
}).

-record(acl, {
    value :: [#access_control_entity{}]
}).

-record(get_acl, {
}).

-record(set_acl, {
    acl :: #acl{}
}).

-record(remove_acl, {
}).

-record(get_transfer_encoding, {
}).

-record(set_transfer_encoding, {
    value :: binary()
}).

-record(get_cdmi_completion_status, {
}).

-record(set_cdmi_completion_status, {
    value :: binary()
}).

-record(get_mimetype, {
}).

-record(set_mimetype, {
    value :: binary()
}).

-record(get_file_path, {
}).

-record(get_file_distribution, {
}).

-record(schedule_file_transfer, {
    % meaning of fields in this record is explained in datastore_models.hrl
    % in definition of transfer record
    replicating_provider_id :: undefined | oneprovider:id(),
    evicting_provider_id :: undefined | oneprovider:id(),
    callback :: transfer:callback()
}).

-record(schedule_view_transfer, {
    % meaning of fields in this record is explained in datastore_models.hrl
    % in definition of transfer record
    replicating_provider_id :: undefined | oneprovider:id(),
    evicting_provider_id :: undefined | oneprovider:id(),
    view_name :: transfer:view_name(),
    query_view_params :: transfer:query_view_params(),
    callback :: transfer:callback()
}).

%% TODO VFS-6365 remove deprecated replicas endpoints
-record(schedule_file_replication, {
    % meaning of fields in this record is explained in datastore_models.hrl
    % in definition of transfer record
    target_provider_id :: oneprovider:id(),
    block :: undefined | #file_block{},
    callback :: transfer:callback(),
    view_name :: transfer:view_name(),
    query_view_params :: transfer:query_view_params()
}).

%% TODO VFS-6365 remove deprecated replicas endpoints
-record(schedule_replica_invalidation, {
    % meaning of fields in this record is explained in datastore_models.hrl
    % in definition of transfer record
    source_provider_id :: oneprovider:id(),
    target_provider_id :: undefined | oneprovider:id(),
    view_name :: transfer:view_name(),
    query_view_params :: transfer:query_view_params()
}).

-record(query_view_params, {
    params :: proplists:proplist()
}).

-record(get_metadata, {
    type :: custom_metadata:type(),
    query = [] :: custom_metadata:query(),
    inherited = false :: boolean()
}).

-record(set_metadata, {
    metadata :: custom_metadata:metadata(),
    query = [] :: custom_metadata:query()
}).

-record(remove_metadata, {
    type :: custom_metadata:type()
}).

-record(check_perms, {
    flag :: fslogic_worker:open_flag()
}).

-record(create_share, {
    name :: od_share:name(),
    description :: od_share:description()
}).

-record(remove_share, {
    share_id :: od_share:id()
}).

% messages for QoS management
-record(add_qos_entry, {
    expression :: qos_expression:expression(),
    replicas_num :: qos_entry:replicas_num(),
    entry_type = user_defined :: qos_entry:type()
}).

-record(get_qos_entry, {
    id :: qos_entry:id()
}).

-record(remove_qos_entry, {
    id :: qos_entry:id()
}).

-record(get_effective_file_qos, {
}).

-record(check_qos_status, {
    qos_id :: qos_entry:id()
}).

-record(establish_dataset, {
    protection_flags = ?no_flags_mask :: data_access_control:bitmask()
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

-record(get_dataset_info, {
    id :: dataset:id()
}).

-record(get_file_eff_dataset_summary, {
}).

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

-record(archive_dataset, {
    id :: dataset:id(),
    config :: archive:config(),
    preserved_callback :: archive:callback(),
    purged_callback :: archive:callback(),
    description :: archive:description()
}).

-record(update_archive, {
    id :: archive:id(),
    description :: archive:description() | undefined,
    diff :: archive:diff()
}).

-record(get_archive_info, {
    id :: archive:id()
}).

-record(list_archives, {
    dataset_id :: dataset:id(),
    opts :: archives_list:opts(),
    mode = ?BASIC_INFO :: archive_api:listing_mode()
}).

-record(init_archive_purge, {
    id :: archive:id(),
    callback :: archive:callback()
}).


-type provider_request_type() ::
    #get_parent{} | #get_acl{} | #set_acl{} | #remove_acl{} |
    #get_transfer_encoding{} | #set_transfer_encoding{} |
    #get_cdmi_completion_status{} | #set_cdmi_completion_status{} |
    #get_mimetype{} | #set_mimetype{} | #get_file_path{} |
    #get_file_distribution{} |
    #schedule_file_transfer{} | #schedule_view_transfer{} |
    #schedule_file_replication{} | #schedule_replica_invalidation{} |
    #get_metadata{} | #remove_metadata{} | #set_metadata{} | #check_perms{} |
    #create_share{} | #remove_share{} |
    #add_qos_entry{} | #get_effective_file_qos{} | #get_qos_entry{} | #remove_qos_entry{} | #check_qos_status{} |
    #establish_dataset{} | #update_dataset{} | #remove_dataset{} |
    #get_dataset_info{} | #get_file_eff_dataset_summary{} | #list_top_datasets{} | #list_children_datasets{} |
    #archive_dataset{} | #update_archive{} | #get_archive_info{} | #list_archives{} | #init_archive_purge{}.

-record(transfer_encoding, {
    value :: binary()
}).

-record(cdmi_completion_status, {
    value :: binary()
}).

-record(mimetype, {
    value :: binary()
}).

-record(file_path, {
    value :: binary()
}).

-record(provider_file_distribution, {
    provider_id :: oneprovider:id(),
    blocks :: [#file_block{}]
}).

-record(file_distribution, {
    provider_file_distributions :: [#provider_file_distribution{}]
}).

-record(metadata, {
    type :: custom_metadata:type(),
    value :: term()
}).

-record(share, {
    share_id :: od_share:id()
}).

-record(scheduled_transfer, {
    transfer_id :: transfer:id()
}).

-record(qos_entry_id, {
    id :: qos_entry:id()
}).

-record(qos_status_response, {
    status :: qos_status:summary()
}).

-record(eff_qos_response, {
    entries_with_status = #{} :: #{qos_entry:id() => qos_status:summary()},
    assigned_entries = #{} :: file_qos:assigned_entries()
}).

-record(dataset_established, {
    id :: dataset:id()
}).

-record(dataset_info, {
    id :: dataset:id(),
    state :: dataset:state(),
    root_file_guid :: fslogic_worker:file_guid(),
    root_file_path :: file_meta:path(),
    root_file_type :: file_meta:type(),
    creation_time :: time:seconds(),
    protection_flags = ?no_flags_mask :: data_access_control:bitmask(),
    eff_protection_flags = ?no_flags_mask :: data_access_control:bitmask(),
    parent :: undefined | dataset:id(),
    archive_count = 0 :: non_neg_integer(),
    index :: dataset_api:index()
}).

-record(file_eff_dataset_summary, {
    direct_dataset :: dataset:id(),
    eff_ancestor_datasets :: [dataset:id()],
    eff_protection_flags = ?no_flags_mask :: data_access_control:bitmask()
}).

-record(datasets, {
   datasets = [] :: dataset_api:entries(),
   is_last :: boolean()
}).

-record(dataset_archived, {
    id :: archive:id()
}).

-record(archive_info, {
    id :: archive:id(),
    dataset_id :: dataset:id(),
    state :: archive:state(),
    root_file_guid :: undefined | file_id:file_guid(),
    creation_time :: time:millis(),
    config :: archive:config(),
    preserved_callback :: archive:callback(),
    purged_callback :: archive:callback(),
    description :: archive:description(),
    index :: archive_api:index(),
    stats :: archive_stats:stats()
}).

-record(archives, {
    archives = [] :: archive_api:entries(),
    is_last :: boolean()
}).

-type provider_response_type() ::
    #transfer_encoding{} | #cdmi_completion_status{} | #mimetype{} | #acl{} |
    #dir{} | #file_path{} | #file_distribution{} | #metadata{} | #share{} | #scheduled_transfer{} |
    #qos_entry_id{} | #qos_entry{} | #eff_qos_response{} | #qos_status_response{} |
    #dataset_established{} | #dataset_info{} | #file_eff_dataset_summary{} | #datasets{}|
    #dataset_archived{} | #archive_info{} | #archives{} |
    undefined.

-record(provider_request, {
    context_guid :: fslogic_worker:file_guid(),
    provider_request :: provider_request_type()
}).

-record(provider_response, {
    status :: undefined | #status{},
    provider_response :: provider_response_type()
}).

-define(PROVIDER_OK_RESP, ?PROVIDER_OK_RESP(undefined)).
-define(PROVIDER_OK_RESP(__RESPONSE), #provider_response{
    status = #status{code = ?OK},
    provider_response = __RESPONSE
}).

-endif.
