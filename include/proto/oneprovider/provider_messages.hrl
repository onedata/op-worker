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

-type provider_request_type() ::
    #get_parent{} | #get_acl{} | #set_acl{} | #remove_acl{} |
    #get_transfer_encoding{} | #set_transfer_encoding{} |
    #get_cdmi_completion_status{} | #set_cdmi_completion_status{} |
    #get_mimetype{} | #set_mimetype{} | #get_file_path{} |
    #get_file_distribution{} |
    #schedule_file_replication{} | #schedule_replica_invalidation{} |
    #get_metadata{} | #remove_metadata{} | #set_metadata{} | #check_perms{} |
    #create_share{} | #remove_share{}.

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

-record(dataset_info, {
    id :: dataset:id(),
    state :: dataset:state(),
    root_file_guid :: fslogic_worker:file_guid(),
    root_file_path :: file_meta:path(),
    root_file_type :: file_meta:type(),
    root_file_deleted = false :: boolean(),
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

-record(archive_info, {
    id :: archive:id(),
    dataset_id :: dataset:id(),
    state :: archive:state(),
    root_dir_guid :: undefined | file_id:file_guid(),
    creation_time :: time:millis(),
    config :: archive:config(),
    preserved_callback :: archive:callback(),
    purged_callback :: archive:callback(),
    description :: archive:description(),
    index :: archive_api:index(),
    stats :: archive_stats:record(),
    base_archive_id :: archive:id() | undefined,
    related_aip :: undefined | archive:id(),
    related_dip :: undefined | archive:id()
}).

-type provider_response_type() ::
    #transfer_encoding{} | #cdmi_completion_status{} | #mimetype{} | #acl{} |
    #dir{} | #file_path{} | #file_distribution{} | #metadata{} | #share{} | #scheduled_transfer{} |
    #dataset_info{} | #file_eff_dataset_summary{} | #archive_info{} |
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
