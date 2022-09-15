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

-record(check_perms, {
    flag :: fslogic_worker:open_flag()
}).

-record(get_file_path, {
}).

-record(get_recursive_file_list, {
    listing_options :: recursive_file_listing:options(),
    optional_attrs = [] :: [attr_req:optional_attr()]
}).

-type provider_request_type() ::
    #get_parent{} |
    #get_acl{} | #set_acl{} | #remove_acl{} | #check_perms{} |
    #get_file_path{} | #get_recursive_file_list{}.


-record(file_path, {
    value :: binary()
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
    direct_dataset :: dataset:id() | undefined,
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
    deleted_callback :: archive:callback(),
    description :: archive:description(),
    index :: archive_api:index(),
    stats :: archive_stats:record(),
    parent_archive_id :: undefined | archive:id(),
    base_archive_id :: undefined | archive:id(),
    related_aip_id :: undefined | archive:id(),
    related_dip_id :: undefined | archive:id()
}).

-record(recursive_listing_result, {
    entries :: [any()], % [recursive_listing:result_entry()] but dialyzer does not accept it
    inaccessible_paths :: [any()], % [recursive_listing:node_path()] but dialyzer does not accept it
    pagination_token :: undefined | recursive_listing:pagination_token()
}).

-type provider_response_type() ::
    #acl{} | #dir{} | #file_path{}| #dataset_info{} | #file_eff_dataset_summary{} |
    #archive_info{} | #recursive_listing_result{} | undefined.

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
