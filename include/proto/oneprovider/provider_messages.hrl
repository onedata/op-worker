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

-include("proto/oneclient/common_messages.hrl").
-include("modules/datastore/qos.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/posix/acl.hrl").

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

-record(schedule_file_replication, {
    % meaning of fields in this record is explained in datastore_models.hrl
    % in definition of transfer record
    target_provider_id :: oneprovider:id(),
    block :: undefined | #file_block{},
    callback :: transfer:callback(),
    view_name :: transfer:view_name(),
    query_view_params :: transfer:query_view_params()
}).

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
    names = [] :: custom_metadata:names(),
    inherited = false :: boolean()
}).

-record(set_metadata, {
    metadata :: custom_metadata:metadata(),
    names = [] :: custom_metadata:names()
}).

-record(remove_metadata, {
    type :: custom_metadata:type()
}).

-record(check_perms, {
    flag :: fslogic_worker:open_flag()
}).

-record(create_share, {
    name :: od_share:name()
}).

-record(remove_share, {
}).

% messages for adding, listing, getting and removing qos
-record(add_qos, {
    expression :: binary(),
    replicas_num :: qos_entry:replicas_num()
}).

-record(get_effective_file_qos, {
}).

-record(get_qos, {
    id :: qos_entry:id()
}).

-record(remove_qos, {
    id :: qos_entry:id()
}).

-record(check_qos_fulfillment, {
    qos_id :: qos_entry:id()
}).

-type provider_request_type() ::
#get_parent{} | #get_acl{} | #set_acl{} | #remove_acl{} |
#get_transfer_encoding{} | #set_transfer_encoding{} |
#get_cdmi_completion_status{} | #set_cdmi_completion_status{} |
#get_mimetype{} | #set_mimetype{} | #get_file_path{} |
#get_file_distribution{} | #schedule_file_replication{} | #schedule_replica_invalidation{} |
#get_metadata{} | #remove_metadata{} | #set_metadata{} | #check_perms{} |
#create_share{} | #remove_share{} |
#add_qos{} | #get_effective_file_qos{} | #get_qos{} | #remove_qos{} | #check_qos_fulfillment{}.

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
    share_id :: od_share:id(),
    root_file_guid :: od_share:root_file_guid()
}).

-record(scheduled_transfer, {
    transfer_id :: transfer:id()
}).

-record(qos_id, {
    id :: qos_entry:id()
}).

-record(get_qos_resp, {
    expression = [] :: qos_expression:expression(), % QoS expression in RPN form.
    replicas_num = 1 :: qos_entry:replicas_num(), % Number of required file replicas.
    is_possible :: boolean()
}).

-record(effective_file_qos, {
    qos_list = [] :: file_qos:qos_list(),
    target_storages = #{} :: file_qos:target_storages()
}).

-record(qos_fulfillment, {
    fulfilled :: boolean()
}).

-type provider_response_type() ::
    #transfer_encoding{} | #cdmi_completion_status{} |#mimetype{} | #acl{} |
    #dir{} | #file_path{} | #file_distribution{} | #metadata{} | #share{} |
    #scheduled_transfer{} | #qos_id{} | #get_qos_resp{} | #effective_file_qos{} |
    #qos_fulfillment{} | undefined.

-record(provider_request, {
    context_guid :: fslogic_worker:file_guid(),
    provider_request :: provider_request_type()
}).

-record(provider_response, {
    status :: undefined | #status{},
    provider_response :: provider_response_type()
}).

-endif.
