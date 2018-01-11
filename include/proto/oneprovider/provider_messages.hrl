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
    target_provider_id :: oneprovider:id(),
    block :: #file_block{} | undefined,
    callback :: transfer:callback()
}).

-record(schedule_replica_invalidation, {
    source_provider_id :: oneprovider:id(),
    target_provider_id :: undefined | oneprovider:id()
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

-type provider_request_type() ::
#get_parent{} | #get_acl{} | #set_acl{} | #remove_acl{} |
#get_transfer_encoding{} | #set_transfer_encoding{} |
#get_cdmi_completion_status{} | #set_cdmi_completion_status{} |
#get_mimetype{} | #set_mimetype{} | #get_file_path{} |
#get_file_distribution{} | #schedule_file_replication{} | #schedule_replica_invalidation{} |
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
    share_id :: od_share:id(),
    share_file_guid :: od_share:share_guid()
}).

-record(scheduled_transfer, {
    transfer_id :: transfer:id()
}).

-type provider_response_type() ::
    #transfer_encoding{} | #cdmi_completion_status{} |#mimetype{} | #acl{} |
    #dir{} | #file_path{} | #file_distribution{} | #metadata{} | #share{} |
    #scheduled_transfer{} | undefined.

-record(provider_request, {
    context_guid :: fslogic_worker:file_guid(),
    provider_request :: provider_request_type()
}).

-record(provider_response, {
    status :: undefined | #status{},
    provider_response :: provider_response_type()
}).

-endif.
