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

-record(get_parent, {
    uuid :: file_meta:uuid() | fslogic_worker:file_guid()
}).

-record(get_xattr, {
    uuid :: file_meta:uuid() | fslogic_worker:file_guid(),
    name :: xattr:name()
}).

-record(set_xattr, {
    uuid :: file_meta:uuid() | fslogic_worker:file_guid(),
    xattr :: #xattr{}
}).

-record(remove_xattr, {
    uuid :: file_meta:uuid() | fslogic_worker:file_guid(),
    name :: xattr:name()
}).

-record(list_xattr, {
    uuid :: file_meta:uuid() | fslogic_worker:file_guid()
}).

-record(acl, {
    value :: binary()
}).

-record(get_acl, {
    uuid :: file_meta:uuid() | fslogic_worker:file_guid()
}).

-record(set_acl, {
    uuid :: file_meta:uuid() | fslogic_worker:file_guid(),
    acl :: #acl{}
}).

-record(remove_acl, {
    uuid :: file_meta:uuid() | fslogic_worker:file_guid()
}).

-record(get_transfer_encoding, {
    uuid :: file_meta:uuid() | fslogic_worker:file_guid()
}).

-record(set_transfer_encoding, {
    uuid :: file_meta:uuid() | fslogic_worker:file_guid(),
    value :: binary()
}).

-record(get_cdmi_completion_status, {
    uuid :: file_meta:uuid() | fslogic_worker:file_guid()
}).

-record(set_cdmi_completion_status, {
    uuid :: file_meta:uuid() | fslogic_worker:file_guid(),
    value :: binary()
}).

-record(get_mimetype, {
    uuid :: file_meta:uuid() | fslogic_worker:file_guid()
}).

-record(set_mimetype, {
    uuid :: file_meta:uuid() | fslogic_worker:file_guid(),
    value :: binary()
}).

-record(get_file_path, {
    uuid :: fslogic_worker:file_guid()
}).

-record(fsync, {
    uuid :: fslogic_worker:file_guid()
}).

-record(get_file_distribution, {
    uuid :: fslogic_worker:file_guid()
}).

-record(replicate_file, {
    uuid :: fslogic_worker:file_guid(),
    provider_id :: oneprovider:id(),
    block :: #file_block{} | undefined
}).

-record(get_metadata, {
    uuid :: file_meta:uuid(),
    type :: custom_metadata:type(),
    names = [] :: custom_metadata:names()
}).

-record(set_metadata, {
    uuid :: file_meta:uuid(),
    metadata :: custom_metadata:metadata(),
    names = [] :: custom_metadata:names()
}).

-type provider_request() ::
    #get_parent{} | #get_xattr{} | #set_xattr{} | #remove_xattr{} |
    #list_xattr{} | #get_acl{} | #set_acl{} | #remove_acl{} |
    #get_transfer_encoding{} | #set_transfer_encoding{} |
    #get_cdmi_completion_status{} | #set_cdmi_completion_status{} |
    #get_mimetype{} | #set_mimetype{} | #get_file_path{} | #fsync{} |
    #get_file_distribution{} | #replicate_file{} | #get_metadata{} |
    #set_metadata{}.

-record(xattr_list, {
    names :: [xattr:name()]
}).

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

-type provider_response() ::
    #xattr{} | #xattr_list{} | #transfer_encoding{} | #cdmi_completion_status{} |
    #mimetype{} | #acl{} | #dir{} | #file_path{} | #file_distribution{} |
    #metadata{}.

-record(provider_request, {
    provider_request :: provider_request()
}).

-record(provider_response, {
    status :: #status{},
    provider_response :: provider_response()
}).

-endif.
