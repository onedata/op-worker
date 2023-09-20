%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% File attributes record definition.
%%% @end
%%%--------------------------------------------------------------------
-ifndef(FILE_ATTR_HRL).
-define(FILE_ATTR_HRL, 1).

%% File types
-define(REGULAR_FILE_TYPE, 'REG').
-define(DIRECTORY_TYPE, 'DIR').
-define(LINK_TYPE, 'LNK'). % hard link
-define(SYMLINK_TYPE, 'SYMLNK'). % symbolic link


-record(file_attr, {
    guid :: undefined | fslogic_worker:file_guid(),
    name :: undefined | file_meta:name(),
    mode :: undefined | file_meta:mode(),
    parent_guid :: undefined | fslogic_worker:file_guid(),
    uid :: undefined | non_neg_integer(),
    gid :: undefined | non_neg_integer(),
    atime :: undefined | times:a_time(),
    mtime :: undefined | times:m_time(),
    ctime :: undefined | times:c_time(),
    type :: undefined | file_attr:file_type(),
    size :: undefined | file_meta:size(),
    shares :: undefined | [od_share:id()],
    provider_id :: undefined | od_provider:id(),
    owner_id :: undefined | od_user:id(),
    is_fully_replicated :: undefined | boolean(),
    link_count :: undefined | non_neg_integer(),
    % Listing index can be used to list parent dir children starting from this file
    index :: undefined | file_listing:index(),
    xattrs :: undefined | #{custom_metadata:name() => custom_metadata:value()},
    active_permissions_type :: undefined | file_meta:permissions_type(),
    symlink_value :: undefined | file_meta_symlinks:symlink(),
    conflicting_name :: undefined | file_meta:name(),
    local_replication_rate :: undefined | float(),
    recall_root_id :: undefined | file_id:file_guid(),
    eff_protection_flags :: undefined | data_access_control:bitmask(),
    eff_dataset_protection_flags :: undefined | data_access_control:bitmask(),
    eff_dataset_membership :: undefined | dataset:membership(),
    eff_qos_membership :: undefined | file_qos:membership(),
    qos_status :: undefined | qos_status:summary(),
    has_metadata :: undefined | boolean(),
    is_deleted :: undefined | boolean(),
    conflicting_files :: undefined | file_meta:conflicts(),
    archive_id :: undefined | archive:id(),
    path :: undefined | file_meta:path()
}).


-define(IMPLICIT_ATTRS, [guid]).
-define(FILE_META_ATTRS, [active_permissions_type, index, mode, owner_id, parent_guid,
    provider_id, shares, symlink_value, type, link_count, is_deleted]).
-define(LINKS_ATTRS, [name, conflicting_name, conflicting_files]).
-define(TIMES_ATTRS, [atime, mtime, ctime]).
-define(LOCATION_ATTRS, [size, local_replication_rate, is_fully_replicated]).
-define(LUMA_ATTRS, [gid, uid]).
-define(ARCHIVE_RECALL_ATTRS, [recall_root_id]).
-define(DATASET_ATTRS, [eff_dataset_membership, eff_dataset_protection_flags, eff_protection_flags, archive_id]).
-define(QOS_STATUS_ATTRS, [qos_status]).
-define(QOS_EFF_VALUE_ATTRS, [eff_qos_membership]).
-define(PATH_ATTRS, [path]).
-define(METADATA_ATTRS, [has_metadata]).

-define(ALL_ATTRS, lists:merge([?IMPLICIT_ATTRS, ?LINKS_ATTRS, ?FILE_META_ATTRS, ?TIMES_ATTRS, ?LOCATION_ATTRS, ?LUMA_ATTRS,
    ?ARCHIVE_RECALL_ATTRS, ?DATASET_ATTRS, ?QOS_STATUS_ATTRS, ?QOS_EFF_VALUE_ATTRS, ?PATH_ATTRS, ?METADATA_ATTRS])).

-define(PUBLIC_ATTRS, [guid, parent_guid, name, mode, atime, mtime, ctime, type, size, shares, index]).

% attrs that should not be visible in API
-define(INTERNAL_ATTRS, [is_deleted, conflicting_files, is_fully_replicated]).

-define(API_ATTRS, ?ALL_ATTRS -- ?INTERNAL_ATTRS).

-define(ONECLIENT_ATTRS, [
    guid, name, mode, parent_guid, uid, gid, atime, mtime, ctime, type, size, shares, provider_id, owner_id
]).

%% @TODO VFS-11377 deprecated, remove when possible
-define(DEPRECATED_ALL_ATTRS, [guid, parent_guid, name, mode, atime, mtime, ctime, type, size, shares, index,
    uid, gid, owner_id, provider_id, link_count]).
-define(DEPRECATED_PUBLIC_ATTRS, [guid, parent_guid, name, mode, atime, mtime, ctime, type, size, shares, index]).

% Macros defining types of membership
-define(NONE_MEMBERSHIP, none).
-define(DIRECT_MEMBERSHIP, direct).
-define(ANCESTOR_MEMBERSHIP, ancestor).
-define(DIRECT_AND_ANCESTOR_MEMBERSHIP, direct_and_ancestor).

-endif.
