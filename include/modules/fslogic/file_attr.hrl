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
    index :: undefined | file_listing:index(),
    type :: undefined | file_attr:file_type(),
    active_permissions_type :: undefined | file_meta:permissions_type(),
    mode :: undefined | file_meta:mode(),
    acl :: undefined | acl:acl(),
    name :: undefined | file_meta:name(),
    conflicting_name :: undefined | file_meta:name(),
    path :: undefined | file_meta:path(),
    parent_guid :: undefined | fslogic_worker:file_guid(),
    gid :: undefined | non_neg_integer(),
    uid :: undefined | non_neg_integer(),
    atime :: undefined | times:a_time(),
    mtime :: undefined | times:m_time(),
    ctime :: undefined | times:c_time(),
    size :: undefined | file_meta:size(),
    is_fully_replicated :: undefined | boolean(),
    local_replication_rate :: undefined | float(),
    provider_id :: undefined | od_provider:id(),
    shares :: undefined | [od_share:id()],
    owner_id :: undefined | od_user:id(),
    hardlink_count :: undefined | non_neg_integer(),
    symlink_value :: undefined | file_meta_symlinks:symlink(),
    has_custom_metadata :: undefined | boolean(),
    eff_protection_flags :: undefined | data_access_control:bitmask(),
    eff_dataset_protection_flags :: undefined | data_access_control:bitmask(),
    eff_dataset_inheritance_path :: undefined | dataset:inheritance_path(),
    eff_qos_inheritance_path :: undefined | file_qos:inheritance_path(),
    qos_status :: undefined | qos_status:summary(),
    recall_root_id :: undefined | file_id:file_guid(),
    is_deleted :: undefined | boolean(),
    conflicting_files :: undefined | file_meta:conflicts(),
    xattrs :: undefined | #{custom_metadata:name() => custom_metadata:value()}
}).

-define(attr_guid, guid).
-define(attr_index, index).
-define(attr_type, type).
-define(attr_active_permissions_type, active_permissions_type).
-define(attr_mode, mode).
-define(attr_acl, acl).
-define(attr_name, name).
-define(attr_conflicting_name, conflicting_name).
-define(attr_path, path).
-define(attr_parent_guid, parent_guid).
-define(attr_gid, gid).
-define(attr_uid, uid).
-define(attr_atime, atime).
-define(attr_mtime, mtime).
-define(attr_ctime, ctime).
-define(attr_size, size).
-define(attr_is_fully_replicated, is_fully_replicated).
-define(attr_local_replication_rate, local_replication_rate).
-define(attr_provider_id, provider_id).
-define(attr_shares, shares).
-define(attr_owner_id, owner_id).
-define(attr_hardlink_count, hardlink_count).
-define(attr_symlink_value, symlink_value).
-define(attr_has_custom_metadata, has_custom_metadata).
-define(attr_eff_protection_flags, eff_protection_flags).
-define(attr_eff_dataset_protection_flags, eff_dataset_protection_flags).
-define(attr_eff_dataset_inheritance_path, eff_dataset_inheritance_path).
-define(attr_eff_qos_inheritance_path, eff_qos_inheritance_path).
-define(attr_qos_status, qos_status).
-define(attr_recall_root_id, recall_root_id).
-define(attr_is_deleted, is_deleted).
-define(attr_conflicting_files, conflicting_files).
-define(attr_xattrs(XattrNames), {xattrs, XattrNames}).

-define(IMPLICIT_ATTRS, [
    ?attr_guid
]).
-define(FILE_META_ATTRS, [
    ?attr_index, ?attr_type, ?attr_active_permissions_type, ?attr_mode, ?attr_acl, ?attr_parent_guid,
    ?attr_provider_id, ?attr_shares, ?attr_owner_id, ?attr_hardlink_count, ?attr_symlink_value, ?attr_is_deleted
]).
-define(LINKS_TREE_ATTRS, [
    ?attr_name, ?attr_conflicting_name, ?attr_conflicting_files
]).
-define(PATH_ATTRS, [
    ?attr_path
]).
-define(LUMA_ATTRS, [
    ?attr_gid, ?attr_uid
]).
-define(TIMES_ATTRS, [
    ?attr_atime, ?attr_mtime, ?attr_ctime
]).
-define(LOCATION_ATTRS, [
    ?attr_size, ?attr_is_fully_replicated, ?attr_local_replication_rate
]).
-define(METADATA_ATTRS, [
    ?attr_has_custom_metadata
]).
-define(DATASET_ATTRS, [
    ?attr_eff_dataset_inheritance_path, ?attr_eff_dataset_protection_flags, ?attr_eff_protection_flags
]).
-define(QOS_EFF_VALUE_ATTRS, [
    ?attr_eff_qos_inheritance_path
]).
-define(QOS_STATUS_ATTRS, [
    ?attr_qos_status
]).
-define(ARCHIVE_RECALL_ATTRS, [
    ?attr_recall_root_id
]).

% Below list does not contain xattrs
-define(ALL_ATTRS, lists:merge([
    ?IMPLICIT_ATTRS, ?FILE_META_ATTRS, ?LINKS_TREE_ATTRS, ?PATH_ATTRS, ?LUMA_ATTRS, ?TIMES_ATTRS,
    ?LOCATION_ATTRS, ?METADATA_ATTRS, ?DATASET_ATTRS, ?QOS_EFF_VALUE_ATTRS, ?QOS_STATUS_ATTRS, ?ARCHIVE_RECALL_ATTRS
])).

% attrs that are used internally by op-worker mechanisms (like events, storage import); are not visible in API
-define(INTERNAL_ATTRS, [
    ?attr_is_deleted, ?attr_conflicting_files
]).

-define(API_ATTRS, ?ALL_ATTRS -- ?INTERNAL_ATTRS).
-define(PUBLIC_API_ATTRS, [
    ?attr_guid, ?attr_index, ?attr_type, ?attr_active_permissions_type, ?attr_mode, ?attr_name, ?attr_conflicting_name,
    ?attr_parent_guid, ?attr_atime, ?attr_mtime, ?attr_ctime, ?attr_size, ?attr_shares,  ?attr_symlink_value,
    ?attr_has_custom_metadata
]).

%% @TODO VFS-11378 remove when all usages provide their custom required attrs
-define(ONECLIENT_ATTRS, [
    ?attr_guid, ?attr_type, ?attr_mode, ?attr_name, ?attr_parent_guid, ?attr_gid, ?attr_uid, ?attr_atime, ?attr_mtime,
    ?attr_ctime, ?attr_size, ?attr_provider_id, ?attr_shares, ?attr_owner_id
]).

%% @TODO VFS-11377 deprecated, remove when possible
-define(DEPRECATED_ALL_ATTRS, [
    ?attr_guid, ?attr_parent_guid, ?attr_name, ?attr_mode, ?attr_atime, ?attr_mtime, ?attr_ctime, ?attr_type, ?attr_size,
    ?attr_shares, ?attr_index, ?attr_uid, ?attr_gid, ?attr_owner_id, ?attr_provider_id, ?attr_hardlink_count
]).
-define(DEPRECATED_PUBLIC_ATTRS, [
    ?attr_guid, ?attr_parent_guid, ?attr_name, ?attr_mode, ?attr_atime, ?attr_mtime, ?attr_ctime, ?attr_type, ?attr_size,
    ?attr_shares, ?attr_index
]).

% Macros defining types of inheritance path
-define(NONE_INHERITANCE_PATH, none).
-define(DIRECT_INHERITANCE_PATH, direct).
-define(ANCESTOR_INHERITANCE, ancestor).
-define(DIRECT_AND_ANCESTOR_INHERITANCE_PATH, direct_and_ancestor).

-endif.
