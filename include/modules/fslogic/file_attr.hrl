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
    eff_dataset_membership :: undefined | dataset:membership(),
    eff_qos_membership :: undefined | file_qos:membership(),
    qos_status :: undefined | qos_status:summary(),
    recall_root_id :: undefined | file_id:file_guid(),
    is_deleted :: undefined | boolean(),
    conflicting_files :: undefined | file_meta:conflicts(),
    xattrs :: undefined | #{custom_metadata:name() => custom_metadata:value()}
}).


-define(IMPLICIT_ATTRS, [guid]).
-define(FILE_META_ATTRS, [index, type, active_permissions_type, mode, acl, parent_guid, provider_id, shares, owner_id,
    hardlink_count, symlink_value, is_deleted]).
-define(LINKS_ATTRS, [name, conflicting_name, conflicting_files]).
-define(PATH_ATTRS, [path]).
-define(LUMA_ATTRS, [gid, uid]).
-define(TIMES_ATTRS, [atime, mtime, ctime]).
-define(LOCATION_ATTRS, [size, is_fully_replicated, local_replication_rate]).
-define(METADATA_ATTRS, [has_custom_metadata]).
-define(DATASET_ATTRS, [eff_dataset_membership, eff_dataset_protection_flags, eff_protection_flags]).
-define(QOS_EFF_VALUE_ATTRS, [eff_qos_membership]).
-define(QOS_STATUS_ATTRS, [qos_status]).
-define(ARCHIVE_RECALL_ATTRS, [recall_root_id]).

-define(ALL_ATTRS, lists:merge([?IMPLICIT_ATTRS, ?FILE_META_ATTRS, ?LINKS_ATTRS, ?PATH_ATTRS, ?LUMA_ATTRS, ?TIMES_ATTRS,
    ?LOCATION_ATTRS, ?METADATA_ATTRS, ?DATASET_ATTRS, ?QOS_EFF_VALUE_ATTRS, ?QOS_STATUS_ATTRS, ?ARCHIVE_RECALL_ATTRS])).

-define(PUBLIC_ATTRS, [guid, index, type, active_permissions_type, mode, name, conflicting_name, parent_guid,
    atime, mtime, ctime, size, shares,  symlink_value, has_custom_metadata]).

% attrs that are used internally by op-worker mechanisms (like events, storage import); are not visible in API
-define(INTERNAL_ATTRS, [is_deleted, conflicting_files]).

-define(API_ATTRS, ?ALL_ATTRS -- ?INTERNAL_ATTRS).

%% @TODO VFS-11378 remove when all usages provide their custom required attrs
-define(ONECLIENT_ATTRS, [
    guid, type, mode, name, parent_guid, gid, uid, atime, mtime, ctime, size, provider_id, shares, owner_id
]).

%% @TODO VFS-11377 deprecated, remove when possible
-define(DEPRECATED_ALL_ATTRS, [guid, parent_guid, name, mode, atime, mtime, ctime, type, size, shares, index,
    uid, gid, owner_id, provider_id, hardlink_count]).
-define(DEPRECATED_PUBLIC_ATTRS, [guid, parent_guid, name, mode, atime, mtime, ctime, type, size, shares, index]).

% Macros defining types of membership
-define(NONE_MEMBERSHIP, none).
-define(DIRECT_MEMBERSHIP, direct).
-define(ANCESTOR_MEMBERSHIP, ancestor).
-define(DIRECT_AND_ANCESTOR_MEMBERSHIP, direct_and_ancestor).

-endif.
