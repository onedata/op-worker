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


-include_lib("ctool/include/onedata_file.hrl").


-record(file_attr, {
    guid :: undefined | fslogic_worker:file_guid(),
    index :: undefined | file_listing:index(),
    type :: undefined | onedata_file:type(),
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
    xattrs :: undefined | #{onedata_file:xattr_name() => onedata_file:value()}
}).


% Macros defining types of inheritance path
-define(none_inheritance_path, none).
-define(direct_inheritance_path, direct).
-define(ancestor_inheritance, ancestor).
-define(direct_and_ancestor_inheritance_path, direct_and_ancestor).


-endif.
