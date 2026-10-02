%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Constant properties of the supported storage types - what a storage backend
%%% of a given type can ever do, no matter how a particular storage is set up.
%%%
%%% Some operations are additionally constrained by the settings of a specific
%%% storage (e.g. importing existing data requires canonical paths). Such checks
%%% combine the capability listed here with the storage's helper spec and live
%%% in the helper_spec module.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_type).
-author("Bartosz Walkowicz").

-include("modules/storage/helpers/helpers.hrl").

%% API
-export([
    list_all/0,
    list_capabilities/1,
    list_supported_storage_path_types/1,
    get_default_block_size/1,

    has_capability/2,
    list_types_with_capability/1,
    list_types_with_capabilities/1
]).

%% posix_compatible - files carry POSIX ownership (uid/gid) and mode.
%% object_storage   - data is kept as objects rather than as a file tree.
%% rename           - files can be renamed on the storage.
%% nfs4_acl         - NFSv4 ACLs can be read from the storage.
%% oauth2           - access to the storage can be authorized with an OAuth2 token.
%% readwrite        - the storage can be written to (any storage can be readonly).
%% auto_import      - existing data can be discovered and imported by scanning.
%% manual_import    - existing files can be registered one by one.
%% getting_size     - the size of a file can be read from the storage.
-type capability() ::
    posix_compatible | object_storage | rename | nfs4_acl | oauth2 |
    readwrite | auto_import | manual_import | getting_size.

-export_type([capability/0]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% NOTE: must be kept in sync with the list_capabilities/1 clauses.
%% @end
%%--------------------------------------------------------------------
-spec list_all() -> [onedata_storage:type()].
list_all() -> [
    ?CEPH_HELPER_NAME,
    ?CEPHRADOS_HELPER_NAME,
    ?GLUSTERFS_HELPER_NAME,
    ?HTTP_HELPER_NAME,
    ?NFS_HELPER_NAME,
    ?NULL_DEVICE_HELPER_NAME,
    ?POSIX_HELPER_NAME,
    ?S3_HELPER_NAME,
    ?SWIFT_HELPER_NAME,
    ?WEBDAV_HELPER_NAME,
    ?XROOTD_HELPER_NAME
].


-spec list_capabilities(onedata_storage:type()) -> [capability()].
list_capabilities(?CEPH_HELPER_NAME) ->
    [object_storage, readwrite];
list_capabilities(?CEPHRADOS_HELPER_NAME) ->
    [object_storage, readwrite, manual_import, getting_size];
list_capabilities(?GLUSTERFS_HELPER_NAME) ->
    [posix_compatible, readwrite, rename, nfs4_acl, auto_import, manual_import, getting_size];
list_capabilities(?HTTP_HELPER_NAME) ->
    [oauth2, manual_import, getting_size];
list_capabilities(?NFS_HELPER_NAME) ->
    [posix_compatible, readwrite, rename, auto_import, manual_import, getting_size];
list_capabilities(?NULL_DEVICE_HELPER_NAME) ->
    [posix_compatible, readwrite, rename, auto_import, manual_import, getting_size];
list_capabilities(?POSIX_HELPER_NAME) ->
    [posix_compatible, readwrite, rename, nfs4_acl, auto_import, manual_import, getting_size];
list_capabilities(?S3_HELPER_NAME) ->
    [object_storage, readwrite, auto_import, manual_import, getting_size];
list_capabilities(?SWIFT_HELPER_NAME) ->
    [object_storage, readwrite, manual_import, getting_size];
list_capabilities(?WEBDAV_HELPER_NAME) ->
    [readwrite, rename, oauth2, auto_import, manual_import, getting_size];
list_capabilities(?XROOTD_HELPER_NAME) ->
    [readwrite, rename, auto_import, manual_import, getting_size].


%%--------------------------------------------------------------------
%% @doc
%% Storage path types the backend is able to work with. Mirrors the domain
%% declared by the <type>_helper_configuration record in the Onepanel contract
%% (and by the storagePathType enums in the Onepanel swagger).
%% @end
%%--------------------------------------------------------------------
-spec list_supported_storage_path_types(onedata_storage:type()) -> [storage_path_type()].
list_supported_storage_path_types(?CEPH_HELPER_NAME) ->
    [?CANONICAL_STORAGE_PATH, ?FLAT_STORAGE_PATH];
list_supported_storage_path_types(?CEPHRADOS_HELPER_NAME) ->
    [?FLAT_STORAGE_PATH];
list_supported_storage_path_types(?GLUSTERFS_HELPER_NAME) ->
    [?CANONICAL_STORAGE_PATH];
list_supported_storage_path_types(?HTTP_HELPER_NAME) ->
    [?CANONICAL_STORAGE_PATH, ?FLAT_STORAGE_PATH];
list_supported_storage_path_types(?NFS_HELPER_NAME) ->
    [?CANONICAL_STORAGE_PATH];
list_supported_storage_path_types(?NULL_DEVICE_HELPER_NAME) ->
    [?CANONICAL_STORAGE_PATH, ?FLAT_STORAGE_PATH];
list_supported_storage_path_types(?POSIX_HELPER_NAME) ->
    [?CANONICAL_STORAGE_PATH];
list_supported_storage_path_types(?S3_HELPER_NAME) ->
    [?CANONICAL_STORAGE_PATH, ?FLAT_STORAGE_PATH];
list_supported_storage_path_types(?SWIFT_HELPER_NAME) ->
    [?CANONICAL_STORAGE_PATH, ?FLAT_STORAGE_PATH];
list_supported_storage_path_types(?WEBDAV_HELPER_NAME) ->
    [?CANONICAL_STORAGE_PATH];
list_supported_storage_path_types(?XROOTD_HELPER_NAME) ->
    [?CANONICAL_STORAGE_PATH, ?FLAT_STORAGE_PATH].


%%--------------------------------------------------------------------
%% @doc
%% Size of the objects a file is split into on the storage, used unless the
%% admin sets one explicitly. Returns undefined for types that always store
%% a file as a whole.
%% @end
%%--------------------------------------------------------------------
-spec get_default_block_size(onedata_storage:type()) -> undefined | non_neg_integer().
get_default_block_size(?CEPHRADOS_HELPER_NAME) -> 4194304;
get_default_block_size(?S3_HELPER_NAME) -> 10485760;
get_default_block_size(?SWIFT_HELPER_NAME) -> 10485760;
get_default_block_size(_) -> undefined.


-spec has_capability(onedata_storage:type(), capability()) -> boolean().
has_capability(Type, Capability) ->
    lists:member(Capability, list_capabilities(Type)).


-spec list_types_with_capability(capability()) -> [onedata_storage:type()].
list_types_with_capability(Capability) ->
    list_types_with_capabilities([Capability]).


-spec list_types_with_capabilities([capability()]) -> [onedata_storage:type()].
list_types_with_capabilities(Capabilities) ->
    lists:filter(fun(Type) ->
        lists:all(fun(Capability) -> has_capability(Type, Capability) end, Capabilities)
    end, list_all()).
