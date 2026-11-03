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

-record(dataset_info, {
    id :: dataset:id(),
    state :: dataset:state(),
    root_file_guid :: fslogic_worker:file_guid(),
    root_file_path :: file_meta:path(),
    root_file_type :: onedata_file:type(),
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
    archiving_provider :: oneprovider:id(),
    creator :: od_user:id(),
    state :: archive:state(),
    root_dir_guid :: undefined | file_id:file_guid(),
    data_dir_guid :: undefined | file_id:file_guid(),
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

-endif.
