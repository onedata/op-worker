%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% File details record definition. This record is used exclusively by gui
%%% to get and show, in one go instead of multiple requests, not only file
%%% basic attributes (#file_attr{}) but also additional information about
%%% active permissions type or existence of metadata.
%%% @end
%%%-------------------------------------------------------------------
-ifndef(FILE_DETAILS_HRL).
-define(FILE_DETAILS_HRL, 1).

-include("modules/fslogic/file_attr.hrl").

-record(file_details, {
    file_attr :: #file_attr{},
    symlink_value = undefined :: undefined | file_meta_symlinks:symlink(),
    active_permissions_type :: file_meta:permissions_type(),
    has_metadata :: boolean(),
    eff_qos_membership :: file_qos:membership() | undefined,
    eff_dataset_membership :: dataset:membership() | undefined,
    eff_protection_flags :: data_access_control:bitmask(),
    eff_dataset_protection_flags :: data_access_control:bitmask(),
    recall_root_id :: file_id:file_guid() | undefined,
    conflicting_name = undefined :: undefined | file_meta:name()
}).

% Macros defining types of membership
-define(NONE_MEMBERSHIP, none).
-define(DIRECT_MEMBERSHIP, direct).
-define(ANCESTOR_MEMBERSHIP, ancestor).
-define(DIRECT_AND_ANCESTOR_MEMBERSHIP, direct_and_ancestor).

-endif.
