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
    % StartId can be used to list dir children starting from this file
    index_startid :: binary(),
    active_permissions_type :: file_meta:permissions_type(),
    has_metadata :: boolean(),
    eff_qos_membership :: file_qos:membership() | undefined,
    eff_dataset_membership :: dataset:membership() | undefined,
    eff_protection_flags :: data_access_control:bitmask() | undefined
}).

-endif.
