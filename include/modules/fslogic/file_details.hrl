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

-include_lib("ctool/include/posix/file_attr.hrl").

-record(file_details, {
    file_attr :: #file_attr{},
    % StartId can be used to list dir children starting from this file
    index_startid :: binary(),
    active_permissions_type :: file_meta:permissions_type(),
    has_metadata :: boolean(),
    has_direct_qos :: boolean(),
    has_eff_qos :: boolean()
}).

-endif.
