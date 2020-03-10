%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc File details record definition.
%%% @end
%%%-------------------------------------------------------------------
-author("Bartosz Walkowicz").

-ifndef(FILE_DETAILS_HRL).
-define(FILE_DETAILS_HRL, 1).

-include_lib("ctool/include/posix/file_attr.hrl").

-record(file_details, {
    file_attr :: #file_attr{},
    active_permissions_type :: file_meta:permissions_type(),
    has_metadata :: boolean()
}).

-endif.
