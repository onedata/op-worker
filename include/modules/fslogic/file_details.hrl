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

-record(file_details, {
    guid :: undefined | binary() | atom() | integer(),
    name :: binary(),
    active_permissions_type :: file_meta:permissions_type(),
    mode :: non_neg_integer(),
    parent_guid :: undefined | binary(),
    uid = 0 :: non_neg_integer(),
    gid = 0 :: non_neg_integer(),
    atime = 0 :: non_neg_integer(),
    mtime = 0 :: non_neg_integer(),
    ctime = 0 :: non_neg_integer(),
    type :: file_meta:type(),
    size = 0 :: undefined | non_neg_integer(),
    shares = [] :: [binary()],
    provider_id :: binary(),
    owner_id :: binary(),
    has_metadata :: boolean()
}).

-endif.
