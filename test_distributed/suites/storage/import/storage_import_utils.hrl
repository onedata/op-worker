%%%-------------------------------------------------------------------
%%% @author Katarzyna Such
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%% @doc
%%% Common definitions concerning storage import tests
%%% @end
%%%-------------------------------------------------------------------
-ifndef(STORAGE_IMPORT_HRL).


-record(posix_storage_params, {
    type :: binary(),
    mountPoint :: binary()
}).

-type posix_storage_params() :: #posix_storage_params{}.

-record(storage_spec, {
    name :: atom(),
    params :: posix_storage_params()
}).

-type storage_spec() :: #storage_spec{}.

-record(support_spec, {
    provider :: atom(),
    storage :: storage_spec(),
    size :: integer()
}).

-record(space_spec, {
    name :: atom(),
    owner :: atom(),
    users :: list(),
    supports :: list()
}).

-endif.