%%%-------------------------------------------------------------------
%%% @author Katarzyna Such
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%% @doc
%%% Common definitions concerning storage import tests.
%%% @end
%%%-------------------------------------------------------------------
-ifndef(STORAGE_IMPORT_HRL).


-record(posix_storage_params, {
    mount_point :: binary()
}).

-type posix_storage_params() :: #posix_storage_params{}.

-record(support_spec, {
    provider :: oct_background:entity_selector(),
    storage_params :: posix_storage_params(),
    size :: integer()
}).

-type support_spec() :: #support_spec{}.

-record(space_spec, {
    name :: atom(),
    owners :: [oct_background:entity_selector()],
    users :: list(),
    supports :: [support_spec()]
}).

-type space_spec() :: #space_spec{}.

-endif.