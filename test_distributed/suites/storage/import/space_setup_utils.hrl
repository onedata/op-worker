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

-record(support_spec, {
    provider :: oct_background:entity_selector(),
    storage_spec :: space_setup_utils:posix_storage_params(),
    size :: integer()
}).

-record(space_spec, {
    name :: atom(),
    owner :: oct_background:entity_selector(),
    users :: [oct_background:entity_selector()],
    supports :: [space_setup_utils:support_spec()]
}).

-endif.