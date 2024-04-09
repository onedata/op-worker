%%%-------------------------------------------------------------------
%%% @author Katarzyna Such
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Common definitions concerning space setup in tests.
%%% @end
%%%-------------------------------------------------------------------
-ifndef(STORAGE_IMPORT_HRL).


-record(s3_storage_params, {
    hostname :: binary(),
    bucket_name :: binary(),
    block_size = 10485760 :: integer(),
    storage_path_type = <<"flat">> :: binary(),
    imported_storage = false :: boolean()
}).


-record(posix_storage_params, {
    mount_point :: binary(),
    imported_storage = false :: boolean()
}).

-record(support_spec, {
    provider :: oct_background:entity_selector(),
    storage_spec :: space_setup_utils:posix_storage_params() | space_setup_utils:s3_storage_params()
    | storage:id(),
    size :: integer()
}).

-record(space_spec, {
    name :: atom(),
    owner :: oct_background:entity_selector(),
    users :: [oct_background:entity_selector()],
    supports :: [space_setup_utils:support_spec()]
}).

-endif.