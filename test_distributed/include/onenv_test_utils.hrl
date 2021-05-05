%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Definitions of macros and records used in file tests.
%%% @end
%%%-------------------------------------------------------------------
-author("Bartosz Walkowicz").

-ifndef(ONENV_TEST_UTILS_HRL).
-define(ONENV_TEST_UTILS_HRL, 1).


-include("modules/dataset/dataset.hrl").
-include("modules/fslogic/fslogic_common.hrl").


-record(archive_spec, {
    params :: undefined | archive:params(),
    attrs :: undefined | archive:attrs()
}).

-record(dataset_spec, {
    state = ?ATTACHED_DATASET :: dataset:state(),
    protection_flags = [] :: [binary()],
    archives = 0 :: non_neg_integer() | [onenv_archive_test_utils:archive_spec()]
}).

-record(file_spec, {
    name = undefined :: undefined | binary(),
    mode = ?DEFAULT_FILE_MODE :: file_meta:mode(),
    shares = [] :: [onenv_file_test_utils:share_spec()],
    dataset = undefined :: undefined | onenv_dataset_test_utils:dataset_spec(),
    content = <<"">> :: binary()
}).

-record(dir_spec, {
    name = undefined :: undefined | binary(),
    mode = ?DEFAULT_DIR_MODE :: file_meta:mode(),
    shares = [] :: [onenv_file_test_utils:share_spec()],
    dataset = undefined :: undefined | onenv_dataset_test_utils:dataset_spec(),
    children = [] :: [#dir_spec{} | #file_spec{}]
}).

-record(symlink_spec, {
    name = undefined :: undefined | binary(),
    shares = [] :: [onenv_file_test_utils:share_spec()],
    symlink_value :: binary()
}).

-record(share_spec, {
    name = <<"share">> :: binary(),
    description = <<>> :: binary()
}).


-record(archive_object, {
    id :: archive:id(),
    params :: archive:params(),
    attrs :: archive:attrs(),
    index :: dataset_api:archive_index()
}).


-record(dataset_object, {
    id :: dataset:id(),
    state :: dataset:state(),
    protection_flags :: [binary()],
    space_id :: od_space:id(),
    archives = [] :: [onenv_archive_test_utils:archive_object()]
}).

-record(object, {
    guid :: file_id:file_guid(),
    name :: binary(),
    type :: file_meta:type(),
    mode :: file_meta:mode(),
    shares :: [od_share:id()],
    dataset = undefined :: undefined | onenv_dataset_test_utils:dataset_object(),
    content = undefined :: undefined | binary(),  % set only for files
    children = undefined :: undefined | [onenv_file_test_utils:object()],  % set only for dirs
    symlink_value = undefined :: undefined | file_meta_symlinks:symlink()  % set only for symlinks
}).


-define(OCT_USER_ID(__USER_SELECTOR), oct_background:get_user_id(__USER_SELECTOR)).
-define(OCT_RAND_OP_NODE(__PROVIDER_SELECTOR),
    oct_background:get_random_provider_node(__PROVIDER_SELECTOR)
).


-endif.
