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


-record(share_spec, {
    name = <<"share">> :: binary(),
    description = <<>> :: binary()
}).

-record(dataset_spec, {
    state = ?ATTACHED_DATASET :: dataset:state(),
    protection_flags = [] :: [binary()]
}).

-record(dataset_obj, {
    id :: dataset:id(),
    state :: dataset:state(),
    protection_flags :: [binary()]
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

-record(object, {
    guid :: file_id:file_guid(),
    name :: binary(),
    type :: file_meta:type(),
    mode :: file_meta:mode(),
    shares :: [od_share:id()],
    dataset = undefined :: undefined | onenv_dataset_test_utils:dataset_obj(),
    content = undefined :: undefined | binary(),  % set only for files
    children = undefined :: undefined | [onenv_file_test_utils:object()]  % set only for dirs
}).


-define(RAND_OP_NODE(__PROVIDER_SELECTOR),
    lists_utils:random_element(oct_background:get_provider_nodes(__PROVIDER_SELECTOR))
).


-endif.
