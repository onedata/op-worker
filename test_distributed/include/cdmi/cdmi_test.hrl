%%%-------------------------------------------------------------------
%%% @author Katarzyna Such
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This file contains definitions of macros used in cdmi tests.
%%% @end
%%%-------------------------------------------------------------------
-ifndef(CDMI_TEST_HRL).
-define(CDMI_TEST_HRL, 1).

-include("http/rest.hrl").

-define(TIMEOUT, timer:seconds(5)).
-define(ATTEMPTS, 100).

-define(CDMI_VERSION_HEADER, {<<"X-CDMI-Specification-Version">>, <<"1.1.1">>}).
-define(CDMI_CONTAINER_CONTENT_TYPE_HEADER, {?HDR_CONTENT_TYPE, <<"application/cdmi-container">>}).
-define(CDMI_OBJECT_CONTENT_TYPE_HEADER, {?HDR_CONTENT_TYPE, <<"application/cdmi-object">>}).
-define(CDMI_DEFAULT_STORAGE_BLOCK_SIZE, 100).

-define(FILE_OFFSET_START, 0).
-define(FILE_SIZE_INFINITY, 9999).
-define(FILE_CONTENT, <<"File content!">>).

-define(WORKERS(__CONFIG), [
    oct_background:get_random_provider_node(__CONFIG#cdmi_test_config.p1_selector),
    oct_background:get_random_provider_node(__CONFIG#cdmi_test_config.p2_selector)
]).

-define(build_test_root_path(__CONFIG),
    cdmi_test_utils:build_test_root_path(__CONFIG, ?FUNCTION_NAME)
).

-record(cdmi_test_config, {
    p1_selector :: oct_background:entity_selector(),
    p2_selector :: oct_background:entity_selector(),
    space_selector = undefined :: undefined | oct_background:entity_selector()
}).

-record(chunk, {
    offset :: non_neg_integer(),
    size :: non_neg_integer()
}).

-endif.

