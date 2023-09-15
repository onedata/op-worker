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
-include("http/rest.hrl").

-define(TIMEOUT, timer:seconds(5)).
-define(ATTEMPTS, 100).

-define(CDMI_VERSION_HEADER, {<<"X-CDMI-Specification-Version">>, <<"1.1.1">>}).
-define(CONTAINER_CONTENT_TYPE_HEADER, {?HDR_CONTENT_TYPE, <<"application/cdmi-container">>}).
-define(OBJECT_CONTENT_TYPE_HEADER, {?HDR_CONTENT_TYPE, <<"application/cdmi-object">>}).
-define(DEFAULT_STORAGE_BLOCK_SIZE, 100).

-define(FILE_BEGINNING, 0).
-define(INFINITY, 9999).

-record(cdmi_test_config, {
    p1_selector :: binary(),
    p2_selector :: binary(),
    space_selector = undefined :: undefined | binary()
}).

-record(chunk, {
    offset :: non_neg_integer(),
    size :: non_neg_integer()
}).



