%% ===================================================================
%% @author Tomasz Lichon
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This file contains common macros and records for cdmi protocol handlers
%% @end
%% ===================================================================

-include("err.hrl").
-include("veil_modules/fslogic/fslogic.hrl").
-include("veil_modules/control_panel/rest_messages.hrl").
-include("veil_modules/control_panel/common.hrl").

-record(state, {
    handler_module = undefined :: atom(),
    method = <<"GET">> :: binary(),
    filepath = undefined :: string(),
    objectid = undefined :: binary(),
    attributes = undefined :: #fileattributes{},
    cdmi_version = undefined :: binary(),
    opts = [] :: [binary()],
    capability = undefined :: atom()
}).

-define(ok, 200).
-define(ok_partial_content, 206).

-define(moved_pemanently_code, 301).

-define(error_bad_request_code, 400).
-define(error_unauthorized_code, 401).
-define(error_forbidden_code, 403).
-define(error_not_found_code, 404).
-define(error_not_acceptable_code, 406).
-define(error_conflict_code, 409).

-define(error_internal_code,500).
