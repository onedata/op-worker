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
    handler_module = undefined,
    method = <<"GET">> :: binary(),
    filepath = undefined :: binary(),
    attributes = undefined :: #fileattributes{},
    cdmi_version = undefined :: undefined | binary(),
    opts = [] :: [binary()]
}).

-define(error_forbidden_code, 403).
-define(error_not_found_code, 404).
-define(error_not_acceptable_code, 406).
-define(error_conflict_code, 409).
