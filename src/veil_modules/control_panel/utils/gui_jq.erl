%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains page manipulation and asynchronous updates
%% functions based on jquery.
%% IMPORTANT: n2o's wf module must not be used directly!
%% These functions are a wrapper to that module, which gives control over
%% such aspects as javascript escaping.
%% @end
%% ===================================================================

-module(gui_jq).
%% -include("veil_modules/control_panel/common.hrl").
-include("logging.hrl").