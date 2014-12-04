%% ===================================================================
%% @author Tomasz Lichon
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This module provides definitions for range storing data structure
%% @end
%% ===================================================================

-ifndef(RANGES_STRUCT_HRL).
-define(RANGES_STRUCT_HRL, 1).

-define(infinity, 9999999999999999). %year 2286

-record(range, {from = 0, to = -1, timestamp = 0}).

-type ranges_struct() :: [#range{}].

-endif.