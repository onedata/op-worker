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

-record(range, {from = 0, to = -1, timestamp = 0}).