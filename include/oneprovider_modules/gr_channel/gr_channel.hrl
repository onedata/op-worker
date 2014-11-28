%% ===================================================================
%% @author Krzysztof Trzepla
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This file contains common definitions for Global Registry
%% channel.
%% @end
%% ===================================================================

-ifndef(GR_CHANNEL_HRL).
-define(GR_CHANNEL_HRL, 1).

-define(GR_CHANNEL_WORKER, gr_channel).
-define(GR_CHANNEL_STATE, gr_channel_state).

-record(?GR_CHANNEL_STATE, {status, pid}).

-endif.