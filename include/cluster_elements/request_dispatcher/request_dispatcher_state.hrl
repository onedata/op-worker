%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% The state of request dispatcher
%%% @end
%%%-------------------------------------------------------------------

-ifndef(REQUEST_DISPATCHER_STATE_HRL).
-define(REQUEST_DISPATCHER_STATE_HRL, 1).

%% This record is used by requests_dispatcher (it contains its state).
-record(dispatcher_state, {state_num = 0}).

-endif.