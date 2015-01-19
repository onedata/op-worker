%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% request_dispatcher definitions.
%%% @end
%%%-------------------------------------------------------------------
-ifndef(REQUEST_DISPATCHER_HRL).
-define(REQUEST_DISPATCHER, 1).

%% This record is used by requests_dispatcher (it contains its state).
-record(dispatcher_state, {modules = [], modules_const_list = [], state_num = 0, current_load = 0, avg_load = 0, request_map = [], asnych_mode = false}).
%% -record(dispatcher_state, {modules = [], state_num = 0, request_map = []}).

-endif.