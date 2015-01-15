%%%-------------------------------------------------------------------
%%% @author lichon
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 15. Jan 2015 11:16
%%%-------------------------------------------------------------------

%% This record is used by requests_dispatcher (it contains its state).
-record(dispatcher_state, {modules = [], modules_const_list = [], state_num = 0, current_load = 0, avg_load = 0, request_map = [], asnych_mode = false}).

