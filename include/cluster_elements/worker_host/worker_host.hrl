%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% worker_host definitions.
%%% @end
%%%-------------------------------------------------------------------
-ifndef(WORKER_HOST_HRL).
-define(WORKER_HOST_HRL, 1).

%% This record is used by worker_host (it contains its state). It describes
%% plug_in that is used and state of this plug_in. It contains also
%% information about time of requests processing (used by ccm during
%% load balancing).
-record(host_state, {plug_in = non, request_map = non, dispatcher_request_map = non, dispatcher_request_map_ok = true, plug_in_state = [], load_info = [], current_seq_job = none, seq_queue = []}).
%% This method is used to init worker_host when it is using sub proccesses
-record(initial_host_description, {request_map = non, dispatcher_request_map = non, plug_in_state = []}).

-define(BORTER_CHILD_WAIT_TIME, 10000).
-define(MAX_CHILD_WAIT_TIME, 60000000).
-define(MAX_CALCULATION_WAIT_TIME, 10000000).

-endif.