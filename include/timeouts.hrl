%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%% This file contains timeout definitions
%%% @end
%%% Created : 17. Mar 2016 10:45 AM
%%%-------------------------------------------------------------------
-author("Jakub Kudzia").

%%lfm_files.erl
-define(FSYNC_TIMEOUT, timer:seconds(30)).

%% replica_synchronizer
-define(SYNC_TIMEOUT, timer:minutes(5)).

%% gateway_connection
-define(CONNECTION_TIMEOUT, timer:minutes(1)).
-define(REQUEST_COMPLETION_TIMEOUT, timer:minutes(5)).

-define(SEND_RETRY_DELAY, timer:seconds(10)).
-define(DEFAULT_REQUEST_TIMEOUT, timer:minutes(5)).
