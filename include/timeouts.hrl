%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains timeout definitions
%%% @end
-author("Jakub Kudzia").

%%lfm_files.erl
-define(FSYNC_TIMEOUT, timer:minutes(1)).

%% replica_synchronizer
-define(SYNC_TIMEOUT, timer:seconds(20)).

%% gateway_connection
-define(CONNECTION_TIMEOUT, timer:minutes(1)).
-define(REQUEST_COMPLETION_TIMEOUT, timer:seconds(30)).

-define(SEND_RETRY_DELAY, timer:seconds(10)).
-define(DEFAULT_REQUEST_TIMEOUT, timer:seconds(15)).
