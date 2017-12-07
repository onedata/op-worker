%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This file contains definitions of macros used by transfer module.
%%% @end
%%%-------------------------------------------------------------------

-include("global_definitions.hrl").

-define(SUCCESSFUL_TRANSFERS_KEY, <<"SUCCESSFUL_TRANSFERS_KEY">>).
-define(FAILED_TRANSFERS_KEY, <<"FAILED_TRANSFERS_KEY">>).
-define(UNFINISHED_TRANSFERS_KEY, <<"UNFINISHED_TRANSFERS_KEY">>).

-define(FIVE_SEC_TIME_WINDOW, 5).
-define(MIN_TIME_WINDOW, 60).
-define(HR_TIME_WINDOW, 3600).
-define(DY_TIME_WINDOW, 86400).

-define(REPLICATION_POOL, begin
    {ok, __FileWorkerPool} = application:get_env(?APP_NAME, replication_pool),
    __FileWorkerPool
end).

-define(REPLICATION_POOL_SIZE, application:get_env(?APP_NAME, replication_pool_size, 10)).