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

-define(PAST_TRANSFERS_KEY, <<"PAST_TRANSFERS_KEY">>).
-define(CURRENT_TRANSFERS_KEY, <<"CURRENT_TRANSFERS_KEY">>).

-define(FIVE_SEC_TIME_WINDOW, 5).
-define(MIN_TIME_WINDOW, 60).
-define(HR_TIME_WINDOW, 3600).
-define(DY_TIME_WINDOW, 86400).

-define(TRANSFER_WORKERS_POOL, begin
    {ok, __TransferWorkersPool} = application:get_env(?APP_NAME, transfer_workers_pool),
    __TransferWorkersPool
end).

-define(TRANSFER_WORKERS_NUM, application:get_env(?APP_NAME, transfer_workers_num, 10)).