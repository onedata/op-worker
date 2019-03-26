%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains common definitions concerning rtransfer.
%%% @end
%%%-------------------------------------------------------------------
-author("Michal Stanisz").


-ifndef(OP_WORKER_MODULES_RTRANSFER_HRL).
-define(OP_WORKER_MODULES_RTRANSFER_HRL, 1).

%% Port number used by rtransfer
-define(RTRANSFER_PORT, proplists:get_value(server_port,
    application:get_env(rtransfer_link, transfer, []),
    6665)).

-endif.

