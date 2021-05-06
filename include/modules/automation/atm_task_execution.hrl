%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This file contains definitions of macros used by atm_workflow_task
%%% machinery.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(ATM_TASK_EXECUTION_HRL).
-define(ATM_TASK_EXECUTION_HRL, 1).


-record(atm_task_execution_ctx, {
    item :: json_utils:json_term(),
    stores :: map()
}).

-endif.
