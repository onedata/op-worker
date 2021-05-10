%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This file contains definitions of macros and records used by
%%% automation task execution machinery.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(ATM_TASK_EXECUTION_HRL).
-define(ATM_TASK_EXECUTION_HRL, 1).


-record(atm_task_execution_ctx, {
    item :: json_utils:json_term(),
    stores :: map()
}).

-record(atm_task_execution_argument_spec, {
    name :: binary(),
    input_spec :: atm_task_execution:arg_input_spec(),
    data_spec :: atm_data_spec:record(),
    is_batch :: boolean()
}).


-endif.
