%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This file contains definitions of macros used by atm_workflow_execution module.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(ATM_WORKFLOW_EXECUTION_HRL).
-define(ATM_WORKFLOW_EXECUTION_HRL, 1).

-include("global_definitions.hrl").

-define(WAITING_STATE, <<"waiting">>).
-define(ONGOING_STATE, <<"ongoing">>).
-define(ENDED_STATE, <<"ended">>).

-endif.
