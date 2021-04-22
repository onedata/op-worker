%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This file contains definitions of macros used by atm_workflow module.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(ATM_WORKFLOW_HRL).
-define(ATM_WORKFLOW_HRL, 1).

-include("global_definitions.hrl").

-define(WAITING_WORKFLOWS_STATE, <<"waiting">>).
-define(ONGOING_WORKFLOWS_STATE, <<"ongoing">>).
-define(ENDED_WORKFLOWS_STATE, <<"ended">>).

-endif.
