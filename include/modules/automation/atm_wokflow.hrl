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

-define(WORKFLOWS_TREE_ID, <<"WORKFLOWS_TREE">>).

-define(WAITING_WORKFLOWS_KEY, <<"WAITING_WORKFLOWS_KEY">>).
-define(ONGOING_WORKFLOWS_KEY, <<"ONGOING_WORKFLOWS_KEY">>).
-define(ENDED_WORKFLOWS_KEY, <<"ENDED_WORKFLOWS_KEY">>).

-endif.