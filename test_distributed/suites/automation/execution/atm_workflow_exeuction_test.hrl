%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Common includes and definitions used in automation workflow execution tests.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(ATM_WORKFLOW_EXECUTION_TEST).
-define(ATM_WORKFLOW_EXECUTION_TEST, 1).


-include("atm/atm_test_schema.hrl").
-include("atm_workflow_exeuction_test_runner.hrl").
-include("onenv_test_utils.hrl").
-include_lib("ctool/include/automation/automation.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").


% provider on which workflows shall be executed
-define(PROVIDER_SELECTOR, krakow).

% space in which workflows shall be executed
-define(SPACE_SELECTOR, space_krk).

% test inventory member with limited privileges
-define(USER_SELECTOR, user2).


-endif.
