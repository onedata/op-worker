%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022-2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This file contains examples of automation schema record drafts
%%% used in CT tests.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(ATM_TEST_SCHEMA_DRAFTS_HRL).
-define(ATM_TEST_SCHEMA_DRAFTS_HRL, 1).


-include("atm/atm_test_schema.hrl").
-include_lib("ctool/include/onedata_file.hrl").


%% @TODO VFS-12091 include all attrs after atm versioning is introduced
-define(ATM_FILE_ATTRIBUTES, ?API_FILE_ATTRS -- [?attr_creation_time, ?attr_json_metadata, ?attr_has_json_metadata]).

-define(ECHO_DOCKER_IMAGE_ID, <<"test/echo">>).

% Failing by not returning result if size metric measurements are present in arguments
-define(FAILING_ECHO_MEASUREMENTS_DOCKER_IMAGE_ID_1, <<"test/failing_echo_measurements_1">>).
% Failing by returning integer in case of size metric measurement as an argument
-define(FAILING_ECHO_MEASUREMENTS_DOCKER_IMAGE_ID_2, <<"test/failing_echo_measurements_2">>).
-define(FAILING_ECHO_MEASUREMENTS_DOCKER_IMAGE_ID_2_RET_VALUE, 10).
% Failing by returning custom exception
-define(FAILING_ECHO_MEASUREMENTS_DOCKER_IMAGE_ID_3, <<"test/failing_echo_measurements_3">>).
-define(FAILING_ECHO_MEASUREMENTS_DOCKER_IMAGE_ID_3_EXCEPTION, <<"too hot to do any thinking!!!">>).
% Failing by lambda error
-define(FAILING_ECHO_MEASUREMENTS_DOCKER_IMAGE_ID_4, <<"test/failing_echo_measurements_4">>).
-define(FAILING_ECHO_MEASUREMENTS_DOCKER_IMAGE_ID_4_ERROR_MSG, <<"signal: illegal instruction (core dumped)\n">>).

% Sleeps, reporting a heartbeat only at the very beginning - as such it stops
% responding and is bound to be timed out under a short 'atm_workflow_job_timeout_sec'.
% NOTE: the silence must dwarf the timeout rather than merely outlast it. A result
% reaching op before its job was registered in the workflow engine is taken as a
% raced one and processed as any other - the job never gets a keepalive timer and
% can not be timed out at all. Registration happens only after the mocked
% 'run_task_for_item' step returns, and the test runner may hold that step back for
% seconds on end, so the answer has to stay far out of reach for that whole time.
% Being generous costs nothing - the job is killed by the timeout long before the
% sleep ends, and the lambda is left talking to itself.
-define(ECHO_WITH_SLEEP_DOCKER_IMAGE_ID, <<"test/echo_with_sleep">>).
-define(ECHO_WITH_SLEEP_SILENCE_SEC, 60).
% Keeps reporting heartbeats and answers only once its workflow execution has been
% asked to stop - a job that runs for as long as there is any point in running it,
% rather than one that hangs. Its execution lasts exactly as long as the test needs
% it to, with no sleep long enough to cover the slowest run to guess at.
-define(ECHO_UNTIL_STOPPING_DOCKER_IMAGE_ID, <<"test/echo_until_stopping">>).
% Must stay well below the 'atm_workflow_job_timeout_sec' of any suite using the
% above image - that is the keepalive timeout the heartbeats are there to reset
-define(ECHO_HEARTBEAT_INTERVAL_MILLIS, 200).

-define(ECHO_WITH_PAUSE_DOCKER_IMAGE_ID, <<"test/echo_with_pause">>).

-define(ECHO_WITH_EXCEPTION_ON_EVEN_NUMBERS, <<"test/echo_with_exception_on_even_numbers">>).
-define(ECHO_CONFIG_DOCKER_ID, <<"test/echo_config">>).


-define(ECHO_ARG_NAME, <<"value">>).

-define(ECHO_LAMBDA_DRAFT(__DATA_SPEC, __RELAY_METHOD), #atm_lambda_revision_draft{
    operation_spec = #atm_openfaas_operation_spec_draft{
        docker_image = ?ECHO_DOCKER_IMAGE_ID
    },
    config_parameter_specs = [#atm_parameter_spec{
        name = ?ECHO_ARG_NAME,
        data_spec = #atm_number_data_spec{
            integers_only = true,
            allowed_values = undefined
        },
        is_optional = true,
        default_value = 0
    }],
    argument_specs = [#atm_parameter_spec{
        name = ?ECHO_ARG_NAME,
        data_spec = __DATA_SPEC,
        is_optional = false
    }],
    result_specs = [#atm_lambda_result_spec{
        name = ?ECHO_ARG_NAME,
        data_spec = __DATA_SPEC,
        relay_method = __RELAY_METHOD
    }]
}).
-define(ECHO_LAMBDA_DRAFT(__DATA_SPEC), ?ECHO_LAMBDA_DRAFT(__DATA_SPEC, return_value)).
-define(NUMBER_ECHO_LAMBDA_DRAFT, ?ECHO_LAMBDA_DRAFT(#atm_number_data_spec{
    integers_only = false,
    allowed_values = undefined
})).

-define(ECHO_LAMBDA_ID, <<"echo">>).
-define(ECHO_LAMBDA_REVISION_NUM, 1).

-define(ITERATED_ITEM_ARG_MAPPER(__ARG_NAME), #atm_task_schema_argument_mapper{
    argument_name = __ARG_NAME,
    value_builder = #atm_task_argument_value_builder{
        type = iterated_item,
        recipe = undefined
    }
}).


-endif.
