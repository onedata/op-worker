%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
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


-define(ECHO_DOCKER_IMAGE_ID, <<"test/echo">>).

% Failing by not returning result if size metric measurements are present in arguments
-define(FAILING_ECHO_MEASUREMENTS_DOCKER_IMAGE_ID_1, <<"test/failing_echo_measurements_1">>).
% Failing by returning integer in case of size metric measurement as an argument
-define(FAILING_ECHO_MEASUREMENTS_DOCKER_IMAGE_ID_2, <<"test/failing_echo_measurements_2">>).
% Failing by returning custom exception
-define(FAILING_ECHO_MEASUREMENTS_DOCKER_IMAGE_ID_3, <<"test/failing_echo_measurements_3">>).


-define(INTEGER_LIST_STORE_SCHEMA_DRAFT(__ID, __DEFAULT_INITIAL_CONTENT), #atm_store_schema_draft{
    id = __ID,
    type = list,
    config = #atm_list_store_config{item_data_spec = #atm_data_spec{
        type = atm_integer_type
    }},
    requires_initial_content = false,
    default_initial_content = __DEFAULT_INITIAL_CONTENT
}).
-define(INTEGER_LIST_STORE_SCHEMA_DRAFT(__ID), ?INTEGER_LIST_STORE_SCHEMA_DRAFT(__ID, undefined)).


-define(ECHO_ARG_NAME, <<"value">>).

-define(ECHO_LAMBDA_DRAFT(__DATA_SPEC, __RELAY_METHOD), #atm_lambda_revision_draft{
    operation_spec = #atm_openfaas_operation_spec_draft{
        docker_image = ?ECHO_DOCKER_IMAGE_ID
    },
    argument_specs = [#atm_lambda_argument_spec{
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
-define(INTEGER_ECHO_LAMBDA_DRAFT, ?ECHO_LAMBDA_DRAFT(#atm_data_spec{type = atm_integer_type})).

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
