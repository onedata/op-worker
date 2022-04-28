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


-define(ECHO_LAMBDA_DRAFT, #atm_lambda_revision_draft{
    operation_spec = #atm_openfaas_operation_spec_draft{
        docker_image = <<"test/echo">>
    },
    argument_specs = [#atm_lambda_argument_spec{
        name = <<"val">>,
        data_spec = #atm_data_spec{type = atm_integer_type},
        is_optional = false
    }],
    result_specs = [#atm_lambda_result_spec{
        name = <<"val">>,
        data_spec = #atm_data_spec{type = atm_integer_type},
        relay_method = return_value
    }]
}).

-define(ECHO_TASK_DRAFT(__TARGET_STORE_SCHEMA_ID, __TARGET_STORE_UPDATE_OPTIONS), #atm_task_schema_draft{
    lambda_id = <<"echo">>,
    lambda_revision_number = 1,
    argument_mappings = [#atm_task_schema_argument_mapper{
        argument_name = <<"val">>,
        value_builder = #atm_task_argument_value_builder{
            type = iterated_item,
            recipe = undefined
        }
    }],
    result_mappings = [#atm_task_schema_result_mapper{
        result_name = <<"val">>,
        store_schema_id = __TARGET_STORE_SCHEMA_ID,
        store_content_update_options = __TARGET_STORE_UPDATE_OPTIONS
    }]
}).


-endif.
