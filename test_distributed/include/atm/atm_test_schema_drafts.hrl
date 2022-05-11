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


-define(INTEGER_ECHO_LAMBDA_DRAFT, #atm_lambda_revision_draft{
    operation_spec = #atm_openfaas_operation_spec_draft{
        docker_image = <<"test/echo">>
    },
    argument_specs = [#atm_lambda_argument_spec{
        name = <<"value">>,
        data_spec = #atm_data_spec{type = atm_integer_type},
        is_optional = false
    }],
    result_specs = [#atm_lambda_result_spec{
        name = <<"value">>,
        data_spec = #atm_data_spec{type = atm_integer_type},
        relay_method = return_value
    }]
}).

-define(ECHO_LAMBDA_ID, <<"echo">>).
-define(ECHO_LAMBDA_REVISION_NUM, 1).

-define(ECHO_TASK_DRAFT(__ID, __TARGET_STORE_SCHEMA_ID, __TARGET_STORE_UPDATE_OPTIONS), #atm_task_schema_draft{
    id = __ID,
    lambda_id = ?ECHO_LAMBDA_ID,
    lambda_revision_number = ?ECHO_LAMBDA_REVISION_NUM,
    argument_mappings = [#atm_task_schema_argument_mapper{
        argument_name = <<"value">>,
        value_builder = #atm_task_argument_value_builder{
            type = iterated_item,
            recipe = undefined
        }
    }],
    result_mappings = [#atm_task_schema_result_mapper{
        result_name = <<"value">>,
        store_schema_id = __TARGET_STORE_SCHEMA_ID,
        store_content_update_options = __TARGET_STORE_UPDATE_OPTIONS
    }]
}).
-define(ECHO_TASK_DRAFT(__TARGET_STORE_SCHEMA_ID, __TARGET_STORE_UPDATE_OPTIONS),
    ?ECHO_TASK_DRAFT(?ATM_AUTOGENERATE, __TARGET_STORE_SCHEMA_ID, __TARGET_STORE_UPDATE_OPTIONS)
).


-endif.
