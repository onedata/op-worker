%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This file contains tests concerning QoS basic API (REST + gs).
%%% @end
%%%--------------------------------------------------------------------
-module(qos_api_test_SUITE).
-author("Michal Stanisz").

-include("api_test_runner.hrl").
-include("global_definitions.hrl").
-include("modules/datastore/qos.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/http/codes.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/graph_sync/gri.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("cluster_worker/include/time_series/browsing.hrl").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    create_qos_test/1,
    get_qos_test/1,
    delete_qos_test/1,
    get_qos_summary_test/1,
    get_available_qos_parameters_test/1,
    evaluate_qos_expression_test/1,
    get_qos_entry_audit_log/1,
    get_qos_transfer_stats_collection_schema/1,
    get_qos_transfer_stats_collection_layout/1,
    get_qos_transfer_stats_collection_slice/1
]).


all() -> [
    create_qos_test,
    get_qos_test,
    delete_qos_test,
    get_qos_summary_test,
    get_available_qos_parameters_test,
    evaluate_qos_expression_test,
    get_qos_entry_audit_log,
    get_qos_transfer_stats_collection_schema,
    get_qos_transfer_stats_collection_layout,
    get_qos_transfer_stats_collection_slice
].

-define(ATTEMPTS, 40).

%%%===================================================================
%%% QoS API tests.
%%%===================================================================

create_qos_test(Config) ->
    [P2, P1] = ?config(op_worker_nodes, Config),
    SessIdP1 = ?USER_IN_BOTH_SPACES_SESS_ID(P1, Config),
    SessIdP2 = ?USER_IN_BOTH_SPACES_SESS_ID(P2, Config),
    FileType = api_test_utils:randomly_choose_file_type_for_test(),

    {ok, FileToShareGuid} = lfm_test_utils:create_file(
        FileType, P1, SessIdP1, filename:join(["/", ?SPACE_2, ?RANDOM_FILE_NAME()])),
    ?assertMatch({ok, _}, lfm_proxy:stat(P2, SessIdP2, ?FILE_REF(FileToShareGuid)), ?ATTEMPTS),
    {ok, ShareId} = opt_shares:create(P1, SessIdP1, ?FILE_REF(FileToShareGuid), <<"share">>),

    MemRef = api_test_memory:init(),

    SetupFun = fun() ->
        FilePath = filename:join(["/", ?SPACE_2, ?RANDOM_FILE_NAME()]),
        {ok, Guid} = lfm_test_utils:create_file(FileType, P1, SessIdP1, FilePath),
        ?assertMatch({ok, _}, lfm_proxy:stat(P2, SessIdP2, ?FILE_REF(Guid)), ?ATTEMPTS),
        api_test_memory:set(MemRef, guid, Guid)
    end,

    CreateDataSpec = #data_spec{
        required = [<<"expression">>, <<"fileId">>],
        optional = [<<"replicasNum">>],
        correct_values = #{
            <<"expression">> => [
                % no need to test more correct values as they are checked in unit tests 
                <<"key=value">>
            ],
            <<"fileId">> => [objectId],
            <<"replicasNum">> => [rand:uniform(10)]
        },
        bad_values = [
            {<<"expression">>, <<"aaa">>, ?ERROR_INVALID_QOS_EXPRESSION(<<"syntax error before: ">>)},
            {<<"fileId">>, <<"gibberish">>, ?ERROR_BAD_VALUE_IDENTIFIER(<<"fileId">>)},
            {<<"replicasNum">>, <<"aaa">>, ?ERROR_BAD_VALUE_INTEGER(<<"replicasNum">>)},
            {<<"replicasNum">>, 0, ?ERROR_BAD_VALUE_TOO_LOW(<<"replicasNum">>, 1)}
        ]
    },

    ?assert(api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = [P1, P2],
            client_spec = ?CLIENT_SPEC_FOR_SPACE_2_SCENARIOS(Config),
            setup_fun = SetupFun,
            verify_fun = verify_fun(MemRef, Config, {instance, create}),
            scenario_templates = [
                #scenario_template{
                    name = <<"Create QoS using rest endpoint">>,
                    type = rest,
                    prepare_args_fun = prepare_args_fun_rest(MemRef, {instance, create}),
                    validate_result_fun = validate_result_fun_rest(MemRef, {instance, create})
                },
                #scenario_template{
                    name = <<"Create QoS using gs endpoint">>,
                    type = gs,
                    prepare_args_fun = prepare_args_fun_gs(MemRef, {instance, create}),
                    validate_result_fun = validate_result_fun_gs(MemRef, {instance, create})
                }
            ],
            data_spec = api_test_utils:replace_enoent_with_error_not_found_in_error_expectations(
                api_test_utils:add_cdmi_id_errors_for_operations_not_available_in_share_mode(
                    FileToShareGuid, ?SPACE_2, ShareId, CreateDataSpec
                )
            )
        }
    ])),
    ok.


get_qos_test(Config) ->
    [P2, P1] = ?config(op_worker_nodes, Config),
    SessIdP1 = ?USER_IN_BOTH_SPACES_SESS_ID(P1, Config),
    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    FilePath = filename:join(["/", ?SPACE_2, ?RANDOM_FILE_NAME()]),
    {ok, Guid} = lfm_test_utils:create_file(FileType, P1, SessIdP1, FilePath),

    MemRef = api_test_memory:init(),

    ?assert(api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = [P1, P2],
            client_spec = ?CLIENT_SPEC_FOR_SPACE_2_SCENARIOS(Config),
            setup_fun = setup_fun(MemRef, Config, Guid),
            scenario_templates = [
                #scenario_template{
                    name = <<"Get QoS using rest endpoint">>,
                    type = rest,
                    prepare_args_fun = prepare_args_fun_rest(MemRef, {instance, get}),
                    validate_result_fun = validate_result_fun_rest(MemRef, {instance, get})
                },
                #scenario_template{
                    name = <<"Get QoS using gs endpoint">>,
                    type = gs,
                    prepare_args_fun = prepare_args_fun_gs(MemRef, {instance, get}),
                    validate_result_fun = validate_result_fun_gs(MemRef, {instance, get})
                }
            ],
            data_spec = #data_spec{
                bad_values = [{bad_id, <<"NonExistingRequirement">>, ?ERROR_NOT_FOUND}]
            }
        }
    ])),
    ok.


delete_qos_test(Config) ->
    [P2, P1] = ?config(op_worker_nodes, Config),
    SessIdP1 = ?USER_IN_BOTH_SPACES_SESS_ID(P1, Config),
    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    FilePath = filename:join(["/", ?SPACE_2, ?RANDOM_FILE_NAME()]),
    {ok, Guid} = lfm_test_utils:create_file(FileType, P1, SessIdP1, FilePath),

    MemRef = api_test_memory:init(),

    ?assert(api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = [P1, P2],
            client_spec = ?CLIENT_SPEC_FOR_SPACE_2_SCENARIOS(Config),
            setup_fun = setup_fun(MemRef, Config, Guid),
            verify_fun = verify_fun(MemRef, Config, {instance, delete}),
            scenario_templates = [
                #scenario_template{
                    name = <<"Delete QoS using rest endpoint">>,
                    type = rest,
                    prepare_args_fun = prepare_args_fun_rest(MemRef, {instance, delete}),
                    validate_result_fun = fun(_, {ok, ?HTTP_204_NO_CONTENT, _, #{}}) -> ok end
                },
                #scenario_template{
                    name = <<"Delete QoS using gs endpoint">>,
                    type = gs,
                    prepare_args_fun = prepare_args_fun_gs(MemRef, {instance, delete}),
                    validate_result_fun = fun(_, ok) -> ok end
                }
            ],
            data_spec = #data_spec{
                bad_values = [{bad_id, <<"NonExistingRequirement">>, ?ERROR_NOT_FOUND}]
            }
        }
    ])),
    ok.


get_qos_summary_test(Config) ->
    [P2, P1] = ?config(op_worker_nodes, Config),
    SessIdP1 = ?USER_IN_BOTH_SPACES_SESS_ID(P1, Config),
    SessIdP2 = ?USER_IN_BOTH_SPACES_SESS_ID(P2, Config),
    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    FilePath = filename:join(["/", ?SPACE_2, ?RANDOM_FILE_NAME()]),
    {ok, DirGuid} = lfm_test_utils:create_file(<<"dir">>, P1, SessIdP1, FilePath),
    {ok, Guid} = lfm_test_utils:create_file(FileType, P1, SessIdP1, filename:join(FilePath, ?RANDOM_FILE_NAME())),
    {ok, QosEntryIdInherited} = opt_qos:add_qos_entry(P1, SessIdP1, ?FILE_REF(DirGuid), <<"key=value">>, 8),
    {ok, QosEntryIdDirect} = opt_qos:add_qos_entry(P1, SessIdP1, ?FILE_REF(Guid), <<"key=value">>, 3),
    % wait for qos entries to be dbsynced to other provider
    ?assertMatch({ok, _}, opt_qos:get_qos_entry(P2, SessIdP2, QosEntryIdInherited), ?ATTEMPTS),
    ?assertMatch({ok, _}, opt_qos:get_qos_entry(P2, SessIdP2, QosEntryIdDirect), ?ATTEMPTS),
    {ok, ShareId} = opt_shares:create(P1, SessIdP1, ?FILE_REF(Guid), <<"share">>),

    MemRef = api_test_memory:init(),

    ?assert(api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = [P1, P2],
            client_spec = ?CLIENT_SPEC_FOR_SPACE_2_SCENARIOS(Config),
            setup_fun = fun() ->
                api_test_memory:set(MemRef, guid, Guid),
                api_test_memory:set(MemRef, qos, [QosEntryIdInherited, QosEntryIdDirect])
            end,
            scenario_templates = [
                #scenario_template{
                    name = <<"Get QoS summary using rest endpoint">>,
                    type = rest,
                    prepare_args_fun = prepare_args_fun_rest(MemRef, qos_summary),
                    validate_result_fun = validate_result_fun_rest(MemRef, qos_summary)
                },
                #scenario_template{
                    name = <<"Get QoS summary using gs endpoint">>,
                    type = gs,
                    prepare_args_fun = prepare_args_fun_gs(MemRef, qos_summary),
                    validate_result_fun = validate_result_fun_gs(MemRef, qos_summary)
                }
            ],
            data_spec = api_test_utils:replace_enoent_with_error_not_found_in_error_expectations(
                api_test_utils:add_file_id_errors_for_operations_not_available_in_share_mode(Guid, ShareId, undefined)
            )
        }
    ])),
    ok.


get_available_qos_parameters_test(Config) ->
    [P2, P1] = Workers = ?config(op_worker_nodes, Config),
    MemRef = api_test_memory:init(),
    api_test_memory:set(MemRef, providers, lists:usort(lists:map(fun(Worker) -> ?GET_DOMAIN_BIN(Worker) end, Workers))),

    ?assert(api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = [P1, P2],
            client_spec = ?CLIENT_SPEC_FOR_SPACE_2_SCENARIOS(Config),
            scenario_templates = [
                #scenario_template{
                    name = <<"Get QoS parameters using gs endpoint">>,
                    type = gs,
                    prepare_args_fun = prepare_args_fun_gs(MemRef, available_qos_parameters),
                    validate_result_fun = validate_result_fun_gs(MemRef, available_qos_parameters)
                }
            ]
        }
    ])),
    ok.


evaluate_qos_expression_test(Config) ->
    WorkerNodes = ?config(op_worker_nodes, Config),
    MemRef = api_test_memory:init(),
    api_test_memory:set(MemRef, space_id, ?SPACE_2),

    ?assert(api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = WorkerNodes,
            client_spec = ?CLIENT_SPEC_FOR_SPACE_2_SCENARIOS(Config),
            scenario_templates = [
                #scenario_template{
                    name = <<"Match storages by QoS expression using gs endpoint">>,
                    type = gs,
                    prepare_args_fun = prepare_args_fun_gs(MemRef, evaluate_qos_expression),
                    validate_result_fun = validate_result_fun_gs(MemRef, evaluate_qos_expression)
                },
                #scenario_template{
                    name = <<"Match storages by QoS expression using rest endpoint">>,
                    type = rest,
                    prepare_args_fun = prepare_args_fun_rest(MemRef, evaluate_qos_expression),
                    validate_result_fun = validate_result_fun_rest(MemRef, evaluate_qos_expression)
                }
            ],
            data_spec = #data_spec{
                required = [<<"expression">>],
                correct_values = #{
                    <<"expression">> => [<<"some_number = 8">>]
                },
                bad_values = [
                    {<<"expression">>, <<"invalid_qos_expression">>,
                        ?ERROR_INVALID_QOS_EXPRESSION(<<"syntax error before: ">>)}
                ]
            }
        }
    ])),
    ok.


get_qos_entry_audit_log(Config) ->
    [P2, P1] = ?config(op_worker_nodes, Config),
    SessIdP1 = ?USER_IN_BOTH_SPACES_SESS_ID(P1, Config),
    SessIdP2 = ?USER_IN_BOTH_SPACES_SESS_ID(P2, Config),
    FilePath = filename:join(["/", ?SPACE_2, ?RANDOM_FILE_NAME()]),
    {ok, Guid} = lfm_test_utils:create_file(<<"file">>, P1, SessIdP1, FilePath),
    ProviderId1 = ?GET_DOMAIN_BIN(P1),
    {ok, QosEntryId} = opt_qos:add_qos_entry(P1, SessIdP1, ?FILE_REF(Guid), <<"providerId=", ProviderId1/binary>>, 1),
    % wait for qos entries to be dbsynced to other provider
    ?assertMatch({ok, _}, opt_qos:get_qos_entry(P2, SessIdP2, QosEntryId), ?ATTEMPTS),
    ?assertEqual({ok, ?FULFILLED_QOS_STATUS}, opt_qos:check_qos_status(P1, SessIdP1, QosEntryId)),

    MemRef = api_test_memory:init(),

    SuiteSpec = #suite_spec{
        target_nodes = [P1],
        client_spec = ?CLIENT_SPEC_FOR_SPACE_2_SCENARIOS(Config),
        setup_fun = fun() ->
            api_test_memory:set(MemRef, qos_entry_id, QosEntryId)
        end,
        scenario_templates = [
            #scenario_template{
                name = <<"Get QoS audit log using rest endpoint">>,
                type = rest,
                prepare_args_fun = prepare_args_fun_rest(MemRef, qos_audit_log),
                validate_result_fun = validate_result_fun_rest(MemRef, qos_audit_log)
            }
        ],
        data_spec = #data_spec{
            optional = [<<"timestamp">>, <<"offset">>],
            correct_values = #{
                <<"timestamp">> => [7967656156000], % some time in the future
                <<"offset">> => [0]
            },
            bad_values = [
                {<<"timestamp">>, <<"aaa">>, ?ERROR_BAD_VALUE_INTEGER(<<"timestamp">>)},
                {<<"timestamp">>, -8, ?ERROR_BAD_VALUE_TOO_LOW(<<"timestamp">>, 0)},
                {<<"offset">>, <<"aaa">>, ?ERROR_BAD_VALUE_INTEGER(<<"offset">>)}
            ]
        }
    },
    api_test_memory:set(MemRef, audit_log_deleted, false),
    ?assert(api_test_runner:run_tests(Config, [SuiteSpec])),

    % simulate expiration of the audit log
    opw_test_rpc:call(P1, audit_log, delete, [QosEntryId]),
    api_test_memory:set(MemRef, audit_log_deleted, true),
    ?assert(api_test_runner:run_tests(Config, [SuiteSpec])).


get_qos_transfer_stats_collection_schema(Config) ->
    get_qos_transfer_stats_collection_schema_test_base(Config, ?BYTES_STATS),
    get_qos_transfer_stats_collection_schema_test_base(Config, ?FILES_STATS).

get_qos_transfer_stats_collection_schema_test_base(Config, Type) ->
    ?assert(api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = ?config(op_worker_nodes, Config),
            client_spec = ?CLIENT_SPEC_FOR_PUBLIC_ACCESS_SCENARIOS(Config),
            scenario_templates = [
                #scenario_template{
                    name = str_utils:format_bin("Get ~ts stats collection schema", [Type]),
                    type = gs,
                    prepare_args_fun = prepare_args_fun_gs(undefined, {qos_transfer_stats_collection, schema, Type}),
                    validate_result_fun = validate_result_fun_gs(undefined, {qos_transfer_stats_collection, schema, Type})
                }
            ]
        }
    ])).


get_qos_transfer_stats_collection_layout(Config) ->
    get_qos_transfer_stats_collection_layout_test_base(Config, ?BYTES_STATS),
    get_qos_transfer_stats_collection_layout_test_base(Config, ?FILES_STATS).

get_qos_transfer_stats_collection_layout_test_base(Config, CollectionType) ->
    [FileCreatingProvider, TransferringProvider] = TargetNodes = ?config(op_worker_nodes, Config),
    FileSize = rand:uniform(1000),
    QosEntryId = setup_preexisting_fulfilled_qos_causing_file_transfer(
        Config, ?SPACE_2, FileCreatingProvider, TransferringProvider, FileSize
    ),

    MemRef = api_test_memory:init(),
    api_test_memory:set(MemRef, file_creating_provider, FileCreatingProvider),

    lists:foreach(fun(TargetNode) ->
        ?assert(api_test_runner:run_tests(Config, [
            #suite_spec{
                target_nodes = [TargetNode],
                client_spec = ?CLIENT_SPEC_FOR_SPACE_2_SCENARIOS(Config),
                setup_fun = fun() ->
                    api_test_memory:set(MemRef, qos_entry_id, QosEntryId),
                    api_test_memory:set(MemRef, space_id, ?SPACE_2)
                end,
                scenario_templates = [
                    #scenario_template{
                        name = <<"Get time series collection layout of type '", CollectionType/binary, "' using gs api">>,
                        type = gs,
                        prepare_args_fun = prepare_args_fun_gs(MemRef, {qos_transfer_stats_collection, layout, CollectionType}),
                        validate_result_fun = validate_result_fun_gs(MemRef, {qos_transfer_stats_collection, layout, CollectionType})
                    }
                ],
                data_spec = #data_spec{
                    optional = [<<"mode">>],
                    correct_values = #{<<"mode">> => [<<"layout">>]},
                    bad_values = [
                        {<<"mode">>, mode, ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"mode">>, [<<"layout">>, <<"slice">>])}
                    ]
                }
            }
        ]))
    end, TargetNodes).


get_qos_transfer_stats_collection_slice(Config) ->
    get_qos_transfer_stats_collection_slice_test_base(Config, ?BYTES_STATS),
    get_qos_transfer_stats_collection_slice_test_base(Config, ?FILES_STATS).

get_qos_transfer_stats_collection_slice_test_base(Config, CollectionType) ->
    [FileCreatingProvider, TransferringProvider] = TargetNodes = ?config(op_worker_nodes, Config),
    FileSize = rand:uniform(1000),
    QosEntryId = setup_preexisting_fulfilled_qos_causing_file_transfer(
        Config, ?SPACE_2, FileCreatingProvider, TransferringProvider, FileSize
    ),

    TimestampFarInThePast = 123456789,

    MemRef = api_test_memory:init(),
    api_test_memory:set(MemRef, file_creating_provider, FileCreatingProvider),

    lists:foreach(fun(TargetNode) ->
        ?assert(api_test_runner:run_tests(Config, [
            #suite_spec{
                target_nodes = [TargetNode],
                client_spec = ?CLIENT_SPEC_FOR_SPACE_2_SCENARIOS(Config),
                setup_fun = fun() ->
                    api_test_memory:set(MemRef, qos_entry_id, QosEntryId),
                    api_test_memory:set(MemRef, file_size, FileSize),
                    api_test_memory:set(MemRef, timestamp_far_in_the_past, TimestampFarInThePast)
                end,
                scenario_templates = [
                    #scenario_template{
                        name = <<"Get time series collection slice of type '", CollectionType/binary, "' using gs api">>,
                        type = gs,
                        prepare_args_fun = prepare_args_fun_gs(MemRef, {qos_transfer_stats_collection, slice, CollectionType}),
                        validate_result_fun = validate_result_fun_gs(MemRef, {qos_transfer_stats_collection, slice, CollectionType})
                    }
                ],
                data_spec = #data_spec{
                    required = [<<"layout">>],
                    optional = [
                        <<"mode">>,
                        <<"startTimestamp">>,
                        <<"windowLimit">>
                    ],
                    correct_values = #{
                        <<"mode">> => [<<"slice">>],
                        <<"layout">> => [fun() ->
                            maps_utils:random_submap(expected_transfer_stats_layout(MemRef, TargetNode, ?SPACE_2, CollectionType))
                        end],
                        <<"startTimestamp">> => [global_clock:timestamp_seconds(), TimestampFarInThePast],
                        <<"windowLimit">> => [1, 500]
                    },
                    bad_values = [
                        {bad_id, <<"NonExistingRequirement">>, ?ERROR_FORBIDDEN},
                        {<<"layout">>, 8, ?ERROR_BAD_VALUE_JSON(<<"layout">>)},
                        {<<"layout">>, #{<<"a">> => [<<"b">>, <<"c">>]}, ?ERROR_TSC_MISSING_LAYOUT(#{<<"a">> => [<<"b">>, <<"c">>]})},
                        {<<"startTimestamp">>, <<"aaa">>, ?ERROR_BAD_VALUE_INTEGER(<<"startTimestamp">>)},
                        {<<"startTimestamp">>, -8, ?ERROR_BAD_VALUE_TOO_LOW(<<"startTimestamp">>, 0)},
                        {<<"windowLimit">>, <<"aaa">>, ?ERROR_BAD_VALUE_INTEGER(<<"windowLimit">>)},
                        {<<"windowLimit">>, 99999, ?ERROR_BAD_VALUE_NOT_IN_RANGE(<<"windowLimit">>, 1, 1000)}
                    ]
                }
            }
        ]))
    end, TargetNodes).


%%%===================================================================
%%% Setup functions
%%%===================================================================

setup_fun(MemRef, Config, Guid) ->
    fun() ->
        [P2, P1] = ?config(op_worker_nodes, Config),
        SessIdP1 = ?USER_IN_BOTH_SPACES_SESS_ID(P1, Config),
        SessIdP2 = ?USER_IN_BOTH_SPACES_SESS_ID(P2, Config),
        ReplicasNum = rand:uniform(10),
        Expression = <<"key=value & a=b">>,
        {ok, QosEntryId} = opt_qos:add_qos_entry(P1, SessIdP1, ?FILE_REF(Guid), Expression, ReplicasNum),
        % wait for qos entry to be dbsynced to other provider
        ?assertMatch({ok, _}, opt_qos:get_qos_entry(P2, SessIdP2, QosEntryId), ?ATTEMPTS),

        api_test_memory:set(MemRef, guid, Guid),
        api_test_memory:set(MemRef, qos, QosEntryId),
        api_test_memory:set(MemRef, replicas_num, ReplicasNum),
        api_test_memory:set(MemRef, expression, Expression)
    end.


%%%===================================================================
%%% Prepare args functions
%%%===================================================================

prepare_args_fun_rest(MemRef, {instance, create}) ->
    fun(#api_test_ctx{data = Data}) ->
        Guid = api_test_memory:get(MemRef, guid),

        #rest_args{
            method = post,
            path = <<"qos_requirements">>,
            body = json_utils:encode(maybe_inject_object_id(Data, Guid)),
            headers = #{?HDR_CONTENT_TYPE => <<"application/json">>}
        }
    end;

prepare_args_fun_rest(MemRef, {instance, Method}) ->
    fun(#api_test_ctx{data = Data}) ->
        QosEntryId = api_test_memory:get(MemRef, qos),

        {Id, _} = api_test_utils:maybe_substitute_bad_id(QosEntryId, Data),
        #rest_args{
            method = Method,
            path = <<"qos_requirements/", Id/binary>>
        }
    end;

prepare_args_fun_rest(MemRef, qos_summary) ->
    fun(#api_test_ctx{data = Data}) ->
        Guid = api_test_memory:get(MemRef, guid),

        {ok, ObjectId} = file_id:guid_to_objectid(Guid),
        {Id, _} = api_test_utils:maybe_substitute_bad_id(ObjectId, Data),
        #rest_args{
            method = get,
            path = <<"data/", Id/binary, "/qos/summary">>
        }
    end;

prepare_args_fun_rest(MemRef, evaluate_qos_expression) ->
    fun(#api_test_ctx{data = Data}) ->
        SpaceId = api_test_memory:get(MemRef, space_id),
        #rest_args{
            method = post,
            path = <<"spaces/", SpaceId/binary, "/evaluate_qos_expression">>,
            body = json_utils:encode(Data),
            headers = #{?HDR_CONTENT_TYPE => <<"application/json">>}
        }
    end;

prepare_args_fun_rest(MemRef, qos_audit_log) ->
    fun(#api_test_ctx{data = Data}) ->
        QosEntryId = api_test_memory:get(MemRef, qos_entry_id),

        #rest_args{
            method = get,
            path = http_utils:append_url_parameters(
                <<"qos_requirements/", QosEntryId/binary, "/audit_log">>, Data
            )
        }
    end.


prepare_args_fun_gs(MemRef, {instance, create}) ->
    fun(#api_test_ctx{data = Data}) ->
        Guid = api_test_memory:get(MemRef, guid),

        #gs_args{
            operation = create,
            gri = #gri{type = op_qos, aspect = instance, scope = private},
            data = maybe_inject_object_id(Data, Guid)
        }
    end;

prepare_args_fun_gs(MemRef, {instance, Method}) ->
    fun(#api_test_ctx{data = Data}) ->
        QosEntryId = api_test_memory:get(MemRef, qos),
        {Id, _} = api_test_utils:maybe_substitute_bad_id(QosEntryId, Data),
        #gs_args{
            operation = Method,
            gri = #gri{type = op_qos, id = Id, aspect = instance, scope = private}
        }
    end;

prepare_args_fun_gs(MemRef, qos_summary) ->
    fun(#api_test_ctx{data = Data}) ->
        Guid = api_test_memory:get(MemRef, guid),
        {Id, _} = api_test_utils:maybe_substitute_bad_id(Guid, Data),
        #gs_args{
            operation = get,
            gri = #gri{type = op_file, id = Id, aspect = qos_summary, scope = private}
        }
    end;

prepare_args_fun_gs(_MemRef, available_qos_parameters) ->
    fun(_) ->
        #gs_args{
            operation = get,
            gri = #gri{type = op_space, id = ?SPACE_2, aspect = available_qos_parameters, scope = private}
        }
    end;

prepare_args_fun_gs(_MemRef, evaluate_qos_expression) ->
    fun(#api_test_ctx{data = Data}) ->
        #gs_args{
            operation = create,
            gri = #gri{type = op_space, id = ?SPACE_2, aspect = evaluate_qos_expression, scope = private},
            data = Data
        }
    end;

prepare_args_fun_gs(_MemRef, {qos_transfer_stats_collection, schema, Type}) ->
    fun(#api_test_ctx{}) ->
        #gs_args{
            operation = get,
            gri = #gri{type = op_qos, id = undefined, aspect = {transfer_stats_collection_schema, Type}, scope = public}
        }
    end;

prepare_args_fun_gs(MemRef, {qos_transfer_stats_collection, layout, Type}) ->
    fun(#api_test_ctx{data = Data}) ->
        QosEntryId = api_test_memory:get(MemRef, qos_entry_id),
        {Id, UpdatedData} = api_test_utils:maybe_substitute_bad_id(QosEntryId, Data),
        #gs_args{
            operation = get,
            gri = #gri{type = op_qos, id = Id, aspect = {transfer_stats_collection, Type}, scope = private},
            data = UpdatedData
        }
    end;

prepare_args_fun_gs(MemRef, {qos_transfer_stats_collection, slice, Type}) ->
    fun(#api_test_ctx{data = Data}) ->
        QosEntryId = api_test_memory:get(MemRef, qos_entry_id),
        {Id, UpdatedData} = api_test_utils:maybe_substitute_bad_id(QosEntryId, Data),
        #gs_args{
            operation = get,
            gri = #gri{type = op_qos, id = Id, aspect = {transfer_stats_collection, Type}, scope = private},
            % mode is optional and so must be specified in the data spec, but if left out, defaults to layout,
            % hence the mode must be added here to the data for slice retrieval tests
            data = UpdatedData#{<<"mode">> => <<"slice">>}
        }
    end.


%%%===================================================================
%%% Validate result functions
%%%===================================================================

validate_result_fun_rest(MemRef, {instance, create}) ->
    fun(_ApiTestCtx, {ok, RespCode, _RespHeaders, RespBody}) ->
        ?assertEqual(?HTTP_201_CREATED, RespCode),
        QosEntryId = maps:get(<<"qosRequirementId">>, RespBody),
        api_test_memory:set(MemRef, qos, QosEntryId)
    end;

validate_result_fun_rest(MemRef, {instance, get}) ->
    fun(_, {ok, RespCode, _RespHeaders, RespBody}) ->
        Guid = api_test_memory:get(MemRef, guid),
        QosEntryId = api_test_memory:get(MemRef, qos),
        ReplicasNum = api_test_memory:get(MemRef, replicas_num),
        Expression = api_test_memory:get(MemRef, expression),

        {ok, ObjectId} = file_id:guid_to_objectid(Guid),
        ReceivedExpression = maps:get(<<"expression">>, RespBody),
        ?assertEqual(?HTTP_200_OK, RespCode),
        ?assertEqual(ObjectId, maps:get(<<"fileId">>, RespBody)),
        % Because of unambiguity of parenthesis and whitespaces check that received expression is equivalent
        ?assertEqual(qos_expression:parse(Expression), qos_expression:parse(ReceivedExpression)),
        ?assertEqual(ReplicasNum, maps:get(<<"replicasNum">>, RespBody)),
        ?assertEqual(QosEntryId, maps:get(<<"qosRequirementId">>, RespBody)),
        ok
    end;

validate_result_fun_rest(MemRef, qos_summary) ->
    fun(_, {ok, RespCode, _RespHeaders, RespBody}) ->
        [QosEntryIdInherited, QosEntryIdDirect] = api_test_memory:get(MemRef, qos),

        ?assertEqual(?HTTP_200_OK, RespCode),
        ?assertMatch(#{
            <<"requirements">> := #{
                QosEntryIdDirect := <<"impossible">>,
                QosEntryIdInherited := <<"impossible">>
            },
            <<"status">> := <<"impossible">>
        }, RespBody),
        ok
    end;

validate_result_fun_rest(MemRef, evaluate_qos_expression) ->
    fun(#api_test_ctx{node = Node, data = Data}, {ok, RespCode, _, Result}) ->
        ?assertEqual(?HTTP_200_OK, RespCode),
        SpaceId = api_test_memory:get(MemRef, space_id),
        Expression = qos_expression:parse(maps:get(<<"expression">>, Data)),
        check_evaluate_expression_result_storages(Node, SpaceId, Expression, maps:get(<<"matchingStorages">>, Result))
    end;

validate_result_fun_rest(MemRef, qos_audit_log) ->
    fun(_, {ok, RespCode, _RespHeaders, RespBody}) ->
        case api_test_memory:get(MemRef, audit_log_deleted) of
            true ->
                ?assertEqual(?HTTP_404_NOT_FOUND, RespCode),
                ?assertEqual(?ERROR_NOT_FOUND, errors:from_json(maps:get(<<"error">>, RespBody)));
            false ->
                ?assertEqual(?HTTP_200_OK, RespCode),
                ?assertMatch(#{
                    <<"isLast">> := true,
                    <<"logEntries">> := [
                        #{
                            <<"index">> := _,
                            <<"timestamp">> := _,
                            <<"severity">> := <<"info">>,
                            <<"content">> := #{
                                <<"status">> := <<"completed">>,
                                <<"fileId">> := _,
                                <<"description">> := <<"Local replica reconciled.">>
                            }
                        },
                        #{
                            <<"index">> := _,
                            <<"timestamp">> := _,
                            <<"severity">> := <<"info">>,
                            <<"content">> := #{
                                <<"status">> := <<"scheduled">>,
                                <<"fileId">> := _,
                                <<"description">> := <<"Remote replica differs, reconciliation started.">>
                            }
                        }
                    ]
                }, RespBody)
        end,
        ok
    end.


validate_result_fun_gs(MemRef, {instance, create}) ->
    fun(_ApiTestCtx, {ok, Result}) ->
        #gri{id = QosEntryId} = ?assertMatch(#gri{type = op_qos}, gri:deserialize(maps:get(<<"gri">>, Result))),
        api_test_memory:set(MemRef, qos, QosEntryId)
    end;

validate_result_fun_gs(MemRef, {instance, get}) ->
    fun(_, {ok, Result}) ->
        Guid = api_test_memory:get(MemRef, guid),
        QosEntryId = api_test_memory:get(MemRef, qos),
        ReplicasNum = api_test_memory:get(MemRef, replicas_num),
        Expression = api_test_memory:get(MemRef, expression),

        ExpressionRpn = qos_expression:to_rpn(qos_expression:parse(Expression)),
        ?assertMatch(#gri{type = op_file, id = Guid}, gri:deserialize(maps:get(<<"file">>, Result))),
        ?assertEqual(ExpressionRpn, maps:get(<<"expressionRpn">>, Result)),
        ?assertEqual(ReplicasNum, maps:get(<<"replicasNum">>, Result)),
        ?assertMatch(#gri{type = op_qos, id = QosEntryId}, gri:deserialize(maps:get(<<"gri">>, Result))),
        ok
    end;

validate_result_fun_gs(MemRef, qos_summary) ->
    fun(_, {ok, Result}) ->
        [QosEntryIdInherited, QosEntryIdDirect] = api_test_memory:get(MemRef, qos),

        ?assertMatch(#{
            <<"requirements">> := #{
                QosEntryIdDirect := <<"impossible">>,
                QosEntryIdInherited := <<"impossible">>
            }
        }, Result),
        ok
    end;

validate_result_fun_gs(MemRef, available_qos_parameters) ->
    fun(_, {ok, #{<<"qosParameters">> := AvailableQosParameters}}) ->
        Providers = api_test_memory:get(MemRef, providers),
        % parameters are set in env_desc.json
        ?assertMatch(#{
            <<"storageId">> := #{
                <<"stringValues">> := [<<"mntst1">>, <<"mntst2">>],
                <<"numberValues">> := []
            },
            <<"some_number">> := #{
                <<"stringValues">> := [],
                <<"numberValues">> := [8]
            },
            <<"providerId">> := #{
                <<"stringValues">> := Providers,
                <<"numberValues">> := []
            },
            <<"other_number">> := #{
                <<"stringValues">> := [],
                <<"numberValues">> := [64]
            },
            <<"geo">> := #{
                <<"stringValues">> := [<<"FR">>, <<"PL">>],
                <<"numberValues">> := []
            }
        }, AvailableQosParameters),
        ok
    end;

validate_result_fun_gs(MemRef, evaluate_qos_expression) ->
    fun(#api_test_ctx{data = Data, node = Node}, {ok, Result}) ->
        SpaceId = api_test_memory:get(MemRef, space_id),
        Expression = qos_expression:parse(maps:get(<<"expression">>, Data)),
        check_evaluate_expression_result_storages(Node, SpaceId, Expression, maps:get(<<"matchingStorages">>, Result)),
        ?assertEqual(maps:get(<<"expressionRpn">>, Result), qos_expression:to_rpn(Expression))
    end;

validate_result_fun_gs(_MemRef, {qos_transfer_stats_collection, schema, CollectionType}) ->
    fun(#api_test_ctx{}, {ok, Result}) ->
        ?assertEqual(jsonable_record:from_json(Result, time_series_collection_schema), case CollectionType of
            ?BYTES_STATS -> ?QOS_BYTES_STATS_COLLECTION_SCHEMA;
            ?FILES_STATS -> ?QOS_FILES_STATS_COLLECTION_SCHEMA
        end)
    end;

validate_result_fun_gs(MemRef, {qos_transfer_stats_collection, layout, CollectionType}) ->
    fun(#api_test_ctx{node = TargetNode}, Result) ->
        SpaceId = api_test_memory:get(MemRef, space_id),
        ?assertEqual({ok, #{
            <<"layout">> => expected_transfer_stats_layout(MemRef, TargetNode, SpaceId, CollectionType)}
        }, Result)
    end;

validate_result_fun_gs(MemRef, {qos_transfer_stats_collection, slice, CollectionType}) ->
    fun(#api_test_ctx{node = TargetNode, data = Data}, Result) ->
        TargetProviderId = ?GET_DOMAIN_BIN(TargetNode),
        FileSize = api_test_memory:get(MemRef, file_size),
        TimestampFarInThePast = api_test_memory:get(MemRef, timestamp_far_in_the_past),
        FileCreatingProviderId = ?GET_DOMAIN_BIN(api_test_memory:get(MemRef, file_creating_provider)),

        RequestedLayout = maps:get(<<"layout">>, Data),

        {ok, #{<<"slice">> := Slice}} = ?assertMatch({ok, #{<<"slice">> := _}}, Result),
        ?assertEqual(RequestedLayout, tsc_structure:to_layout(Slice)),

        StartTimestamp = maps:get(<<"startTimestamp">>, Data, undefined),
        ExpectingEmptyWindows = if
            StartTimestamp == TimestampFarInThePast ->
                true;
            TargetProviderId == FileCreatingProviderId ->
                % providers store only their own statistics concerning the incoming transfers
                % (statistics are not synchronized), hence providers that not replicate any blocks
                % concerning a QoS entry have empty statistics
                true;
            true ->
                false
        end,

        tsc_structure:foreach(fun(_TimeSeriesName, _MetricName, Windows) ->
            case {ExpectingEmptyWindows, CollectionType} of
                {true, _} ->
                    ?assertEqual([], Windows);
                {false, ?BYTES_STATS} ->
                    ?assertMatch([#{<<"value">> := FileSize, <<"timestamp">> := _}], Windows);
                {false, ?FILES_STATS} ->
                    [#{<<"value">> := Value}] = ?assertMatch([#{<<"value">> := _, <<"timestamp">> := _}], Windows),
                    % depending on the order of db-synced documents, the replicating provider may process
                    % the file one or two times (the latter in case it first synced an empty file location,
                    % then observed a changed file location)
                    ?assert(Value == 1 orelse Value == 2)
            end
        end, Slice)
    end.

%%%===================================================================
%%% Verify env functions
%%%===================================================================

verify_fun(MemRef, Config, {instance, create}) ->
    [P2, P1] = ?config(op_worker_nodes, Config),
    SessIdP1 = ?USER_IN_BOTH_SPACES_SESS_ID(P1, Config),
    SessIdP2 = ?USER_IN_BOTH_SPACES_SESS_ID(P2, Config),
    fun
        (expected_success, #api_test_ctx{data = Data}) ->
            Guid = api_test_memory:get(MemRef, guid),
            QosEntryId = api_test_memory:get(MemRef, qos),

            Uuid = file_id:guid_to_uuid(Guid),
            ReplicasNum = maps:get(<<"replicasNum">>, Data, 1),
            InfixExpression = maps:get(<<"expression">>, Data),
            Expression = qos_expression:parse(InfixExpression),
            {ok, EntryP2} = ?assertMatch({ok, _}, opt_qos:get_qos_entry(P2, SessIdP2, QosEntryId), ?ATTEMPTS),
            {ok, EntryP1} = ?assertMatch({ok, _}, opt_qos:get_qos_entry(P1, SessIdP1, QosEntryId), ?ATTEMPTS),
            ?assertEqual(EntryP1#qos_entry{traverse_reqs = #{}}, EntryP2#qos_entry{traverse_reqs = #{}}),
            ?assertMatch(#qos_entry{file_uuid = Uuid, expression = Expression, replicas_num = ReplicasNum}, EntryP1),
            true;
        (expected_failure, _) ->
            Guid = api_test_memory:get(MemRef, guid),
            ?assertEqual({ok, {#{}, #{}}}, opt_qos:get_effective_file_qos(P1, SessIdP1, ?FILE_REF(Guid))),
            ?assertEqual({ok, {#{}, #{}}}, opt_qos:get_effective_file_qos(P2, SessIdP2, ?FILE_REF(Guid))),
            true
    end;

verify_fun(MemRef, Config, {instance, delete}) ->
    [P2, P1] = ?config(op_worker_nodes, Config),
    SessIdP1 = ?USER_IN_BOTH_SPACES_SESS_ID(P1, Config),
    SessIdP2 = ?USER_IN_BOTH_SPACES_SESS_ID(P2, Config),
    fun
        (expected_success, _) ->
            QosEntryId = api_test_memory:get(MemRef, qos),
            ?assertEqual({error, not_found}, opt_qos:get_qos_entry(P2, SessIdP2, QosEntryId), ?ATTEMPTS),
            ?assertEqual({error, not_found}, opt_qos:get_qos_entry(P1, SessIdP1, QosEntryId), ?ATTEMPTS),
            true;
        (expected_failure, _) ->
            QosEntryId = api_test_memory:get(MemRef, qos),
            ?assertMatch({ok, _}, opt_qos:get_qos_entry(P2, SessIdP2, QosEntryId), ?ATTEMPTS),
            ?assertMatch({ok, _}, opt_qos:get_qos_entry(P1, SessIdP1, QosEntryId), ?ATTEMPTS),
            true
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
maybe_inject_object_id(Data, Guid) ->
    {ok, ObjectId} = file_id:guid_to_objectid(Guid),
    case maps:get(<<"fileId">>, Data, undefined) of
        objectId -> Data#{<<"fileId">> => ObjectId};
        _ -> Data
    end.


%% @private
check_evaluate_expression_result_storages(Node, SpaceId, Expression, Result) ->
    ExpectedStorages = opw_test_rpc:call(Node, qos_expression, get_matching_storages_in_space, [SpaceId, Expression]),
    lists:foreach(fun(StorageDetails) ->
        #{<<"id">> := Id, <<"name">> := Name, <<"providerId">> := ProviderId} = StorageDetails,
        ?assert(lists:member(Id, ExpectedStorages)),
        ?assertEqual(ProviderId, opw_test_rpc:call(Node, storage, fetch_provider_id_of_remote_storage, [Id, SpaceId])),
        ?assertEqual(Name, opw_test_rpc:call(Node, storage, fetch_name_of_remote_storage, [Id, SpaceId]))
    end, Result).


%% @private
setup_preexisting_fulfilled_qos_causing_file_transfer(Config, SpaceId, FileCreatingProvider, TransferringProvider, FileSize) ->
    SessIdP1 = ?USER_IN_BOTH_SPACES_SESS_ID(FileCreatingProvider, Config),
    SessIdP2 = ?USER_IN_BOTH_SPACES_SESS_ID(TransferringProvider, Config),
    FilePath = filename:join(["/", SpaceId, ?RANDOM_FILE_NAME()]),
    {ok, Guid} = lfm_test_utils:create_file(<<"file">>, FileCreatingProvider, SessIdP1, FilePath),
    lfm_test_utils:write_file(FileCreatingProvider, SessIdP1, Guid, {rand_content, FileSize}),
    TransferringProviderId = ?GET_DOMAIN_BIN(TransferringProvider),
    {ok, QosEntryId} = opt_qos:add_qos_entry(
        FileCreatingProvider, SessIdP1, ?FILE_REF(Guid), <<"providerId=", TransferringProviderId/binary>>, 1
    ),
    % wait for qos entries to be dbsynced to other provider and file content to be transferred
    ?assertMatch({ok, _}, opt_qos:get_qos_entry(TransferringProvider, SessIdP2, QosEntryId), ?ATTEMPTS),
    ?assertEqual({ok, ?FULFILLED_QOS_STATUS}, opt_qos:check_qos_status(FileCreatingProvider, SessIdP1, QosEntryId), ?ATTEMPTS),
    % wait for ?BYTES_STATS to be flushed - they are reported asynchronously by replica synchronizer and
    % may appear some time after QoS fulfillment
    ?assertMatch(
        {ok, #time_series_slice_get_result{
            slice = #{
                ?QOS_TOTAL_TIME_SERIES_NAME := #{
                    ?QOS_MINUTE_METRIC_NAME := [{_, _}]}
            }
        }},
        opw_test_rpc:call(TransferringProvider, qos_transfer_stats, browse, [
            QosEntryId, ?BYTES_STATS, #time_series_slice_get_request{
                layout = #{?QOS_TOTAL_TIME_SERIES_NAME => [?QOS_MINUTE_METRIC_NAME]}
            }
        ]),
        ?ATTEMPTS
    ),
    QosEntryId.


%% @private
expected_transfer_stats_layout(MemRef, Node, SpaceId, CollectionType) ->
    ProviderId = ?GET_DOMAIN_BIN(Node),
    FileCreatingProviderId = ?GET_DOMAIN_BIN(api_test_memory:get(MemRef, file_creating_provider)),

    PerStorageTSNames = case ProviderId of
        FileCreatingProviderId ->
            % time series per storage should be created only on the transferring provider
            [];
        _ ->
            % bytes stats are gathered per source storage, while files stats are gathered per target storage
            ProviderOfIncludedStorages = case CollectionType of
                ?BYTES_STATS -> FileCreatingProviderId;
                ?FILES_STATS -> ProviderId
            end,
            {ok, IncludedStorages} = opw_test_rpc:call(
                Node, space_logic, get_provider_storages, [SpaceId, ProviderOfIncludedStorages]
            ),
            [?QOS_STORAGE_TIME_SERIES_NAME(StorageId) || StorageId <- maps:keys(IncludedStorages)]
    end,
    maps_utils:generate_from_list(fun(TimeSeriesName) ->
        {TimeSeriesName, maps:keys(?QOS_STATS_METRICS)}
    end, [?QOS_TOTAL_TIME_SERIES_NAME | PerStorageTSNames]).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        NewConfig1 = initializer:setup_storage(NewConfig),
        NewConfig2 = initializer:create_test_users_and_spaces(
            ?TEST_FILE(NewConfig1, "env_desc.json"),
            NewConfig1
        ),
        initializer:mock_auth_manager(NewConfig2, _CheckIfUserIsSupported = true),
        ssl:start(),
        application:ensure_all_started(hackney),
        NewConfig2
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].


end_per_suite(Config) ->
    application:stop(hackney),
    ssl:stop(),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:teardown_storage(Config).


init_per_testcase(_Case, Config) ->
    initializer:mock_share_logic(Config),
    ct:timetrap({minutes, 30}),
    lfm_proxy:init(Config).


end_per_testcase(_Case, Config) ->
    initializer:unmock_share_logic(Config),
    lfm_proxy:teardown(Config).

