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
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/http/codes.hrl").
-include_lib("ctool/include/graph_sync/gri.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    create_qos_test/1,
    get_qos_test/1,
    delete_qos_test/1,
    get_qos_summary_test/1
]).


all() -> [
    create_qos_test,
    get_qos_test,
    delete_qos_test,
    get_qos_summary_test
].

create_qos_test(Config) ->
    [P2, P1] = ?config(op_worker_nodes, Config),
    SessIdP1 = ?USER_IN_BOTH_SPACES_SESS_ID(P1, Config),
    SessIdP2 = ?USER_IN_BOTH_SPACES_SESS_ID(P2, Config),
    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    
    {ok, FileToShareGuid} = api_test_utils:create_file(
        FileType, P1, SessIdP1, filename:join(["/", ?SPACE_2, ?RANDOM_FILE_NAME()])),
    ?assertMatch({ok, _}, lfm_proxy:stat(P2, SessIdP2, {guid, FileToShareGuid}), 20),
    {ok, ShareId} = lfm_proxy:create_share(P1, SessIdP1, {guid, FileToShareGuid}, <<"share">>),

    MemRef = api_test_memory:init(),

    SetupFun = fun() ->
        FilePath = filename:join(["/", ?SPACE_2, ?RANDOM_FILE_NAME()]),
        {ok, Guid} = api_test_utils:create_file(FileType, P1, SessIdP1, FilePath),
        ?assertMatch({ok, _}, lfm_proxy:stat(P2, SessIdP2, {guid, Guid}), 20),
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
            verify_fun = verify_fun(MemRef, Config, create),
            scenario_templates = [
                #scenario_template{
                    name = <<"Create QoS using rest endpoint">>,
                    type = rest,
                    prepare_args_fun = prepare_args_fun_rest(MemRef, create),
                    validate_result_fun = validate_result_fun_rest(MemRef, create)
                },
                #scenario_template{
                    name = <<"Create QoS using gs endpoint">>,
                    type = gs,
                    prepare_args_fun = prepare_args_fun_gs(MemRef, create),
                    validate_result_fun = validate_result_fun_gs(MemRef, create)
                }
            ],
            randomly_select_scenarios = true,
            data_spec = 
                api_test_utils:add_cdmi_id_errors_for_operations_not_available_in_share_mode(
                    FileToShareGuid, ?SPACE_2, ShareId, CreateDataSpec
                )
        }
    ])),
    ok.


get_qos_test(Config) ->
    [P2, P1] = ?config(op_worker_nodes, Config),
    SessIdP1 = ?USER_IN_BOTH_SPACES_SESS_ID(P1, Config),
    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    FilePath = filename:join(["/", ?SPACE_2, ?RANDOM_FILE_NAME()]),
    {ok, Guid} = api_test_utils:create_file(FileType, P1, SessIdP1, FilePath),

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
                    prepare_args_fun = prepare_args_fun_rest(MemRef, get),
                    validate_result_fun = validate_result_fun_rest(MemRef, get)
                },
                #scenario_template{
                    name = <<"Get QoS using gs endpoint">>,
                    type = gs,
                    prepare_args_fun = prepare_args_fun_gs(MemRef, get),
                    validate_result_fun = validate_result_fun_gs(MemRef, get)
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
    {ok, Guid} = api_test_utils:create_file(FileType, P1, SessIdP1, FilePath),

    MemRef = api_test_memory:init(),

    ?assert(api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = [P1, P2],
            client_spec = ?CLIENT_SPEC_FOR_SPACE_2_SCENARIOS(Config),
            setup_fun = setup_fun(MemRef, Config, Guid),
            verify_fun = verify_fun(MemRef, Config, delete),
            scenario_templates = [
                #scenario_template{
                    name = <<"Delete QoS using rest endpoint">>,
                    type = rest,
                    prepare_args_fun = prepare_args_fun_rest(MemRef, delete),
                    validate_result_fun = fun(_, {ok, ?HTTP_204_NO_CONTENT, _, #{}}) -> ok end
                },
                #scenario_template{
                    name = <<"Delete QoS using gs endpoint">>,
                    type = gs,
                    prepare_args_fun = prepare_args_fun_gs(MemRef, delete),
                    validate_result_fun = fun(_, {ok, undefined}) -> ok end
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
    {ok, DirGuid} = api_test_utils:create_file(<<"dir">>, P1, SessIdP1, FilePath),
    {ok, Guid} = api_test_utils:create_file(FileType, P1, SessIdP1, filename:join(FilePath, ?RANDOM_FILE_NAME())),
    {ok, QosEntryIdInherited} = lfm_proxy:add_qos_entry(P1, SessIdP1, {guid, DirGuid}, <<"key=value">>, 8),
    {ok, QosEntryIdDirect} = lfm_proxy:add_qos_entry(P1, SessIdP1, {guid, Guid}, <<"key=value">>, 3),
    % wait for qos entries to be dbsynced to other provider
    ?assertMatch({ok, _}, lfm_proxy:get_qos_entry(P2, SessIdP2, QosEntryIdInherited), 20),
    ?assertMatch({ok, _}, lfm_proxy:get_qos_entry(P2, SessIdP2, QosEntryIdDirect), 20),
    {ok, ShareId} = lfm_proxy:create_share(P1, SessIdP1, {guid, Guid}, <<"share">>),

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
            data_spec = api_test_utils:add_file_id_errors_for_operations_not_available_in_share_mode(Guid, ShareId, undefined)
        }
    ])),
    ok.


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
        {ok, QosEntryId} = lfm_proxy:add_qos_entry(P1, SessIdP1, {guid, Guid}, Expression, ReplicasNum),
        % wait for qos entry to be dbsynced to other provider
        ?assertMatch({ok, _}, lfm_proxy:get_qos_entry(P2, SessIdP2, QosEntryId), 20),

        api_test_memory:set(MemRef, guid, Guid),
        api_test_memory:set(MemRef, qos, QosEntryId),
        api_test_memory:set(MemRef, replicas_num, ReplicasNum),
        api_test_memory:set(MemRef, expression, Expression)
    end.


%%%===================================================================
%%% Prepare args functions
%%%===================================================================

prepare_args_fun_rest(MemRef, create) ->
    fun(#api_test_ctx{data = Data}) ->
        Guid = api_test_memory:get(MemRef, guid),

        #rest_args{
            method = post,
            path = <<"qos_requirements">>,
            body = json_utils:encode(maybe_inject_object_id(Data, Guid)),
            headers = #{<<"content-type">> => <<"application/json">>}
        } 
    end;

prepare_args_fun_rest(MemRef, qos_summary) ->
    fun(#api_test_ctx{data = Data}) ->
        Guid = api_test_memory:get(MemRef, guid),

        {ok, ObjectId} = file_id:guid_to_objectid(Guid),
        {Id, _} = api_test_utils:maybe_substitute_bad_id(ObjectId, Data),
        #rest_args{
            method = get,
            path = <<"data/", Id/binary, "/qos_summary">>
        } 
    end;

prepare_args_fun_rest(MemRef, Method) ->
    fun(#api_test_ctx{data = Data}) ->
        QosEntryId = api_test_memory:get(MemRef, qos),

        {Id, _} = api_test_utils:maybe_substitute_bad_id(QosEntryId, Data),
        #rest_args{
            method = Method,
            path = <<"qos_requirements/", Id/binary>>
        } 
    end.


prepare_args_fun_gs(MemRef, create) ->
    fun(#api_test_ctx{data = Data}) ->
        Guid = api_test_memory:get(MemRef, guid),

        #gs_args{
            operation = create,
            gri = #gri{type = op_qos, aspect = instance, scope = private},
            data = maybe_inject_object_id(Data, Guid)
        } 
    end;

prepare_args_fun_gs(MemRef, qos_summary) ->
    fun(#api_test_ctx{data = Data}) ->
        Guid = api_test_memory:get(MemRef, guid),
        {Id, _} = api_test_utils:maybe_substitute_bad_id(Guid, Data),
        #gs_args{
            operation = get,
            gri = #gri{type = op_file, id = Id, aspect = file_qos_summary, scope = private}
        } 
    end;

prepare_args_fun_gs(MemRef, Method) ->
    fun(#api_test_ctx{data = Data}) ->
        QosEntryId = api_test_memory:get(MemRef, qos),
        {Id, _} = api_test_utils:maybe_substitute_bad_id(QosEntryId, Data),
        #gs_args{
            operation = Method,
            gri = #gri{type = op_qos, id = Id, aspect = instance, scope = private}
        } 
    end.


%%%===================================================================
%%% Validate result functions
%%%===================================================================

validate_result_fun_rest(MemRef, create) ->
    fun(_ApiTestCtx, {ok, RespCode, _RespHeaders, RespBody}) ->
        ?assertEqual(?HTTP_201_CREATED, RespCode),
        QosEntryId = maps:get(<<"qosRequirementId">>, RespBody),
        api_test_memory:set(MemRef, qos, QosEntryId)
    end;

validate_result_fun_rest(MemRef, get) ->
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
    end.


validate_result_fun_gs(MemRef, create) ->
    fun(_ApiTestCtx, {ok, Result}) ->
        #gri{id = QosEntryId} = ?assertMatch(#gri{type = op_qos}, gri:deserialize(maps:get(<<"gri">>, Result))),
        api_test_memory:set(MemRef, qos, QosEntryId)
    end;

validate_result_fun_gs(MemRef, get) ->
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
    end.


%%%===================================================================
%%% Verify env functions
%%%===================================================================

verify_fun(MemRef, Config, create) ->
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
            {ok, EntryP2} = ?assertMatch({ok, _}, lfm_proxy:get_qos_entry(P2, SessIdP2, QosEntryId), 20),
            {ok, EntryP1} = ?assertMatch({ok, _}, lfm_proxy:get_qos_entry(P1, SessIdP1, QosEntryId), 20),
            ?assertEqual(EntryP1#qos_entry{traverse_reqs = #{}}, EntryP2#qos_entry{traverse_reqs = #{}}),
            ?assertMatch(#qos_entry{file_uuid = Uuid, expression = Expression, replicas_num = ReplicasNum}, EntryP1),
            true;
        (expected_failure, _) ->
            Guid = api_test_memory:get(MemRef, guid),
            ?assertEqual({ok, {#{}, #{}}}, lfm_proxy:get_effective_file_qos(P1, SessIdP1, {guid, Guid})),
            ?assertEqual({ok, {#{}, #{}}}, lfm_proxy:get_effective_file_qos(P2, SessIdP2, {guid, Guid})),
            true
    end;

verify_fun(MemRef, Config, delete) ->
    [P2, P1] = ?config(op_worker_nodes, Config),
    SessIdP1 = ?USER_IN_BOTH_SPACES_SESS_ID(P1, Config),
    SessIdP2 = ?USER_IN_BOTH_SPACES_SESS_ID(P2, Config),
    fun
        (expected_success, _) ->
            QosEntryId = api_test_memory:get(MemRef, qos),
            ?assertEqual({error, not_found}, lfm_proxy:get_qos_entry(P2, SessIdP2, QosEntryId), 20),
            ?assertEqual({error, not_found}, lfm_proxy:get_qos_entry(P1, SessIdP1, QosEntryId), 20),
            true;
        (expected_failure, _) ->
            QosEntryId = api_test_memory:get(MemRef, qos),
            ?assertMatch({ok, _}, lfm_proxy:get_qos_entry(P2, SessIdP2, QosEntryId), 20),
            ?assertMatch({ok, _}, lfm_proxy:get_qos_entry(P1, SessIdP1, QosEntryId), 20),
            true
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

maybe_inject_object_id(Data, Guid) ->
    {ok, ObjectId} = file_id:guid_to_objectid(Guid),
    case maps:get(<<"fileId">>, Data, undefined) of
        objectId -> Data#{<<"fileId">> => ObjectId};
        _ -> Data
    end.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        NewConfig1 = [{space_storage_mock, false} | NewConfig],
        NewConfig2 = initializer:setup_storage(NewConfig1),
        NewConfig3 = initializer:create_test_users_and_spaces(
            ?TEST_FILE(NewConfig2, "env_desc.json"),
            NewConfig2
        ),
        initializer:mock_auth_manager(NewConfig3, _CheckIfUserIsSupported = true),
        application:start(ssl),
        hackney:start(),
        NewConfig3
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].


end_per_suite(Config) ->
    hackney:stop(),
    application:stop(ssl),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:teardown_storage(Config).


init_per_testcase(_Case, Config) ->
    initializer:mock_share_logic(Config),
    ct:timetrap({minutes, 30}),
    lfm_proxy:init(Config).


end_per_testcase(_Case, Config) ->
    initializer:unmock_share_logic(Config),
    lfm_proxy:teardown(Config).

