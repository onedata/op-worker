%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This file contains tests concerning OqS basic API (REST + gs).
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
    
    SetupFun = fun() ->
        FilePath = filename:join(["/", ?SPACE_2, ?RANDOM_FILE_NAME()]),
        {ok, Guid} = api_test_utils:create_file(FileType, P1, SessIdP1, FilePath),
        ?assertMatch({ok, _}, lfm_proxy:stat(P2, SessIdP2, {guid, Guid}), 20),
        #{guid => Guid}
    end, 
    
    ?assert(api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = [P1, P2],
            client_spec = ?CLIENT_SPEC_FOR_SPACE_2_SCENARIOS(Config),
            setup_fun = SetupFun,
            verify_fun = verify_fun(Config, create),
            scenario_templates = [
                #scenario_template{
                    name = <<"Create QoS using rest endpoint">>,
                    type = rest,
                    prepare_args_fun = prepare_args_fun_rest(create),
                    validate_result_fun = validate_result_fun_rest(create)
                },
                #scenario_template{
                    name = <<"Create QoS using gs endpoint">>,
                    type = gs,
                    prepare_args_fun = prepare_args_fun_gs(create),
                    validate_result_fun = validate_result_fun_gs(create)
                }
            ],
            data_spec = #data_spec{
                required = [<<"expression">>, <<"fileId">>],
                optional = [<<"replicasNum">>],
                correct_values = #{
                    <<"expression">> => [
                        <<"key=value">>,
                        <<"key=value | anotherKey=anotherValue">>,
                        <<"key=value & anotherKey=anotherValue">>,
                        <<"key=value - anotherKey=anotherValue">>,
                        <<"(key=value - anotherKey=anotherValue)">>,
                        <<"(key=value - anotherKey=anotherValue) | a=b">>,
                        <<"anyStorage">>
                    ],
                    <<"fileId">> => [objectId],
                    <<"replicasNum">> => [rand:uniform(10)]
                },
                bad_values = [
                    {<<"expression">>, <<"aaa">>, ?ERROR_INVALID_QOS_EXPRESSION},
                    {<<"expression">>, <<"key | value">>, ?ERROR_INVALID_QOS_EXPRESSION},
                    {<<"expression">>, <<"(key = value">>, ?ERROR_INVALID_QOS_EXPRESSION},
                    {<<"expression">>, <<"key = value)">>, ?ERROR_INVALID_QOS_EXPRESSION},
                    {<<"fileId">>, <<"gibberish">>, ?ERROR_BAD_VALUE_IDENTIFIER(<<"fileId">>)},
                    {<<"replicasNum">>, <<"aaa">>, ?ERROR_BAD_VALUE_INTEGER(<<"replicasNum">>)},
                    {<<"replicasNum">>, 0, ?ERROR_BAD_VALUE_TOO_LOW(<<"replicasNum">>, 1)}
                ]
            }
        }
    ])),
    ok.


get_qos_test(Config) ->
    [P2, P1] = ?config(op_worker_nodes, Config),
    SessIdP1 = ?USER_IN_BOTH_SPACES_SESS_ID(P1, Config),
    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    FilePath = filename:join(["/", ?SPACE_2, ?RANDOM_FILE_NAME()]),
    {ok, Guid} = api_test_utils:create_file(FileType, P1, SessIdP1, FilePath),
    
    ?assert(api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = [P1, P2],
            client_spec = ?CLIENT_SPEC_FOR_SPACE_2_SCENARIOS(Config),
            setup_fun = setup_fun(Config, Guid),
            scenario_templates = [
                #scenario_template{
                    name = <<"Get QoS using rest endpoint">>,
                    type = rest,
                    prepare_args_fun = prepare_args_fun_rest(get),
                    validate_result_fun = validate_result_fun_rest(get)
                },
                #scenario_template{
                    name = <<"Get QoS using gs endpoint">>,
                    type = gs,
                    prepare_args_fun = prepare_args_fun_gs(get),
                    validate_result_fun = validate_result_fun_gs(get)
                }
            ],
            data_spec = undefined
        }
    ])),
    ok.


delete_qos_test(Config) ->
    [P2, P1] = ?config(op_worker_nodes, Config),
    SessIdP1 = ?USER_IN_BOTH_SPACES_SESS_ID(P1, Config),
    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    FilePath = filename:join(["/", ?SPACE_2, ?RANDOM_FILE_NAME()]),
    {ok, Guid} = api_test_utils:create_file(FileType, P1, SessIdP1, FilePath),
    
    ?assert(api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = [P1, P2],
            client_spec = ?CLIENT_SPEC_FOR_SPACE_2_SCENARIOS(Config),
            verify_fun = verify_fun(Config, delete),
            setup_fun = setup_fun(Config, Guid),
            scenario_templates = [
                #scenario_template{
                    name = <<"Delete QoS using rest endpoint">>,
                    type = rest,
                    prepare_args_fun = prepare_args_fun_rest(delete),
                    validate_result_fun = fun(_, {ok, ?HTTP_204_NO_CONTENT, #{}}) -> ok end
                },
                #scenario_template{
                    name = <<"Delete QoS using gs endpoint">>,
                    type = gs,
                    prepare_args_fun = prepare_args_fun_gs(delete),
                    validate_result_fun = fun(_, {ok, undefined}) -> ok end
                }
            ],
            data_spec = undefined
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
    {ok, QosEntryIdInherited} = lfm_proxy:add_qos_entry(P1, SessIdP1, {guid, DirGuid}, [<<"key=value">>], 8),
    {ok, QosEntryIdDirect} = lfm_proxy:add_qos_entry(P1, SessIdP1, {guid, Guid}, [<<"key=value">>], 3),
    % wait for qos entries to be dbsynced to other provider
    ?assertMatch({ok, _}, lfm_proxy:get_qos_entry(P2, SessIdP2, QosEntryIdInherited), 20),
    ?assertMatch({ok, _}, lfm_proxy:get_qos_entry(P2, SessIdP2, QosEntryIdDirect), 20),
    
    ?assert(api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = [P1, P2],
            client_spec = ?CLIENT_SPEC_FOR_SPACE_2_SCENARIOS(Config),
            setup_fun = fun() -> #{qos => [QosEntryIdInherited, QosEntryIdDirect], guid =>Guid} end,
            scenario_templates = [
                #scenario_template{
                    name = <<"Get QoS summary using rest endpoint">>,
                    type = rest,
                    prepare_args_fun = prepare_args_fun_rest(qos_summary),
                    validate_result_fun = validate_result_fun_rest(qos_summary)
                },
                #scenario_template{
                    name = <<"Get QoS summary using gs endpoint">>,
                    type = gs,
                    prepare_args_fun = prepare_args_fun_gs(qos_summary),
                    validate_result_fun = validate_result_fun_gs(qos_summary)
                }
            ],
            data_spec = undefined
        }
    ])),
    ok.


%%%===================================================================
%%% Setup functions
%%%===================================================================

setup_fun(Config, Guid) ->
    fun() ->
        [P2, P1] = ?config(op_worker_nodes, Config),
        SessIdP1 = ?USER_IN_BOTH_SPACES_SESS_ID(P1, Config),
        SessIdP2 = ?USER_IN_BOTH_SPACES_SESS_ID(P2, Config),
        ReplicasNum = rand:uniform(10),
        ExpressionRpn = [<<"key=value">>, <<"a=b">>, <<"&">>],
        {ok, QosEntryId} = lfm_proxy:add_qos_entry(P1, SessIdP1, {guid, Guid}, ExpressionRpn, ReplicasNum),
        % wait for qos entry to be dbsynced to other provider
        ?assertMatch({ok, _}, lfm_proxy:get_qos_entry(P2, SessIdP2, QosEntryId), 20),
        #{
            qos => QosEntryId,
            guid => Guid,
            replicas_num => ReplicasNum,
            expression_rpn => ExpressionRpn
        }
    end.


%%%===================================================================
%%% Prepare args functions
%%%===================================================================

prepare_args_fun_rest(create) ->
    fun(#api_test_ctx{data = Data, env = #{guid := Guid}}) ->
        #rest_args{
            method = post,
            path = <<"qos_requirement">>,
            body = json_utils:encode(maybe_inject_object_id(Data, Guid)),
            headers = #{<<"content-type">> => <<"application/json">>}
        } 
    end;

prepare_args_fun_rest(qos_summary) ->
    fun(#api_test_ctx{env = #{guid := Guid}}) ->
        {ok, ObjectId} = file_id:guid_to_objectid(Guid),
        #rest_args{
            method = get,
            path = <<"data/", ObjectId/binary, "/qos_summary">>
        } 
    end;

prepare_args_fun_rest(Method) ->
    fun(#api_test_ctx{env = #{qos := QosEntryId}}) -> 
        #rest_args{
            method = Method,
            path = <<"qos_requirement/", QosEntryId/binary>>
        } 
    end.


prepare_args_fun_gs(create) ->
    fun(#api_test_ctx{data = Data, env = #{guid := Guid}}) ->
        #gs_args{
            operation = create,
            gri = #gri{type = op_qos, aspect = instance, scope = private},
            data = maybe_inject_object_id(Data, Guid)
        } 
    end;

prepare_args_fun_gs(qos_summary) ->
    fun(#api_test_ctx{env = #{guid := Guid}}) -> 
        #gs_args{
            operation = get,
            gri = #gri{type = op_file, id = Guid, aspect = file_qos_summary, scope = private}
        } 
    end;

prepare_args_fun_gs(Method) ->
    fun(#api_test_ctx{env = #{qos := QosEntryId}}) -> 
        #gs_args{
            operation = Method,
            gri = #gri{type = op_qos, id = QosEntryId, aspect = instance, scope = private}
        } 
    end.


%%%===================================================================
%%% Validate result functions
%%%===================================================================

validate_result_fun_rest(create) ->
    fun(#api_test_ctx{env = Env} = ApiTestCtx, {ok, RespCode, RespBody}) ->
        ?assertEqual(?HTTP_201_CREATED, RespCode),
        QosEntryId = maps:get(<<"qosRequirementId">>, RespBody),
        {ok, ApiTestCtx#api_test_ctx{env = Env#{qos => QosEntryId}}}
    end;

validate_result_fun_rest(get) ->
    fun(#api_test_ctx{env = Env}, {ok, RespCode, RespBody}) ->
        #{
            guid := Guid,
            qos := QosEntryId,
            replicas_num := ReplicasNum,
            expression_rpn := ExpressionRpn
        } = Env,
        {ok, ObjectId} = file_id:guid_to_objectid(Guid),
        ?assertEqual(?HTTP_200_OK, RespCode),
        ?assertEqual(ObjectId, maps:get(<<"fileId">>, RespBody)),
        ?assertEqual(qos_expression:rpn_to_infix(ExpressionRpn), {ok, maps:get(<<"expression">>, RespBody)}),
        ?assertEqual(ReplicasNum, maps:get(<<"replicasNum">>, RespBody)),
        ?assertEqual(QosEntryId, maps:get(<<"qosRequirementId">>, RespBody)),
        ok
    end;

validate_result_fun_rest(qos_summary) ->
    fun(#api_test_ctx{env = #{qos := [QosEntryIdInherited, QosEntryIdDirect]}}, {ok, RespCode, RespBody}) ->
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


validate_result_fun_gs(create) ->
    fun(#api_test_ctx{env = Env} = ApiTestCtx, {ok, Result}) ->
        #gri{id = QosEntryId} = ?assertMatch(#gri{type = op_qos}, gri:deserialize(maps:get(<<"gri">>, Result))),
        {ok, ApiTestCtx#api_test_ctx{env = Env#{qos => QosEntryId}}}
    end;

validate_result_fun_gs(get) ->
    fun(#api_test_ctx{env = Env}, {ok, Result}) ->
        #{
            guid := Guid,
            qos := QosEntryId,
            replicas_num := ReplicasNum,
            expression_rpn := ExpressionRpn
        } = Env,
        ?assertMatch(#gri{type = op_file, id = Guid}, gri:deserialize(maps:get(<<"file">>, Result))),
        ?assertEqual(ExpressionRpn, maps:get(<<"expressionRpn">>, Result)),
        ?assertEqual(ReplicasNum, maps:get(<<"replicasNum">>, Result)),
        ?assertMatch(#gri{type = op_qos, id = QosEntryId}, gri:deserialize(maps:get(<<"gri">>, Result))),
        ok
    end;

validate_result_fun_gs(qos_summary) ->
    fun(#api_test_ctx{env = #{qos := [QosEntryIdInherited, QosEntryIdDirect]}}, {ok, Result}) ->
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

verify_fun(Config, create) ->
    [P2, P1] = ?config(op_worker_nodes, Config),
    SessIdP1 = ?USER_IN_BOTH_SPACES_SESS_ID(P1, Config),
    SessIdP2 = ?USER_IN_BOTH_SPACES_SESS_ID(P2, Config),
    fun (expected_success, #api_test_ctx{data = Data, env = #{guid := Guid, qos := QosEntryId}}) ->
            Uuid = file_id:guid_to_uuid(Guid),
            ReplicasNum = maps:get(<<"replicasNum">>, Data, 1),
            Expression = maps:get(<<"expression">>, Data),
            ExpressionRpn = qos_expression:ensure_rpn(Expression),
            {ok, EntryP2} = ?assertMatch({ok, _}, lfm_proxy:get_qos_entry(P2, SessIdP2, QosEntryId), 20),
            {ok, EntryP1} = ?assertMatch({ok, _}, lfm_proxy:get_qos_entry(P1, SessIdP1, QosEntryId), 20),
            ?assertEqual(EntryP1#qos_entry{traverse_reqs = #{}}, EntryP2#qos_entry{traverse_reqs = #{}}),
            ?assertMatch(#qos_entry{file_uuid = Uuid, expression = ExpressionRpn, replicas_num = ReplicasNum}, EntryP1),
            true;
        (expected_failure, #api_test_ctx{env = #{guid := Guid}}) ->
            ?assertEqual({ok, {#{}, #{}}}, lfm_proxy:get_effective_file_qos(P1, SessIdP1, {guid, Guid})),
            ?assertEqual({ok, {#{}, #{}}}, lfm_proxy:get_effective_file_qos(P2, SessIdP2, {guid, Guid})),
            true
    end;

verify_fun(Config, delete) ->
    [P2, P1] = ?config(op_worker_nodes, Config),
    SessIdP1 = ?USER_IN_BOTH_SPACES_SESS_ID(P1, Config),
    SessIdP2 = ?USER_IN_BOTH_SPACES_SESS_ID(P2, Config),
    fun (expected_success, #api_test_ctx{env = #{qos := QosEntryId}}) ->
        ?assertEqual({error, not_found}, lfm_proxy:get_qos_entry(P2, SessIdP2, QosEntryId), 20),
        ?assertEqual({error, not_found}, lfm_proxy:get_qos_entry(P1, SessIdP1, QosEntryId), 20),
        true;
        (expected_failure, #api_test_ctx{env = #{qos := QosEntryId}}) ->
            ?assertMatch({ok, _}, lfm_proxy:get_qos_entry(P2, SessIdP2, QosEntryId), 20),
            ?assertMatch({ok, _}, lfm_proxy:get_qos_entry(P1, SessIdP1, QosEntryId), 20),
            true
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

maybe_inject_object_id(Data, Guid) ->
    {ok, ObjectId} = file_id:guid_to_objectid(Guid),
    case maps:get(<<"fileId">>, Data) of
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

