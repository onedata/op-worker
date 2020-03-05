%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% TODO WRITEME
%%% @end
%%%-------------------------------------------------------------------
-module(data_api_test_SUITE).
-author("Bartosz Walkowicz").

-include("global_definitions.hrl").
-include("api_test_utils.hrl").
-include("lfm_permissions_test.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/common/handshake_messages.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/graph_sync/gri.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/aai/caveats.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    list_dir_test/1
]).

all() ->
    ?ALL([
        list_dir_test
    ]).


-define(SCENARIO_NAME, atom_to_binary(?FUNCTION_NAME, utf8)).


%%%===================================================================
%%% Test functions
%%%===================================================================


list_dir_test(Config) ->
    [Provider2, Provider1] = ?config(op_worker_nodes, Config),

    SpaceId = <<"space2">>,
    UserId = <<"user3">>,
    UserSessId = ?config({session_id, {UserId, ?GET_DOMAIN(Provider2)}}, Config),

    RootDirName = ?SCENARIO_NAME,
    RootDirPath = filename:join(["/", SpaceId, RootDirName]),
    {ok, RootDirGuid} = lfm_proxy:mkdir(Provider2, UserSessId, RootDirPath, 8#777),
    {ok, RootDirObjectId} = file_id:guid_to_objectid(RootDirGuid),

    Files = lists:map(fun(Num) ->
        FileName = <<"file", Num>>,
        {ok, FileGuid} = lfm_proxy:create(Provider2, UserSessId, RootDirGuid, FileName, 8#777),
        {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),
        #{
            <<"id">> => FileObjectId,
            <<"path">> => filename:join([RootDirPath, FileName])
        }
    end, [$0, $1, $2, $3, $4]),

    ClientSpec = #client_spec{
        correct = [{user, UserId}],
        unauthorized = [nobody],
        forbidden = [{user, <<"user1">>}]
    },
    ParamsSpec = #data_spec{
        optional = [<<"limit">>, <<"offset">>],
        correct_values = #{
            <<"limit">> => [1, 100],
            <<"offset">> => [2, 10]
        },
        bad_values = [
            {<<"limit">>, true, ?ERROR_BAD_VALUE_INTEGER(<<"limit">>)},
            {<<"limit">>, -100, ?ERROR_BAD_VALUE_TOO_LOW(<<"limit">>, 1)},
            {<<"limit">>, 0, ?ERROR_BAD_VALUE_TOO_LOW(<<"limit">>, 1)},
            {<<"offset">>, <<"abc">>, ?ERROR_BAD_VALUE_INTEGER(<<"offset">>)}
        ]
    },
    ValidateResultFun = fun(_TargetNode, {ok, 200, Result}, _Env, Data) ->
        Limit = maps:get(<<"limit">>, Data, 100),
        Offset = maps:get(<<"offset">>, Data, 0) + 1,
        ExpFiles = case Offset >= length(Files) of
            true -> [];
            false -> lists:sublist(Files, Offset, Limit)
        end,
        ct:pal("~p", [{_TargetNode, Data}]),
        ?assertEqual(ExpFiles, Result)
    end,

    timer:sleep(timer:seconds(5)),

    ?assert(api_test_utils:run_scenarios(Config, [
        #scenario_spec{
            type = rest_with_file_path,
            target_nodes = [Provider2, Provider1],
            client_spec = ClientSpec,
            prepare_args_fun = fun(_Env, Data) ->
                Qs = prepare_list_dir_qs(Data),
                #rest_args{
                    method = get,
                    path = <<"files", RootDirPath/binary, Qs/binary>>
            }
            end,
            validate_result_fun = ValidateResultFun,
            data_spec = ParamsSpec
        },
        #scenario_spec{
            type = rest,
            target_nodes = [Provider2, Provider1],
            client_spec = ClientSpec,
            prepare_args_fun = fun(_Env, Data) ->
                Qs = prepare_list_dir_qs(Data),
                #rest_args{
                    method = get,
                    path = <<"files-id/", RootDirObjectId/binary, Qs/binary>>
                }
            end,
            validate_result_fun = ValidateResultFun,
            data_spec = ParamsSpec
        },
        #scenario_spec{
            type = gs,
            target_nodes = [Provider2, Provider1],
            client_spec = ClientSpec,
            prepare_args_fun = fun(_Env, Data) ->
                #gs_args{
                    operation = get,
                    gri = #gri{type = op_file, id = RootDirGuid, aspect = children, scope = private},
                    data = Data
                }
            end,
            validate_result_fun = fun(TargetNode, Result, _, _) ->
                ct:pal("QWE: ~p", [{TargetNode, Result}])
            end,
            data_spec = ParamsSpec
        }
    ])).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        NewConfig1 = [{space_storage_mock, false} | NewConfig],
        NewConfig2 = initializer:setup_storage(NewConfig1),
        lists:foreach(fun(Worker) ->
            test_utils:set_env(Worker, ?APP_NAME, dbsync_changes_broadcast_interval, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, couchbase_changes_update_interval, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, couchbase_changes_stream_update_interval, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms, timer:seconds(1)), % TODO - change to 2 seconds
            test_utils:set_env(Worker, ?APP_NAME, prefetching, off),
            test_utils:set_env(Worker, ?APP_NAME, public_block_size_treshold, 0),
            test_utils:set_env(Worker, ?APP_NAME, public_block_percent_treshold, 0)
        end, ?config(op_worker_nodes, NewConfig2)),
        application:start(ssl),
        hackney:start(),
        NewConfig3 = initializer:create_test_users_and_spaces(
            ?TEST_FILE(NewConfig2, "env_desc.json"),
            NewConfig2
        ),
        initializer:mock_auth_manager(NewConfig3),
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
    lfm_proxy:init(Config).


end_per_testcase(_Case, Config) ->
    initializer:unmock_share_logic(Config),
    lfm_proxy:teardown(Config).


%%%===================================================================
%%% Internal functions
%%%===================================================================


prepare_list_dir_qs(Data) ->
    QsParams = [QsParam || QsParam <- [
        prepare_qs_param(<<"limit">>, Data),
        prepare_qs_param(<<"offset">>, Data)
    ], QsParam /= <<>>],

    case str_utils:join_binary(QsParams, <<"&">>) of
        <<>> ->
            <<>>;
        Qs ->
            <<"?", Qs/binary>>
    end.


prepare_qs_param(ParamName, Data) ->
    case maps:get(ParamName, Data, undefined) of
        undefined ->
            <<>>;
        Value ->
            <<ParamName/binary, "=", (str_utils:to_binary(Value))/binary>>
    end.
