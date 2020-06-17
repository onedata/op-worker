%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests concerning share basic API (REST + gs).
%%% @end
%%%-------------------------------------------------------------------
-module(share_api_test_SUITE).
-author("Bartosz Walkowicz").

-include("api_test_runner.hrl").
-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/graph_sync/gri.hrl").
-include_lib("ctool/include/http/codes.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    create_share_test/1
]).

all() ->
    ?ALL([
        create_share_test
    ]).


-define(ATTEMPTS, 30).


%%%===================================================================
%%% Get file distribution test functions
%%%===================================================================


create_share_test(Config) ->
    [P2, P1] = Providers = ?config(op_worker_nodes, Config),
    SessIdP1 = ?USER_IN_BOTH_SPACES_SESS_ID(P1, Config),
    SessIdP2 = ?USER_IN_BOTH_SPACES_SESS_ID(P2, Config),

    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    FilePath = filename:join(["/", ?SPACE_2, ?RANDOM_FILE_NAME()]),
    {ok, FileGuid} = api_test_utils:create_file(FileType, P1, SessIdP1, FilePath, 8#777),
    api_test_utils:wait_for_file_sync(P2, SessIdP2, FileGuid),

    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),

    EnvRef = api_test_utils:init_env(),
    api_test_utils:set_env_var(EnvRef, shares, []),

    VerifyEnvFun = fun(_, _) ->
        {ok, Shares} = api_test_utils:get_env_var(EnvRef, shares),
        lists:foreach(fun(Node) ->
            SessId = ?SESS_ID(?USER_IN_BOTH_SPACES, Node, Config),
            ?assertMatch(
                {ok, #file_attr{shares = Shares}},
                lfm_proxy:stat(Node, SessId, {guid, FileGuid}),
                ?ATTEMPTS
            )
        end, Providers),
        true
    end,

    ?assert(api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SPACE_2_SCENARIOS(Config),
            verify_fun = VerifyEnvFun,
            scenario_templates = [
                #scenario_template{
                    name = <<"Create share for ", FileType/binary, " using /shares rest endpoint">>,
                    type = rest,
                    prepare_args_fun = create_prepare_create_share_rest_args_fun(),
                    validate_result_fun = create_validate_create_share_rest_call_result_fun(
                        EnvRef, FileType, ?SPACE_2, Config
                    )
                },
                #scenario_template{
                    name = <<"Create share for ", FileType/binary, " using gs api">>,
                    type = gs,
                    prepare_args_fun = create_prepare_get_distribution_gs_args_fun(),
                    validate_result_fun = create_validate_create_share_gs_call_result_fun(EnvRef, FileType)
                }
            ],
            randomly_select_scenarios = true,
            data_spec = #data_spec{
                required = [<<"name">>, <<"fileId">>],
                correct_values = #{
                    <<"name">> => [<<"share1">>, <<"share2">>],
                    <<"fileId">> => [FileObjectId]
                }
            }
        }
    ])).


%% @private
create_prepare_create_share_rest_args_fun() ->
    fun(#api_test_ctx{data = Data}) ->
        #rest_args{
            method = post,
            path = <<"shares">>,
            headers = #{<<"content-type">> => <<"application/json">>},
            body = json_utils:encode(Data)
        }
    end.


%% @private
create_prepare_get_distribution_gs_args_fun() ->
    fun(#api_test_ctx{data = Data}) ->
        #gs_args{
            operation = create,
            gri = #gri{type = op_share, aspect = instance, scope = private},
            data = Data
        }
    end.


%% @private
create_validate_create_share_rest_call_result_fun(EnvRef, FileType, SpaceId, Config) ->
    fun(#api_test_ctx{
        node = Node,
        client = ?USER(UserId),
        data = #{<<"name">> := ShareName, <<"fileId">> := FileObjectId}
    }, Result) ->
        {ok, _, Headers, Body} = ?assertMatch(
            {ok, ?HTTP_201_CREATED, #{<<"Location">> := _}, #{<<"shareId">> := _}},
            Result
        ),
        ShareId = maps:get(<<"shareId">>, Body),

        {ok, Shares} = api_test_utils:get_env_var(EnvRef, shares),
        api_test_utils:set_env_var(EnvRef, shares, [ShareId | Shares]),

        ExpLocation = list_to_binary(rpc:call(Node, oneprovider, get_rest_endpoint, [
            string:trim(filename:join([<<"/">>, <<"shares">>, ShareId]), leading, [$/])
        ])),
        ?assertEqual(ExpLocation, maps:get(<<"Location">>, Headers)),

        {ok, FileGuid} = file_id:objectid_to_guid(FileObjectId),
        ShareFileGuid = file_id:guid_to_share_guid(FileGuid, ShareId),

        % Unfortunately due to mocks share record is stored only on provider
        % which received create request and it cannot be fetched from the other
        % one.
        ExpPublicUrl = <<ShareId/binary, "_public_url">>,
        ExpHandle = <<ShareId/binary, "_handle_id">>,
        ExpFileType = binary_to_atom(FileType, utf8),

        ?assertMatch(
            {ok, #document{key = ShareId, value = #od_share{
                name = ShareName,
                space = SpaceId,
                root_file = ShareFileGuid,
                public_url = ExpPublicUrl,
                file_type = ExpFileType,
                handle = ExpHandle
            }}},
            rpc:call(Node, share_logic, get, [?SESS_ID(UserId, Node, Config), ShareId])
        )
    end.


%% @private
create_validate_create_share_gs_call_result_fun(EnvRef, FileType) ->
    fun(#api_test_ctx{data = #{<<"name">> := ShareName, <<"fileId">> := FileObjectId}}, Result) ->
        {ok, ShareData} = ?assertMatch({ok, _}, Result),
        ShareGri = maps:get(<<"gri">>, ShareData),

        #gri{id = ShareId} = ?assertMatch(
            #gri{type = op_share, aspect = instance, scope = private},
            gri:deserialize(ShareGri)
        ),
        {ok, FileGuid} = file_id:objectid_to_guid(FileObjectId),
        ShareFileGuid = file_id:guid_to_share_guid(FileGuid, ShareId),

        {ok, Shares} = api_test_utils:get_env_var(EnvRef, shares),
        api_test_utils:set_env_var(EnvRef, shares, [ShareId | Shares]),

        % Unfortunately there is no oz in api tests and so all *_logic modules are mocked.
        % Because of share_logic mocks created shares already have handle and dummy public url.
        ExpShareData = #{
            <<"revision">> => 1,
            <<"gri">> => ShareGri,
            <<"name">> => ShareName,
            <<"fileType">> => FileType,
            <<"publicUrl">> => <<ShareId/binary, "_public_url">>,
            <<"handle">> => gri:serialize(#gri{
                type = op_handle,
                id = <<ShareId/binary, "_handle_id">>,
                aspect = instance,
                scope = private
            }),
            <<"privateRootFile">> => gri:serialize(#gri{
                type = op_file,
                id = FileGuid,
                aspect = instance,
                scope = private
            }),
            <<"rootFile">> => gri:serialize(#gri{
                type = op_file,
                id = ShareFileGuid,
                aspect = instance,
                scope = public
            })
        },
        ?assertEqual(ExpShareData, ShareData)
    end.


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        NewConfig1 = [{space_storage_mock, false} | NewConfig],
        NewConfig2 = initializer:setup_storage(NewConfig1),
        lists:foreach(fun(Worker) ->
            % TODO VFS-6251
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
    ct:timetrap({minutes, 10}),
    lfm_proxy:init(Config).


end_per_testcase(_Case, Config) ->
    initializer:unmock_share_logic(Config),
    lfm_proxy:teardown(Config).
