%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests concerning dataset crud API (REST + gs).
%%% @end
%%%-------------------------------------------------------------------
-module(api_dataset_crud_test_SUITE).
-author("Bartosz Walkowicz").

-include("api_file_test_utils.hrl").
-include("api_test_runner.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include("onenv_test_utils.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/graph_sync/gri.hrl").
-include_lib("ctool/include/http/codes.hrl").
-include_lib("ctool/include/privileges.hrl").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    establish_dataset_test/1,
    delete_dataset_test/1
]).

all() -> [
    establish_dataset_test,
    delete_dataset_test
].

-define(ATTEMPTS, 30).

-define(PROTECTION_FLAGS_COMBINATIONS, [
    [],
    [?DATA_PROTECTION_BIN],
    [?METADATA_PROTECTION_BIN],
    [?DATA_PROTECTION_BIN, ?METADATA_PROTECTION_BIN]
]).


%%%===================================================================
%%% Create dataset test functions
%%%===================================================================


establish_dataset_test(Config) ->
    Providers = [krakow, paris],
    SpaceId = oct_background:get_space_id(space_krk_par),

    #object{children = [#object{
        guid = FileGuid
    }]} = onenv_file_test_utils:create_and_sync_file_tree(
        user3, space_krk_par, build_test_file_tree_spec([#dataset_spec{}])
    ),
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),

    MemRef = api_test_memory:init(),

    ?assert(onenv_api_test_runner:run_tests([
        #suite_spec{
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SPACE_KRK_PAR(?EPERM),
            setup_fun = build_establish_dataset_setup_fun(MemRef, SpaceId),
            verify_fun = build_verify_establish_dataset_fun(MemRef, Providers, SpaceId, Config),
            scenario_templates = [
                #scenario_template{
                    name = <<"Establish dataset using REST API">>,
                    type = rest,
                    prepare_args_fun = build_establish_dataset_prepare_rest_args_fun(MemRef),
                    validate_result_fun = build_establish_dataset_validate_rest_call_result_fun(MemRef)
                },
                #scenario_template{
                    name = <<"Establish dataset using GS API">>,
                    type = gs,
                    prepare_args_fun = build_establish_datasets_prepare_gs_args_fun(MemRef),
                    validate_result_fun = build_establish_dataset_validate_gs_call_result_fun(MemRef, Config)
                }
            ],
            randomly_select_scenarios = true,
            data_spec = api_test_utils:add_cdmi_id_errors_for_operations_not_available_in_share_mode(
                % Operations should be rejected even before checking if share exists
                % (in case of using share file id) so it is not necessary to use
                % valid share id
                <<"rootFileId">>, FileGuid, SpaceId, <<"NonExistentDataset">>, #data_spec{
                    required = [<<"rootFileId">>],
                    optional = [<<"protectionFlags">>],
                    correct_values = #{
                        <<"rootFileId">> => [file_id],
                        <<"protectionFlags">> => ?PROTECTION_FLAGS_COMBINATIONS
                    },
                    bad_values = [
                        {<<"rootFileId">>, FileObjectId, ?ERROR_POSIX(?EEXIST)},
                        {<<"protectionFlags">>, 100, ?ERROR_BAD_VALUE_LIST_OF_BINARIES(<<"protectionFlags">>)},
                        {<<"protectionFlags">>, [<<"dummyFlag">>], ?ERROR_BAD_VALUE_LIST_NOT_ALLOWED(
                            <<"protectionFlags">>, [?DATA_PROTECTION_BIN, ?METADATA_PROTECTION_BIN]
                        )}
                    ]
                }
            )
        }
    ])).


%% @private
-spec build_establish_dataset_setup_fun(api_test_memory:mem_ref(), od_space:id()) ->
    onenv_api_test_runner:setup_fun().
build_establish_dataset_setup_fun(MemRef, SpaceId) ->
    fun() ->
        #object{name = DirName, children = [#object{
            guid = FileGuid,
            name = FileName,
            type = FileType
        }]} = onenv_file_test_utils:create_and_sync_file_tree(
            user3, SpaceId, build_test_file_tree_spec()
        ),
        {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),
        FilePath = filename:join(["/", ?SPACE_KRK_PAR, DirName, FileName]),

        api_test_memory:set(MemRef, file_guid, FileGuid),
        api_test_memory:set(MemRef, file_id, FileObjectId),
        api_test_memory:set(MemRef, file_type, FileType),
        api_test_memory:set(MemRef, file_path, FilePath)
    end.


%% @private
-spec build_establish_dataset_prepare_rest_args_fun(api_test_memory:mem_ref()) ->
    onenv_api_test_runner:prepare_args_fun().
build_establish_dataset_prepare_rest_args_fun(MemRef) ->
    fun(#api_test_ctx{data = Data}) ->
        #rest_args{
            method = post,
            path = <<"datasets">>,
            headers = #{<<"content-type">> => <<"application/json">>},
            body = json_utils:encode(substitute_root_file(MemRef, Data))
        }
    end.


%% @private
-spec build_establish_datasets_prepare_gs_args_fun(api_test_memory:mem_ref()) ->
    onenv_api_test_runner:prepare_args_fun().
build_establish_datasets_prepare_gs_args_fun(MemRef) ->
    fun(#api_test_ctx{data = Data}) ->
        #gs_args{
            operation = create,
            gri = #gri{type = op_dataset, aspect = instance, scope = private},
            data = substitute_root_file(MemRef, Data)
        }
    end.


%% @private
-spec substitute_root_file(api_test_memory:mem_ref(), map()) -> map().
substitute_root_file(MemRef, Data) ->
    case maps:get(<<"rootFileId">>, Data, undefined) of
        file_id ->
            Data#{<<"rootFileId">> => api_test_memory:get(MemRef, file_id)};
        _ ->
            Data
    end.


%% @private
-spec build_establish_dataset_validate_rest_call_result_fun(api_test_memory:mem_ref()) ->
    onenv_api_test_runner:validate_call_result_fun().
build_establish_dataset_validate_rest_call_result_fun(MemRef) ->
    fun(#api_test_ctx{node = TestNode}, Result) ->
        {ok, _, Headers, Body} = ?assertMatch(
            {ok, ?HTTP_201_CREATED, #{<<"Location">> := _}, #{<<"datasetId">> := _}},
            Result
        ),
        DatasetId = maps:get(<<"datasetId">>, Body),
        api_test_memory:set(MemRef, dataset_id, DatasetId),

        ExpLocation = api_test_utils:build_rest_url(TestNode, [<<"datasets">>, DatasetId]),
        ?assertEqual(ExpLocation, maps:get(<<"Location">>, Headers))
    end.


%% @private
-spec build_establish_dataset_validate_gs_call_result_fun(api_test_memory:mem_ref(), test_config:config()) ->
    onenv_api_test_runner:validate_call_result_fun().
build_establish_dataset_validate_gs_call_result_fun(MemRef, Config) ->
    SpaceDirDatasetId = ?config(space_dir_dataset, Config),

    fun(#api_test_ctx{node = TestNode, data = Data}, Result) ->
        RootFileGuid = api_test_memory:get(MemRef, file_guid),
        RootFileType = api_test_memory:get(MemRef, file_type),
        RootFilePath = api_test_memory:get(MemRef, file_path),
        CreationTime = get_global_time(TestNode),
        ProtectionFlags = maps:get(<<"protectionFlags">>, Data, []),

        {ok, #{<<"gri">> := DatasetGri} = DatasetData} = ?assertMatch({ok, _}, Result),

        #gri{id = DatasetId} = ?assertMatch(
            #gri{type = op_dataset, aspect = instance, scope = private},
            gri:deserialize(DatasetGri)
        ),
        api_test_memory:set(MemRef, dataset_id, DatasetId),

        ExpDatasetData = build_dataset_gs_instance(
            ?ATTACHED_DATASET, DatasetId, SpaceDirDatasetId, ProtectionFlags, CreationTime,
            RootFileGuid, RootFileType, RootFilePath
        ),
        ?assertEqual(ExpDatasetData, DatasetData)
    end.


%% @private
-spec build_verify_establish_dataset_fun(
    api_test_memory:mem_ref(),
    [oct_background:entity_selector()],
    od_space:id(),
    test_config:config()
) ->
    onenv_api_test_runner:verify_fun().
build_verify_establish_dataset_fun(MemRef, Providers, SpaceId, Config) ->
    SpaceDirDatasetId = ?config(space_dir_dataset, Config),

    fun
        (expected_success, #api_test_ctx{
            node = TestNode,
            client = ?USER(UserId),
            data = Data
        }) ->
            DatasetId = api_test_memory:get(MemRef, dataset_id),
            RootFileGuid = api_test_memory:get(MemRef, file_guid),
            RootFileType = api_test_memory:get(MemRef, file_type),
            RootFilePath = api_test_memory:get(MemRef, file_path),
            CreationTime = get_global_time(TestNode),
            ProtectionFlags = maps:get(<<"protectionFlags">>, Data, []),

            lists:foreach(fun(Provider) ->
                verify_dataset(
                    UserId, Provider, SpaceId, DatasetId, ?ATTACHED_DATASET, SpaceDirDatasetId,
                    ProtectionFlags, CreationTime, RootFileGuid, RootFileType, RootFilePath
                )
            end, Providers);
        (expected_failure, _) ->
            ok
    end.


%%%===================================================================
%%% Delete dataset test functions
%%%===================================================================


delete_dataset_test(Config) ->
    Providers = [krakow, paris],
    SpaceId = oct_background:get_space_id(space_krk_par),

    #object{children = Children} = onenv_file_test_utils:create_and_sync_file_tree(
        user3, space_krk_par, build_test_file_tree_spec(lists:map(fun(_) ->
            #dataset_spec{
                state = random_dataset_state(),
                protection_flags = lists_utils:random_element(?PROTECTION_FLAGS_COMBINATIONS)
            }
        end, lists:seq(1, 10)))
    ),
    MemRef = api_test_memory:init(),

    DatasetIds = lists:map(fun(#object{dataset = #dataset_obj{id = DatasetId, state = State}}) ->
        api_test_memory:set(MemRef, {dataset_state, DatasetId}, State),
        DatasetId
    end, Children),
    api_test_memory:set(MemRef, datasets, DatasetIds),

    ?assert(onenv_api_test_runner:run_tests([
        #suite_spec{
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SPACE_KRK_PAR(?EPERM),
            verify_fun = build_verify_delete_dataset_fun(MemRef, Providers, SpaceId, Config),
            scenario_templates = [
                #scenario_template{
                    name = <<"Delete dataset using REST API">>,
                    type = rest,
                    prepare_args_fun = build_delete_dataset_prepare_rest_args_fun(MemRef),
                    validate_result_fun = fun(_, {ok, RespCode, _, RespBody}) ->
                        ?assertEqual({?HTTP_204_NO_CONTENT, #{}}, {RespCode, RespBody})
                    end
                },
                #scenario_template{
                    name = <<"Delete dataset using GS API">>,
                    type = gs,
                    prepare_args_fun = build_delete_dataset_prepare_gs_args_fun(MemRef),
                    validate_result_fun = fun(_, Result) -> ?assertEqual(ok, Result) end
                }
            ],
            data_spec = #data_spec{
                bad_values = [
                    {bad_id, <<"NonExistentDataset">>, ?ERROR_NOT_FOUND}
                ]
            }
        }
    ])).


%% @private
-spec build_delete_dataset_prepare_rest_args_fun(api_test_memory:mem_ref()) ->
    onenv_api_test_runner:prepare_args_fun().
build_delete_dataset_prepare_rest_args_fun(MemRef) ->
    fun(#api_test_ctx{data = Data}) ->
        DatasetId = choose_dataset_to_remove(MemRef),
        {Id, _} = api_test_utils:maybe_substitute_bad_id(DatasetId, Data),

        #rest_args{
            method = delete,
            path = <<"datasets/", Id/binary>>
        }
    end.


%% @private
-spec build_delete_dataset_prepare_gs_args_fun(api_test_memory:mem_ref()) ->
    onenv_api_test_runner:prepare_args_fun().
build_delete_dataset_prepare_gs_args_fun(MemRef) ->
    fun(#api_test_ctx{data = Data0}) ->
        DatasetId = choose_dataset_to_remove(MemRef),
        {Id, Data1} = api_test_utils:maybe_substitute_bad_id(DatasetId, Data0),

        #gs_args{
            operation = delete,
            gri = #gri{type = op_dataset, id = Id, aspect = instance, scope = private},
            data = Data1
        }
    end.


%% @private
-spec choose_dataset_to_remove(api_test_memory:mem_ref()) -> dataset:id().
choose_dataset_to_remove(MemRef) ->
    Datasets = api_test_memory:get(MemRef, datasets),
    DatasetId = lists_utils:random_element(Datasets),
    api_test_memory:set(MemRef, dataset_to_remove, DatasetId),

    DatasetId.


%% @private
-spec build_verify_delete_dataset_fun(
    api_test_memory:mem_ref(),
    [oct_background:entity_selector()],
    od_space:id(),
    test_config:config()
) ->
    onenv_api_test_runner:verify_fun().
build_verify_delete_dataset_fun(MemRef, Providers, SpaceId, Config) ->
    SpaceDirDatasetId = ?config(space_dir_dataset, Config),

    fun(ExpResult, _) ->
        DatasetId = api_test_memory:get(MemRef, dataset_to_remove),

        lists:foreach(fun(Provider) ->
            Node = ?RAND_OP_NODE(Provider),
            UserSessId = oct_background:get_user_session_id(user2, Provider),
            ListOpts = #{offset => 0, limit => 1000},

            GetDatasetsFun = case SpaceDirDatasetId of
                undefined ->
                    State = api_test_memory:get(MemRef, {dataset_state, DatasetId}),
                    fun() -> list_top_dataset_ids(Node, UserSessId, SpaceId, State, ListOpts) end;
                _ ->
                    fun() -> list_child_dataset_ids(Node, UserSessId, SpaceDirDatasetId, ListOpts) end
            end,

            case ExpResult of
                expected_success ->
                    ?assertEqual({error, ?ENOENT}, lfm_proxy:get_dataset_info(Node, UserSessId, DatasetId), ?ATTEMPTS),
                    ?assertEqual(false, lists:member(DatasetId, GetDatasetsFun())),
                    api_test_memory:set(MemRef, datasets, lists:delete(
                        DatasetId, api_test_memory:get(MemRef, datasets)
                    ));
                expected_failure ->
                    ?assertMatch({ok, _}, lfm_proxy:get_dataset_info(Node, UserSessId, DatasetId), ?ATTEMPTS),
                    ?assertEqual(true, lists:member(DatasetId, GetDatasetsFun()))
            end
        end, Providers)
    end.


%%%===================================================================
%%% Common dataset test utils
%%%===================================================================


%% @private
-spec random_dataset_state() -> dataset:state().
random_dataset_state() ->
    case rand:uniform(2) of
        1 -> attached;
        2 -> detached
    end.


%% @private
-spec build_test_file_tree_spec() -> onenv_file_test_utils:file_spec().
build_test_file_tree_spec() ->
    build_test_file_tree_spec([undefined]).


%% @private
-spec build_test_file_tree_spec([onenv_dataset_test_utils:dataset_spec()]) ->
    onenv_file_test_utils:file_spec().
build_test_file_tree_spec(DatasetSpecs) ->
    ChildrenSpec = lists:map(fun(DatasetSpec) ->
        case api_test_utils:randomly_choose_file_type_for_test() of
            <<"file">> -> #file_spec{dataset = DatasetSpec};
            <<"dir">> -> #dir_spec{dataset = DatasetSpec}
        end
    end, DatasetSpecs),
    #dir_spec{children = ChildrenSpec}.


%% @private
-spec build_dataset_gs_instance(
    dataset:state(), dataset:id(), dataset:id(), [binary()], time:seconds(),
    file_id:file_guid(), file_meta:type(), file_meta:path()
) ->
    map().
build_dataset_gs_instance(
    State, DatasetId, ParentId, ProtectionFlagsJson, CreationTime,
    RootFileGuid, RootFileType, RootFilePath
) ->
    BasicInfo = dataset_gui_gs_translator:translate_dataset_info(#dataset_info{
        id = DatasetId,
        state = State,
        guid = RootFileGuid,
        path = RootFilePath,
        type = RootFileType,
        creation_time = CreationTime,
        protection_flags = file_meta:protection_flags_from_json(ProtectionFlagsJson),
        parent = ParentId
    }),
    BasicInfo#{<<"revision">> => 1}.


%% @private
-spec verify_dataset(
    od_user:id(), od_provider:id(), od_space:id(), dataset:id(), dataset:state(), dataset:id(),
    [binary()], time:seconds(), file_id:file_guid(), file_meta:type(), file_meta:path()
) ->
    ok.
verify_dataset(
    UserId, Provider, SpaceId, DatasetId, State, ParentId, ProtectionFlagsJson,
    CreationTime, RootFileGuid, RootFileType, RootFilePath
) ->
    Node = ?RAND_OP_NODE(Provider),
    UserSessId = oct_background:get_user_session_id(UserId, Provider),
    ListOpts = #{offset => 0, limit => 1000},

    GetDatasetsFun = case ParentId of
        undefined -> fun() -> list_top_dataset_ids(Node, UserSessId, SpaceId, State, ListOpts) end;
        _ -> fun() -> list_child_dataset_ids(Node, UserSessId, ParentId, ListOpts) end
    end,

    ?assertEqual(true, lists:member(DatasetId, GetDatasetsFun()), ?ATTEMPTS),

    ExpDatasetInfo = #dataset_info{
        id = DatasetId,
        state = State,
        guid = RootFileGuid,
        path = RootFilePath,
        type = RootFileType,
        creation_time = CreationTime,
        protection_flags = file_meta:protection_flags_from_json(ProtectionFlagsJson),
        parent = ParentId
    },
    ?assertEqual({ok, ExpDatasetInfo}, lfm_proxy:get_dataset_info(Node, UserSessId, DatasetId)),
    ok.


%% @private
-spec list_top_dataset_ids(node(), session:id(), od_space:id(), dataset:state(), datasets_structure:opts()) ->
    [dataset:id()].
list_top_dataset_ids(Node, UserSessId, SpaceId, State, ListOpts) ->
    {ok, Datasets, _} = lfm_proxy:list_top_datasets(
        Node, UserSessId, SpaceId, State, ListOpts
    ),
    lists:map(fun({DatasetId, _}) -> DatasetId end, Datasets).


%% @private
-spec list_child_dataset_ids(node(), session:id(), dataset:id(), datasets_structure:opts()) ->
    [dataset:id()].
list_child_dataset_ids(Node, UserSessId, ParentId, ListOpts) ->
    {ok, Datasets, _} = lfm_proxy:list_nested_datasets(Node, UserSessId, ParentId, ListOpts),
    lists:map(fun({DatasetId, _}) -> DatasetId end, Datasets).


%% @private
-spec get_global_time(node()) -> time:seconds().
get_global_time(Node) ->
    rpc:call(Node, global_clock, timestamp_seconds, []).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    oct_background:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "api_tests",
        envs = [{op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}],
        posthook = fun(NewConfig) ->
            onenv_test_utils:set_user_privileges(user3, space_krk_par, [
                ?SPACE_MANAGE_DATASETS | privileges:space_member()
            ]),
            onenv_test_utils:set_user_privileges(
                user4, space_krk_par, privileges:space_member() -- [?SPACE_VIEW]
            ),

            % Randomly establish for space root dir
            SpaceDirDatasetId = undefined,
%%            SpaceDirDatasetId = case rand:uniform(2) of
%%                1 ->
%%                    undefined;
%%                2 ->
%%                    %% TODO change to cthr:pal to print only on test failure
%%                    ct:pal("Establishing dataset for space root dir"),
%%                    onenv_dataset_test_utils:establish_and_sync_dataset(user3, space_krk_par)
%%            end,

            [{space_dir_dataset, SpaceDirDatasetId} | NewConfig]
        end
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 10}),
    time_test_utils:freeze_time(Config),
    lfm_proxy:init(Config).


end_per_testcase(_Case, Config) ->
    time_test_utils:unfreeze_time(Config),
    lfm_proxy:teardown(Config).
