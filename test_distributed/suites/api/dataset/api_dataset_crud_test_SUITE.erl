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
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/privileges.hrl").

-export([
    groups/0, all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_group/2, end_per_group/2,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    establish_dataset_test/1,
    get_dataset_test/1,
    update_dataset_test/1,
    delete_dataset_test/1
]).

groups() -> [
    {all_tests, [parallel], [
        establish_dataset_test,
        get_dataset_test,
        update_dataset_test,
        delete_dataset_test
    ]}
].

all() -> [
    {group, all_tests}
].

-define(ATTEMPTS, 30).

-define(FMT(__FMT, __ARGS), str_utils:format(__FMT, __ARGS)).

-define(PROTECTION_FLAGS_COMBINATIONS, [
    [],
    [?DATA_PROTECTION_BIN],
    [?METADATA_PROTECTION_BIN],
    [?DATA_PROTECTION_BIN, ?METADATA_PROTECTION_BIN]
]).

-define(NON_EXISTENT_DATASET_ID, <<"NonExistentDataset">>).


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
            data_spec = api_test_utils:add_cdmi_id_errors_for_operations_not_available_in_share_mode(
                % Operations should be rejected even before checking if share exists
                % (in case of using share file id) so it is not necessary to use
                % valid share id
                <<"rootFileId">>, FileGuid, SpaceId, <<"NonExistentShareId">>, #data_spec{
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
            headers = #{?HDR_CONTENT_TYPE => <<"application/json">>},
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
            {ok, ?HTTP_201_CREATED, #{?HDR_LOCATION := _}, #{<<"datasetId">> := _}},
            Result
        ),
        DatasetId = maps:get(<<"datasetId">>, Body),
        api_test_memory:set(MemRef, dataset_id, DatasetId),

        ExpLocation = api_test_utils:build_rest_url(TestNode, [<<"datasets">>, DatasetId]),
        ?assertEqual(ExpLocation, maps:get(?HDR_LOCATION, Headers))
    end.


%% @private
-spec build_establish_dataset_validate_gs_call_result_fun(api_test_memory:mem_ref(), test_config:config()) ->
    onenv_api_test_runner:validate_call_result_fun().
build_establish_dataset_validate_gs_call_result_fun(MemRef, Config) ->
    SpaceDirDatasetId = ?config(space_dir_dataset, Config),

    fun(#api_test_ctx{data = Data}, Result) ->
        RootFileGuid = api_test_memory:get(MemRef, file_guid),
        RootFileType = api_test_memory:get(MemRef, file_type),
        RootFilePath = api_test_memory:get(MemRef, file_path),
        CreationTime = time_test_utils:get_frozen_time_seconds(),
        ProtectionFlags = maps:get(<<"protectionFlags">>, Data, []),

        {ok, #{<<"gri">> := DatasetGri} = DatasetData} = ?assertMatch({ok, _}, Result),

        #gri{id = DatasetId} = ?assertMatch(
            #gri{type = op_dataset, aspect = instance, scope = private},
            gri:deserialize(DatasetGri)
        ),
        api_test_memory:set(MemRef, dataset_id, DatasetId),

        ExpDatasetData = build_dataset_gs_instance(
            ?ATTACHED_DATASET, DatasetId, SpaceDirDatasetId, ProtectionFlags, CreationTime,
            RootFileGuid, RootFileType, RootFilePath, false
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
            client = ?USER(UserId),
            data = Data
        }) ->
            DatasetId = api_test_memory:get(MemRef, dataset_id),
            RootFileGuid = api_test_memory:get(MemRef, file_guid),
            RootFileType = api_test_memory:get(MemRef, file_type),
            RootFilePath = api_test_memory:get(MemRef, file_path),
            CreationTime = time_test_utils:get_frozen_time_seconds(),
            ProtectionFlags = maps:get(<<"protectionFlags">>, Data, []),

            verify_dataset(
                UserId, Providers, SpaceId, DatasetId, ?ATTACHED_DATASET, SpaceDirDatasetId,
                ProtectionFlags, CreationTime, RootFileGuid, RootFileType, RootFilePath
            );
        (expected_failure, _) ->
            ok
    end.


%%%===================================================================
%%% Get dataset test functions
%%%===================================================================


get_dataset_test(Config) ->
    State = random_dataset_state(),
    ProtectionFlags = lists_utils:random_element(?PROTECTION_FLAGS_COMBINATIONS),

    #object{name = DirName, children = [#object{
        guid = FileGuid,
        name = FileName,
        type = FileType,
        dataset = #dataset_object{id = DatasetId}
    }]} = onenv_file_test_utils:create_and_sync_file_tree(
        user3, space_krk_par, build_test_file_tree_spec([
            #dataset_spec{state = State, protection_flags = ProtectionFlags}
        ])
    ),
    OriginalParentId = case State of
        ?ATTACHED_DATASET -> ?config(space_dir_dataset, Config);
        ?DETACHED_DATASET -> undefined
    end,
    OriginalFilePath = filename:join(["/", ?SPACE_KRK_PAR, DirName, FileName]),

    case State == ?ATTACHED_DATASET andalso lists:member(?DATA_PROTECTION_BIN, ProtectionFlags) of
        true ->
            ct:pal(?FMT("Test get ~p dataset", [State])),

            get_dataset_test_base(
                DatasetId, OriginalParentId, State, ProtectionFlags,
                FileGuid, FileType, OriginalFilePath, false
            );
        false ->
            ct:pal(?FMT("Test get ~p dataset after moving root file", [State])),

            NewFilePath = filename:join(["/", ?SPACE_KRK_PAR, FileName]),
            onenv_file_test_utils:mv_and_sync_file(user3, FileGuid, NewFilePath),

            DatasetRecordedFilePath = case State of
                ?ATTACHED_DATASET -> NewFilePath;
                ?DETACHED_DATASET -> OriginalFilePath
            end,

            get_dataset_test_base(
                DatasetId, OriginalParentId, State, ProtectionFlags,
                FileGuid, FileType, DatasetRecordedFilePath, false
            ),

            ct:pal(?FMT("Test get ~p dataset after removing root file", [State])),

            onenv_file_test_utils:rm_and_sync_file(user3, FileGuid),

            get_dataset_test_base(
                DatasetId, undefined, detached, ProtectionFlags,
                FileGuid, FileType, DatasetRecordedFilePath, true
            )
    end.


%% @private
-spec get_dataset_test_base(
    dataset:id(), dataset:id(), dataset:state(), [binary()],
    file_id:file_guid(), file_meta:type(), file_meta:path(), boolean()
) ->
    map().
get_dataset_test_base(
    DatasetId, ParentId, State, ProtectionFlags,
    RootFileGuid, RootFileType, RootFilePath, RootFileDeleted
) ->
    StateBin = atom_to_binary(State, utf8),
    {ok, RootFileObjectId} = file_id:guid_to_objectid(RootFileGuid),
    RootFileTypeBin = str_utils:to_binary(RootFileType),

    ?assert(onenv_api_test_runner:run_tests([
        #suite_spec{
            target_nodes = [krakow, paris],
            client_spec = ?CLIENT_SPEC_FOR_SPACE_KRK_PAR(?EPERM),
            scenario_templates = [
                #scenario_template{
                    name = <<"Get dataset using REST API">>,
                    type = rest,
                    prepare_args_fun = build_get_dataset_prepare_rest_args_fun(DatasetId),
                    validate_result_fun = fun(_, {ok, RespCode, _, RespBody}) ->
                        CreationTime = time_test_utils:get_frozen_time_seconds(),
                        EffProtectionFlags = case State of
                            ?ATTACHED_DATASET -> ProtectionFlags;
                            ?DETACHED_DATASET -> []
                        end,
                        ExpDatasetData = #{
                            <<"datasetId">> => DatasetId,
                            <<"parentId">> => utils:undefined_to_null(ParentId),
                            <<"rootFileId">> => RootFileObjectId,
                            <<"rootFileType">> => RootFileTypeBin,
                            <<"rootFilePath">> => RootFilePath,
                            <<"rootFileDeleted">> => RootFileDeleted,
                            <<"state">> => StateBin,
                            <<"protectionFlags">> => ProtectionFlags,
                            <<"effectiveProtectionFlags">> => EffProtectionFlags,
                            <<"creationTime">> => CreationTime,
                            <<"archiveCount">> => 0
                        },
                        ?assertEqual({?HTTP_200_OK, ExpDatasetData}, {RespCode, RespBody})
                    end
                },
                #scenario_template{
                    name = <<"Get dataset using GS API">>,
                    type = gs,
                    prepare_args_fun = build_get_dataset_prepare_gs_args_fun(DatasetId),
                    validate_result_fun = fun(_, {ok, Result}) ->
                        CreationTime = time_test_utils:get_frozen_time_seconds(),

                        ExpDatasetData = build_dataset_gs_instance(
                            State, DatasetId, ParentId, ProtectionFlags, CreationTime,
                            RootFileGuid, RootFileType, RootFilePath, RootFileDeleted
                        ),
                        ?assertEqual(ExpDatasetData, Result)
                    end
                }
            ],
            data_spec = #data_spec{
                bad_values = [{bad_id, ?NON_EXISTENT_DATASET_ID, ?ERROR_NOT_FOUND}]
            }
        }
    ])).


%% @private
-spec build_get_dataset_prepare_rest_args_fun(dataset:id()) ->
    onenv_api_test_runner:prepare_args_fun().
build_get_dataset_prepare_rest_args_fun(DatasetId) ->
    fun(#api_test_ctx{data = Data}) ->
        {Id, _} = api_test_utils:maybe_substitute_bad_id(DatasetId, Data),

        #rest_args{
            method = get,
            path = <<"datasets/", Id/binary>>
        }
    end.


%% @private
-spec build_get_dataset_prepare_gs_args_fun(dataset:id()) ->
    onenv_api_test_runner:prepare_args_fun().
build_get_dataset_prepare_gs_args_fun(DatasetId) ->
    fun(#api_test_ctx{data = Data0}) ->
        {Id, Data1} = api_test_utils:maybe_substitute_bad_id(DatasetId, Data0),

        #gs_args{
            operation = get,
            gri = #gri{type = op_dataset, id = Id, aspect = instance, scope = private},
            data = Data1
        }
    end.


%%%===================================================================
%%% Update dataset test functions
%%%===================================================================


update_dataset_test(Config) ->
    Providers = [krakow, paris],
    SpaceKrkParId = oct_background:get_space_id(space_krk_par),

    OriginalState = ?ATTACHED_DATASET,
    OriginalProtectionFlags = lists_utils:random_element(?PROTECTION_FLAGS_COMBINATIONS),

    #object{name = DirName, children = [#object{
        guid = FileGuid,
        name = FileName,
        type = FileType,
        dataset = #dataset_object{id = DatasetId}
    }]} = onenv_file_test_utils:create_and_sync_file_tree(
        user3, space_krk_par, build_test_file_tree_spec([
            #dataset_spec{state = OriginalState, protection_flags = OriginalProtectionFlags}
        ])
    ),
    FilePath = filename:join(["/", ?SPACE_KRK_PAR, DirName, FileName]),

    MemRef = api_test_memory:init(),
    api_test_memory:set(MemRef, previous_state, OriginalState),
    api_test_memory:set(MemRef, previous_protection_flags, OriginalProtectionFlags),

    ?assert(onenv_api_test_runner:run_tests([
        #suite_spec{
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SPACE_KRK_PAR(?EPERM),
            verify_fun = build_verify_update_dataset_fun(
                MemRef, Providers, SpaceKrkParId,
                DatasetId, FileGuid, FileType, FilePath, Config
            ),
            scenario_templates = [
                #scenario_template{
                    name = <<"Update dataset using REST API">>,
                    type = rest,
                    prepare_args_fun = build_update_dataset_prepare_rest_args_fun(DatasetId),
                    validate_result_fun = fun(#api_test_ctx{data = Data}, {ok, RespCode, _, RespBody}) ->
                        ExpResult = case get_exp_update_result(MemRef, Data) of
                            ok ->
                                {?HTTP_204_NO_CONTENT, #{}};
                            {error, _} = Error ->
                                {errors:to_http_code(Error), ?REST_ERROR(Error)}
                        end,
                        ?assertEqual(ExpResult, {RespCode, RespBody})
                    end
                },
                #scenario_template{
                    name = <<"Update dataset using GS API">>,
                    type = gs,
                    prepare_args_fun = build_update_dataset_prepare_gs_args_fun(DatasetId),
                    validate_result_fun = fun(#api_test_ctx{data = Data}, Result) ->
                        ExpResult = get_exp_update_result(MemRef, Data),
                        ?assertEqual(ExpResult, Result)
                    end
                }
            ],
            data_spec = #data_spec{
                at_least_one = [
                    <<"state">>, <<"setProtectionFlags">>, <<"unsetProtectionFlags">>
                ],
                correct_values = #{
                    <<"state">> => [<<"attached">>, <<"detached">>],
                    <<"setProtectionFlags">> => ?PROTECTION_FLAGS_COMBINATIONS,
                    <<"unsetProtectionFlags">> => ?PROTECTION_FLAGS_COMBINATIONS
                },
                bad_values = [
                    {<<"state">>, 100, ?ERROR_BAD_VALUE_BINARY(<<"state">>)},
                    {<<"state">>, <<"dummy">>, ?ERROR_BAD_VALUE_NOT_ALLOWED(
                        <<"state">>, [<<"attached">>, <<"detached">>]
                    )},
                    {<<"setProtectionFlags">>, 100, ?ERROR_BAD_VALUE_LIST_OF_BINARIES(<<"setProtectionFlags">>)},
                    {<<"setProtectionFlags">>, [<<"dummyFlag">>], ?ERROR_BAD_VALUE_LIST_NOT_ALLOWED(
                        <<"setProtectionFlags">>, [?DATA_PROTECTION_BIN, ?METADATA_PROTECTION_BIN]
                    )},
                    {<<"unsetProtectionFlags">>, 100, ?ERROR_BAD_VALUE_LIST_OF_BINARIES(<<"unsetProtectionFlags">>)},
                    {<<"unsetProtectionFlags">>, [<<"dummyFlag">>], ?ERROR_BAD_VALUE_LIST_NOT_ALLOWED(
                        <<"unsetProtectionFlags">>, [?DATA_PROTECTION_BIN, ?METADATA_PROTECTION_BIN]
                    )},
                    {bad_id, ?NON_EXISTENT_DATASET_ID, ?ERROR_NOT_FOUND}
                ]
            }
        }
    ])).


%% @private
-spec build_update_dataset_prepare_rest_args_fun(dataset:id()) ->
    onenv_api_test_runner:prepare_args_fun().
build_update_dataset_prepare_rest_args_fun(DatasetId) ->
    fun(#api_test_ctx{data = Data0}) ->
        {Id, Data1} = api_test_utils:maybe_substitute_bad_id(DatasetId, Data0),

        #rest_args{
            method = patch,
            path = <<"datasets/", Id/binary>>,
            headers = #{?HDR_CONTENT_TYPE => <<"application/json">>},
            body = json_utils:encode(Data1)
        }
    end.


%% @private
-spec build_update_dataset_prepare_gs_args_fun(dataset:id()) ->
    onenv_api_test_runner:prepare_args_fun().
build_update_dataset_prepare_gs_args_fun(DatasetId) ->
    fun(#api_test_ctx{data = Data0}) ->
        {Id, Data1} = api_test_utils:maybe_substitute_bad_id(DatasetId, Data0),

        #gs_args{
            operation = update,
            gri = #gri{type = op_dataset, id = Id, aspect = instance, scope = private},
            data = Data1
        }
    end.


%% @private
-spec build_verify_update_dataset_fun(
    api_test_memory:mem_ref(), [oct_background:entity_selector()], od_space:id(),
    dataset:state(), file_id:file_guid(), file_meta:type(), file_meta:path(),
    test_config:config()
) ->
    onenv_api_test_runner:verify_fun().
build_verify_update_dataset_fun(MemRef, Providers, SpaceId, DatasetId, FileGuid, FileType, FilePath, Config) ->
    SpaceDirDatasetId = ?config(space_dir_dataset, Config),

    fun(ExpTestResult, #api_test_ctx{data = Data}) ->
        CreationTime = time_test_utils:get_frozen_time_seconds(),

        PrevState = api_test_memory:get(MemRef, previous_state),
        PrevProtectionFlags = api_test_memory:get(MemRef, previous_protection_flags),

        {ExpState, ExpFlags} = case ExpTestResult == expected_success andalso ok == get_exp_update_result(MemRef, Data) of
            true ->
                NewState = maps:get(<<"state">>, Data, atom_to_binary(PrevState, utf8)),
                NewProtectionFlags = maps:get(<<"setProtectionFlags">>, Data, []) ++ (
                        PrevProtectionFlags -- maps:get(<<"unSetProtectionFlags">>, Data, [])
                ),
                {binary_to_atom(NewState, utf8), NewProtectionFlags};
            false ->
                {PrevState, PrevProtectionFlags}
        end,
        ExpParentId = case ExpState of
            ?ATTACHED_DATASET -> SpaceDirDatasetId;
            ?DETACHED_DATASET -> undefined
        end,
        verify_dataset(
            user2, Providers, SpaceId, DatasetId, ExpState, ExpParentId,
            ExpFlags, CreationTime, FileGuid, FileType, FilePath
        ),
        api_test_memory:set(MemRef, previous_state, ExpState),
        api_test_memory:set(MemRef, previous_protection_flags, ExpFlags)
    end.


%% @private
-spec get_exp_update_result(api_test_memory:mem_ref(), middleware:data()) ->
    ok | errors:error().
get_exp_update_result(MemRef, Data) ->
    PrevState = str_utils:to_binary(api_test_memory:get(MemRef, previous_state)),
    NewState = maps:get(<<"state">>, Data, undefined),
    ProtectionFlagsToSet = maps:get(<<"setProtectionFlags">>, Data, []),
    ProtectionFlagsToUnset = maps:get(<<"unsetProtectionFlags">>, Data, []),

    case {PrevState, NewState, ProtectionFlagsToSet, ProtectionFlagsToUnset} of
        {_, undefined, [], []} ->
            ok;
        {SameState, SameState, _, _} ->
            ?ERROR_ALREADY_EXISTS;
        {<<"attached">>, <<"detach">>, [], []} ->
            ok;
        {<<"attached">>, <<"detach">>, _, _} ->
            ?ERROR_POSIX(?EINVAL);
        {<<"detached">>, undefined, _, _} ->
            ?ERROR_POSIX(?EINVAL);
        _ ->
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

    DatasetIds = lists:map(fun(#object{dataset = #dataset_object{id = DatasetId, state = State}}) ->
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
        api_test_memory:set(MemRef, dataset_to_remove, Id),

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
        api_test_memory:set(MemRef, dataset_to_remove, Id),

        #gs_args{
            operation = delete,
            gri = #gri{type = op_dataset, id = Id, aspect = instance, scope = private},
            data = Data1
        }
    end.


%% @private
-spec choose_dataset_to_remove(api_test_memory:mem_ref()) -> dataset:id().
choose_dataset_to_remove(MemRef) ->
    lists_utils:random_element(api_test_memory:get(MemRef, datasets)).


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
        case api_test_memory:get(MemRef, dataset_to_remove) of
            ?NON_EXISTENT_DATASET_ID ->
                ok;
            DatasetId ->
                State = api_test_memory:get(MemRef, {dataset_state, DatasetId}),

                lists:foreach(fun(Provider) ->
                    Node = ?OCT_RAND_OP_NODE(Provider),
                    UserSessId = oct_background:get_user_session_id(user2, Provider),
                    ListOpts = #{offset => 0, limit => 1000},

                    ListDatasetsFun = case SpaceDirDatasetId == undefined orelse State == ?DETACHED_DATASET of
                        true ->
                            fun() ->
                                list_top_dataset_ids(Node, UserSessId, SpaceId, State, ListOpts)
                            end;
                        false ->
                            fun() ->
                                list_child_dataset_ids(Node, UserSessId, SpaceDirDatasetId, ListOpts)
                            end
                    end,
                    GetDatasetInfo = fun() -> lfm_proxy:get_dataset_info(Node, UserSessId, DatasetId) end,

                    case ExpResult of
                        expected_success ->
                            ?assertEqual({error, ?ENOENT}, GetDatasetInfo(), ?ATTEMPTS),
                            ?assertEqual(false, lists:member(DatasetId, ListDatasetsFun())),
                            api_test_memory:set(MemRef, datasets, lists:delete(
                                DatasetId, api_test_memory:get(MemRef, datasets)
                            ));
                        expected_failure ->
                            ?assertMatch({ok, _}, GetDatasetInfo(), ?ATTEMPTS),
                            ?assertEqual(true, lists:member(DatasetId, ListDatasetsFun()), ?ATTEMPTS)
                    end
                end, Providers)
        end
    end.


%%%===================================================================
%%% Common dataset test utils
%%%===================================================================


%% @private
-spec random_dataset_state() -> dataset:state().
random_dataset_state() ->
    lists_utils:random_element([?ATTACHED_DATASET, ?DETACHED_DATASET]).


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
    file_id:file_guid(), file_meta:type(), file_meta:path(), boolean()
) ->
    map().
build_dataset_gs_instance(
    State, DatasetId, ParentId, ProtectionFlagsJson, CreationTime,
    RootFileGuid, RootFileType, RootFilePath, RootFileDeleted
) ->
    EffProtectionFlagsJson = case State of
        ?ATTACHED_DATASET -> ProtectionFlagsJson;
        ?DETACHED_DATASET -> []
    end,
    BasicInfo = dataset_gui_gs_translator:translate_dataset_info(#dataset_info{
        id = DatasetId,
        state = State,
        root_file_guid = RootFileGuid,
        root_file_path = RootFilePath,
        root_file_type = RootFileType,
        root_file_deleted = RootFileDeleted,
        creation_time = CreationTime,
        protection_flags = file_meta:protection_flags_from_json(ProtectionFlagsJson),
        eff_protection_flags = file_meta:protection_flags_from_json(EffProtectionFlagsJson),
        parent = ParentId,
        index = datasets_structure:pack_entry_index(filename:basename(RootFilePath), DatasetId),
        archive_count = 0
    }),
    BasicInfo#{<<"revision">> => 1}.


%% @private
-spec verify_dataset(
    od_user:id(), [oct_background:entity_selector()], od_space:id(), dataset:id(), dataset:state(),
    dataset:id(), [binary()], time:seconds(), file_id:file_guid(), file_meta:type(), file_meta:path()
) ->
    ok.
verify_dataset(
    UserId, Providers, SpaceId, DatasetId, State, ParentId, ProtectionFlagsJson,
    CreationTime, RootFileGuid, RootFileType, RootFilePath
) ->
    lists:foreach(fun(Provider) ->
        Node = ?OCT_RAND_OP_NODE(Provider),
        UserSessId = oct_background:get_user_session_id(UserId, Provider),
        ListOpts = #{offset => 0, limit => 1000},

        GetDatasetsFun = case ParentId of
            undefined -> fun() -> list_top_dataset_ids(Node, UserSessId, SpaceId, State, ListOpts) end;
            _ -> fun() -> list_child_dataset_ids(Node, UserSessId, ParentId, ListOpts) end
        end,

        ?assertEqual(true, lists:member(DatasetId, GetDatasetsFun()), ?ATTEMPTS),

        EffProtectionFlags = case State of
            ?ATTACHED_DATASET -> file_meta:protection_flags_from_json(ProtectionFlagsJson);
            ?DETACHED_DATASET -> ?no_flags_mask
        end,

        ExpDatasetInfo = #dataset_info{
            id = DatasetId,
            state = State,
            root_file_guid = RootFileGuid,
            root_file_path = RootFilePath,
            root_file_type = RootFileType,
            creation_time = CreationTime,
            protection_flags = file_meta:protection_flags_from_json(ProtectionFlagsJson),
            eff_protection_flags = EffProtectionFlags,
            parent = ParentId,
            index = datasets_structure:pack_entry_index(filename:basename(RootFilePath), DatasetId)
        },
        ?assertEqual({ok, ExpDatasetInfo}, lfm_proxy:get_dataset_info(Node, UserSessId, DatasetId), ?ATTEMPTS)
    end, Providers).


%% @private
-spec list_top_dataset_ids(node(), session:id(), od_space:id(), dataset:state(), dataset_api:listing_opts()) ->
    [dataset:id()].
list_top_dataset_ids(Node, UserSessId, SpaceId, State, ListOpts) ->
    {ok, Datasets, _} = lfm_proxy:list_top_datasets(
        Node, UserSessId, SpaceId, State, ListOpts
    ),
    lists:map(fun({DatasetId, _, _}) -> DatasetId end, Datasets).


%% @private
-spec list_child_dataset_ids(node(), session:id(), dataset:id(), dataset_api:listing_opts()) ->
    [dataset:id()].
list_child_dataset_ids(Node, UserSessId, ParentId, ListOpts) ->
    {ok, Datasets, _} = lfm_proxy:list_children_datasets(Node, UserSessId, ParentId, ListOpts),
    lists:map(fun({DatasetId, _, _}) -> DatasetId end, Datasets).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    oct_background:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "api_tests",
        envs = [{op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}],
        posthook = fun(NewConfig) ->
            SpaceId = oct_background:get_space_id(space_krk_par),
            ozw_test_rpc:space_set_user_privileges(SpaceId, ?OCT_USER_ID(user3), [
                ?SPACE_MANAGE_DATASETS | privileges:space_member()
            ]),
            ozw_test_rpc:space_set_user_privileges(
                SpaceId, ?OCT_USER_ID(user4), privileges:space_member() -- [?SPACE_VIEW]
            ),
            NewConfig
        end
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().

init_per_group(_Group, Config) ->
    time_test_utils:freeze_time(Config),
    time_test_utils:set_current_time_seconds(1600000000),

    NewConfig = lfm_proxy:init(Config, false),

    % Randomly establish for space root dir
    SpaceDirDatasetId = case rand:uniform(2) of
        1 ->
            undefined;
        2 ->
            ct:pal("Establishing dataset for space root dir"),
            DatasetObj = onenv_dataset_test_utils:set_up_and_sync_dataset(user3, space_krk_par),
            DatasetObj#dataset_object.id
    end,
    [{space_dir_dataset, SpaceDirDatasetId} | NewConfig].

end_per_group(_Group, Config) ->
    onenv_dataset_test_utils:cleanup_all_datasets(space_krk_par),
    lfm_proxy:teardown(Config),
    time_test_utils:unfreeze_time(Config).

init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 10}),
    Config.

end_per_testcase(_Case, _Config) ->
    ok.
