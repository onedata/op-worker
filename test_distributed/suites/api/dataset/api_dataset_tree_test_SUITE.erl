%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests concerning dataset tree API (REST + gs).
%%% @end
%%%-------------------------------------------------------------------
-module(api_dataset_tree_test_SUITE).
-author("Bartosz Walkowicz").

-include("api_file_test_utils.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include("onenv_test_utils.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/graph_sync/gri.hrl").
-include_lib("ctool/include/http/codes.hrl").
-include_lib("ctool/include/privileges.hrl").

-export([
    groups/0, all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_group/2, end_per_group/2,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    get_top_datasets_test/1,
    get_child_datasets_test/1,
    get_file_dataset_summary_test/1
]).

groups() -> [
    {all_tests, [parallel], [
        get_top_datasets_test,
        get_child_datasets_test,
        get_file_dataset_summary_test
    ]}
].

all() -> [
    {group, all_tests}
].


-define(FILE_TREE_SPEC, #dir_spec{children = [
    #dir_spec{
        name = <<"get_top_datasets">>,
        children = [
            #file_spec{dataset = #dataset_spec{state = ?ATTACHED_DATASET}},
            #file_spec{dataset = #dataset_spec{state = ?ATTACHED_DATASET}},
            #file_spec{dataset = #dataset_spec{state = ?ATTACHED_DATASET}},
            #file_spec{dataset = #dataset_spec{state = ?DETACHED_DATASET}},
            #file_spec{dataset = #dataset_spec{
                state = ?DETACHED_DATASET,
                protection_flags = [?DATA_PROTECTION_BIN, ?METADATA_PROTECTION_BIN]
            }}
        ]
    },
    #dir_spec{
        name = <<"get_child_datasets_test">>,
        dataset = #dataset_spec{
            state = ?ATTACHED_DATASET,
            protection_flags = [?METADATA_PROTECTION_BIN]
        },
        children = [#dir_spec{
            dataset = #dataset_spec{
                state = ?DETACHED_DATASET,
                protection_flags = [?DATA_PROTECTION_BIN]
            },
            children = [#dir_spec{
                dataset = #dataset_spec{
                    state = ?ATTACHED_DATASET,
                    protection_flags = [?DATA_PROTECTION_BIN]
                },
                children = [#dir_spec{
                    name = <<"dir_with_no_dataset_in_the_middle">>,
                    children = [
                        #file_spec{dataset = #dataset_spec{
                            state = ?ATTACHED_DATASET,
                            protection_flags = [?DATA_PROTECTION_BIN]
                        }},
                        #file_spec{dataset = #dataset_spec{state = ?ATTACHED_DATASET}},
                        #file_spec{dataset = #dataset_spec{
                            state = ?ATTACHED_DATASET,
                            protection_flags = [?DATA_PROTECTION_BIN, ?METADATA_PROTECTION_BIN]
                        }},
                        #file_spec{dataset = #dataset_spec{state = ?DETACHED_DATASET}},
                        #file_spec{dataset = #dataset_spec{
                            state = ?DETACHED_DATASET,
                            protection_flags = [?METADATA_PROTECTION_BIN]
                        }}
                    ]
                }]
            }]
        }]
    },
    #dir_spec{
        name = <<"get_file_dataset_summary_test">>,
        dataset = #dataset_spec{
            state = ?ATTACHED_DATASET,
            protection_flags = [?METADATA_PROTECTION_BIN]
        },
        children = [#dir_spec{
            dataset = #dataset_spec{
                state = ?DETACHED_DATASET,
                protection_flags = [?DATA_PROTECTION_BIN]
            },
            children = [#dir_spec{
                name = <<"dir_with_no_dataset_in_the_middle">>,
                children = [#dir_spec{
                    dataset = #dataset_spec{state = ?ATTACHED_DATASET},
                    children = [#dir_spec{children = [
                        #file_spec{dataset = #dataset_spec{
                            state = ?ATTACHED_DATASET,
                            protection_flags = [?DATA_PROTECTION_BIN]
                        }},
                        #dir_spec{dataset = #dataset_spec{
                            state = ?DETACHED_DATASET,
                            protection_flags = [?DATA_PROTECTION_BIN]
                        }},
                        #file_spec{}
                    ]}]
                }]
            }]
        }]
    }
]}).

-define(DUMMY_SHARE_ID, <<"SHARE_ID">>).

-define(ATTEMPTS, 30).


%%%===================================================================
%%% List/Get top datasets test functions
%%%===================================================================


get_top_datasets_test(Config) ->
    SpaceId = oct_background:get_space_id(space_krk_par),
    SpaceDirPath = filename:join(["/", ?SPACE_KRK_PAR]),

    SpaceDirDataset = ?config(space_dir_dataset, Config),
    FileTree = ?config(file_tree, Config),

    ct:pal("Test listing top attached datasets"),

    TopAttachedDatasets = case SpaceDirDataset of
        undefined ->
            onenv_dataset_test_utils:get_exp_child_datasets(
                ?ATTACHED_DATASET, SpaceDirPath, undefined, FileTree
            );
        {_, _, _} ->
            [SpaceDirDataset]
    end,
    get_top_datasets_test_base(SpaceId, ?ATTACHED_DATASET, TopAttachedDatasets),

    ct:pal("Test listing top detached datasets"),

    TopDetachedDatasets = onenv_dataset_test_utils:get_exp_child_datasets(
        ?DETACHED_DATASET, SpaceDirPath, undefined, FileTree
    ),
    get_top_datasets_test_base(SpaceId, ?DETACHED_DATASET, TopDetachedDatasets).


%% @private
-spec get_top_datasets_test_base(od_space:id(), dataset:state(), [{dataset:id(), map()}]) ->
    true | no_return().
get_top_datasets_test_base(SpaceId, State, TopDatasets) ->
    ?assert(onenv_api_test_runner:run_tests([
        #suite_spec{
            target_nodes = [krakow, paris],
            client_spec = #client_spec{
                correct = [user2, user3],
                unauthorized = [nobody],
                forbidden_not_in_space = [user1],
                forbidden_in_space = [user4]
            },
            scenario_templates = [
                #scenario_template{
                    name = <<"Get top datasets using REST API">>,
                    type = rest,
                    prepare_args_fun = build_get_top_datasets_prepare_rest_args_fun(SpaceId, State),
                    validate_result_fun = fun(#api_test_ctx{data = Data}, {ok, ?HTTP_200_OK, _, Response}) ->
                        validate_listed_datasets(Response, Data, TopDatasets)
                    end
                },
                #scenario_template{
                    name = <<"GET top datasets using GS API">>,
                    type = gs,
                    prepare_args_fun = build_get_top_datasets_prepare_gs_args_fun(SpaceId, State),
                    validate_result_fun = fun(#api_test_ctx{data = Data}, {ok, Result}) ->
                        validate_listed_datasets(Result, Data, TopDatasets)
                    end
                }
            ],
            randomly_select_scenarios = true,
            data_spec = #data_spec{
                optional = [<<"limit">>, <<"offset">>],
                correct_values = #{
                    <<"limit">> => [1, 100],
                    <<"offset">> => [1, 3, 10]
                },
                bad_values = [
                    {bad_id, <<"NonExistentSpace">>, ?ERROR_FORBIDDEN},
                    {<<"state">>, 10, {gs, ?ERROR_BAD_VALUE_BINARY(<<"state">>)}},
                    {<<"state">>, <<"active">>, ?ERROR_BAD_VALUE_NOT_ALLOWED(
                        <<"state">>, [<<"attached">>, <<"detached">>]
                    )},
                    {<<"limit">>, true, ?ERROR_BAD_VALUE_INTEGER(<<"limit">>)},
                    {<<"limit">>, -100, ?ERROR_BAD_VALUE_NOT_IN_RANGE(<<"limit">>, 1, 1000)},
                    {<<"limit">>, 0, ?ERROR_BAD_VALUE_NOT_IN_RANGE(<<"limit">>, 1, 1000)},
                    {<<"limit">>, 1001, ?ERROR_BAD_VALUE_NOT_IN_RANGE(<<"limit">>, 1, 1000)},
                    {<<"offset">>, <<"abc">>, ?ERROR_BAD_VALUE_INTEGER(<<"offset">>)}
                ]
            }
        }
    ])).


%% @private
-spec build_get_top_datasets_prepare_rest_args_fun(od_space:id(), dataset:state()) ->
    onenv_api_test_runner:prepare_args_fun().
build_get_top_datasets_prepare_rest_args_fun(SpaceId, State) ->
    fun(#api_test_ctx{data = Data0}) ->
        Data1 = utils:ensure_defined(Data0, #{}),
        Data2 = inject_state_if_not_defined(State, Data1),
        {Id, Data3} = api_test_utils:maybe_substitute_bad_id(SpaceId, Data2),

        RestPath = <<"spaces/", Id/binary, "/datasets">>,

        #rest_args{
            method = get,
            path = http_utils:append_url_parameters(
                RestPath,
                maps:with([<<"limit">>, <<"offset">>, <<"state">>], Data3)
            )
        }
    end.


%% @private
-spec build_get_top_datasets_prepare_gs_args_fun(od_space:id(), dataset:state()) ->
    onenv_api_test_runner:prepare_args_fun().
build_get_top_datasets_prepare_gs_args_fun(SpaceId, State) ->
    build_prepare_get_top_datasets_gs_args_fun(SpaceId, State, datasets).


%% @private
-spec build_prepare_get_top_datasets_gs_args_fun(od_space:id(), dataset:state(), gri:aspect()) ->
    onenv_api_test_runner:prepare_args_fun().
build_prepare_get_top_datasets_gs_args_fun(SpaceId, State, Aspect) ->
    fun(#api_test_ctx{data = Data0}) ->
        Data1 = utils:ensure_defined(Data0, #{}),
        Data2 = inject_state_if_not_defined(State, Data1),
        {GriId, Data3} = api_test_utils:maybe_substitute_bad_id(SpaceId, Data2),

        #gs_args{
            operation = get,
            gri = #gri{type = op_space, id = GriId, aspect = Aspect, scope = private},
            data = Data3
        }
    end.


%% @private
-spec inject_state_if_not_defined(dataset:state(), map()) -> map().
inject_state_if_not_defined(_State, #{<<"state">> := _} = Data) ->
    Data;
inject_state_if_not_defined(State, Data) ->
    case State of
        ?ATTACHED_DATASET ->
            case rand:uniform(2) of
                1 ->
                    % Attached state should be taken as default
                    Data;
                2 ->
                    Data#{<<"state">> => <<"attached">>}
            end;
        ?DETACHED_DATASET ->
            Data#{<<"state">> => <<"detached">>}
    end.


%%%===================================================================
%%% List/Get child datasets test functions
%%%===================================================================


get_child_datasets_test(Config) ->
    FileTree = ?config(file_tree, Config),

    % See 'create_env' for how file tree looks like
    #object{name = RootDirName, children = [_, #object{name = TestDirName, children = [
        DirWithDetachedDataset = #object{
            name = DirWithDetachedDatasetName,
            dataset = #dataset_object{id = DetachedDatasetId},
            children = [DirWithAttachedDataset = #object{
                name = DirWithAttachedDatasetName,
                dataset = #dataset_object{id = AttachedDatasetId}
            }]
        }
    ]}, _]} = FileTree,

    DirWithDetachedDatasetPath = filename:join([
        "/", ?SPACE_KRK_PAR, RootDirName, TestDirName, DirWithDetachedDatasetName
    ]),
    DirWithAttachedDatasetPath = filename:join([
        "/", DirWithDetachedDatasetPath, DirWithAttachedDatasetName
    ]),

    ct:pal("Listing child datasets of attached dataset"),

    DirWithDetachedDatasetPath = filename:join([
        "/", ?SPACE_KRK_PAR, RootDirName, TestDirName, DirWithDetachedDatasetName
    ]),
    AttachedChildDatasets = onenv_dataset_test_utils:get_exp_child_datasets(
        ?ATTACHED_DATASET, DirWithAttachedDatasetPath, AttachedDatasetId, DirWithAttachedDataset
    ),
    get_child_datasets_test_base(AttachedDatasetId, AttachedChildDatasets),

    ct:pal("Listing child datasets of detached dataset"),

    DetachedChildDatasets = onenv_dataset_test_utils:get_exp_child_datasets(
        ?DETACHED_DATASET, DirWithDetachedDatasetPath, DetachedDatasetId, DirWithDetachedDataset
    ),
    get_child_datasets_test_base(DetachedDatasetId, DetachedChildDatasets).


%% @private
-spec get_child_datasets_test_base(dataset:id(), [{dataset:id(), map()}]) ->
    true | no_return().
get_child_datasets_test_base(DatasetId, ChildDatasets) ->
    ?assert(onenv_api_test_runner:run_tests([
        #suite_spec{
            target_nodes = [krakow, paris],
            client_spec = ?CLIENT_SPEC_FOR_SPACE_KRK_PAR(?EPERM),
            scenario_templates = [
                #scenario_template{
                    name = <<"Get child datasets using REST API">>,
                    type = rest,
                    prepare_args_fun = build_get_child_datasets_prepare_rest_args_fun(DatasetId),
                    validate_result_fun = fun(#api_test_ctx{data = Data}, {ok, ?HTTP_200_OK, _, Response}) ->
                        validate_listed_datasets(Response, Data, ChildDatasets)
                    end
                },
                #scenario_template{
                    name = <<"GET child datasets using GS API">>,
                    type = gs,
                    prepare_args_fun = build_get_child_datasets_prepare_gs_args_fun(DatasetId),
                    validate_result_fun = fun(#api_test_ctx{data = Data}, {ok, Result}) ->
                        validate_listed_datasets(Result, Data, ChildDatasets)
                    end
                }
            ],
            randomly_select_scenarios = true,
            data_spec = #data_spec{
                optional = [<<"limit">>, <<"offset">>],
                correct_values = #{
                    <<"limit">> => [1, 100],
                    <<"offset">> => [1, 3, 10]
                },
                bad_values = [
                    {bad_id, <<"NonExistentDataset">>, ?ERROR_NOT_FOUND},
                    {<<"limit">>, true, ?ERROR_BAD_VALUE_INTEGER(<<"limit">>)},
                    {<<"limit">>, -100, ?ERROR_BAD_VALUE_NOT_IN_RANGE(<<"limit">>, 1, 1000)},
                    {<<"limit">>, 0, ?ERROR_BAD_VALUE_NOT_IN_RANGE(<<"limit">>, 1, 1000)},
                    {<<"limit">>, 1001, ?ERROR_BAD_VALUE_NOT_IN_RANGE(<<"limit">>, 1, 1000)},
                    {<<"offset">>, <<"abc">>, ?ERROR_BAD_VALUE_INTEGER(<<"offset">>)}
                ]
            }
        }
    ])).


%% @private
-spec build_get_child_datasets_prepare_rest_args_fun(dataset:id()) ->
    onenv_api_test_runner:prepare_args_fun().
build_get_child_datasets_prepare_rest_args_fun(ValidId) ->
    fun(#api_test_ctx{data = Data0}) ->
        Data1 = utils:ensure_defined(Data0, #{}),
        {Id, Data2} = api_test_utils:maybe_substitute_bad_id(ValidId, Data1),

        RestPath = <<"datasets/", Id/binary, "/children">>,

        #rest_args{
            method = get,
            path = http_utils:append_url_parameters(
                RestPath,
                maps:with([<<"limit">>, <<"offset">>], Data2)
            )
        }
    end.


%% @private
-spec build_get_child_datasets_prepare_gs_args_fun(dataset:id()) ->
    onenv_api_test_runner:prepare_args_fun().
build_get_child_datasets_prepare_gs_args_fun(DatasetId) ->
    build_prepare_get_child_datasets_gs_args_fun(DatasetId, children).


%% @private
-spec build_prepare_get_child_datasets_gs_args_fun(dataset:id(), gri:aspect()) ->
    onenv_api_test_runner:prepare_args_fun().
build_prepare_get_child_datasets_gs_args_fun(DatasetId, Aspect) ->
    fun(#api_test_ctx{data = Data0}) ->
        {GriId, Data1} = api_test_utils:maybe_substitute_bad_id(DatasetId, Data0),

        #gs_args{
            operation = get,
            gri = #gri{type = op_dataset, id = GriId, aspect = Aspect, scope = private},
            data = Data1
        }
    end.


%%%===================================================================
%%% Common listing datasets helper functions
%%%===================================================================


%% @private
-spec validate_listed_datasets(
    ListedDatasets :: term(),
    Params :: map(),
    AllDatasets :: [{binary(), map()}]
) ->
    ok | no_return().
validate_listed_datasets(ListingResult, Params, AllDatasets) ->
    Limit = maps:get(<<"limit">>, Params, 1000),
    Offset = maps:get(<<"offset">>, Params, 0),

    {ExpDatasets1, IsLast} = case Offset >= length(AllDatasets) of
        true ->
            {[], true};
        false ->
            SubList = lists:sublist(AllDatasets, Offset + 1, Limit),
            {SubList, lists:last(SubList) == lists:last(AllDatasets)}
    end,
    ExpDatasets2 = lists:map(fun({FileName, DatasetId, _Details}) ->
        #{
            <<"name">> => FileName,
            <<"id">> => DatasetId
        }
    end, ExpDatasets1),

    ExpResult = #{
        <<"datasets">> => ExpDatasets2,
        <<"isLast">> => IsLast
    },
    ?assertEqual(ExpResult, ListingResult).


%%%===================================================================
%%% Get file dataset summary test functions
%%%===================================================================


get_file_dataset_summary_test(Config) ->
    #object{children = [_, _, #object{
        dataset = #dataset_object{
            id = AttachedDataset1,
            state = ?ATTACHED_DATASET,
            protection_flags = Flags1
        },
        children = [#object{
            dataset = #dataset_object{state = ?DETACHED_DATASET},
            children = [#object{
                children = [#object{
                    dataset = #dataset_object{
                        id = AttachedDataset2,
                        state = ?ATTACHED_DATASET,
                        protection_flags = Flags2
                    },
                    children = [#object{children = [
                        #object{
                            guid = File1Guid,
                            dataset = #dataset_object{
                                id = AttachedDataset3,
                                state = ?ATTACHED_DATASET,
                                protection_flags = Flags3
                            }
                        },
                        #object{
                            guid = File2Guid,
                            dataset = #dataset_object{
                                id = DetachedDataset2,
                                state = ?DETACHED_DATASET,
                                protection_flags = _Flags4
                            }
                        },
                        #object{guid = File3Guid}
                    ]}]
                }]
            }]
        }]
    }]} = ?config(file_tree, Config),

    EffAncestorDatasets = case ?config(space_dir_dataset, Config) of
        undefined ->
            [AttachedDataset2, AttachedDataset1];
        {_, SpaceDatasetId, _} ->
            [AttachedDataset2, AttachedDataset1, SpaceDatasetId]
    end,

    ct:pal("Get dataset summary for file with attached dataset"),

    File1DatasetSummary = #file_eff_dataset_summary{
        direct_dataset = AttachedDataset3,
        eff_ancestor_datasets = EffAncestorDatasets,
        eff_protection_flags = file_meta:protection_flags_from_json(Flags1 ++ Flags2 ++ Flags3)
    },
    get_file_dataset_summary_test_base(File1Guid, File1DatasetSummary),

    ct:pal("Get dataset summary for file with detached dataset"),

    File2DatasetSummary = #file_eff_dataset_summary{
        direct_dataset = DetachedDataset2,
        eff_ancestor_datasets = EffAncestorDatasets,
        eff_protection_flags = file_meta:protection_flags_from_json(Flags1 ++ Flags2)
    },
    get_file_dataset_summary_test_base(File2Guid, File2DatasetSummary),

    ct:pal("Get dataset summary for file with no direct dataset"),

    File3DatasetSummary = #file_eff_dataset_summary{
        direct_dataset = undefined,
        eff_ancestor_datasets = EffAncestorDatasets,
        eff_protection_flags = file_meta:protection_flags_from_json(Flags1 ++ Flags2)
    },
    get_file_dataset_summary_test_base(File3Guid, File3DatasetSummary).


%% @private
-spec get_file_dataset_summary_test_base(file_id:file_guid(), lfm_datasets:file_eff_summary()) ->
    true | no_return().
get_file_dataset_summary_test_base(FileGuid, ExpSummary) ->
    ExpRestSummary = build_rest_dataset_summary(ExpSummary),
    ExpGsSummary = build_gs_dataset_summary(FileGuid, ExpSummary),

    ?assert(onenv_api_test_runner:run_tests([
        #suite_spec{
            target_nodes = [krakow, paris],
            client_spec = ?CLIENT_SPEC_FOR_SPACE_KRK_PAR(?EPERM),
            scenario_templates = [
                #scenario_template{
                    name = <<"Get file dataset summary using REST API">>,
                    type = rest,
                    prepare_args_fun = build_getfile_dataset_summary_prepare_rest_args_fun(FileGuid),
                    validate_result_fun = fun(_, {ok, ?HTTP_200_OK, _, Response}) ->
                        ?assertEqual(ExpRestSummary, Response)
                    end
                },
                #scenario_template{
                    name = <<"GET file dataset summary using GS API">>,
                    type = gs,
                    prepare_args_fun = build_get_file_dataset_summary_prepare_gs_args_fun(FileGuid),
                    validate_result_fun = fun(_, {ok, Result}) ->
                        ?assertEqual(ExpGsSummary, Result)
                    end
                }
            ],
            data_spec = api_test_utils:add_file_id_errors_for_operations_not_available_in_share_mode(
                FileGuid, ?DUMMY_SHARE_ID, undefined
            )
        }
    ])).


%% @private
-spec build_rest_dataset_summary(lfm_datasets:file_eff_summary()) -> map().
build_rest_dataset_summary(#file_eff_dataset_summary{
    direct_dataset = DirectDatasetId,
    eff_ancestor_datasets = EffAncestorDatasets,
    eff_protection_flags = EffProtectionFlags
}) ->
    #{
        <<"directDataset">> => utils:undefined_to_null(DirectDatasetId),
        <<"effectiveAncestorDatasets">> => EffAncestorDatasets,
        <<"effectiveProtectionFlags">> => file_meta:protection_flags_to_json(EffProtectionFlags)
    }.


%% @private
-spec build_gs_dataset_summary(file_id:file_guid(), lfm_datasets:file_eff_summary()) -> map().
build_gs_dataset_summary(FileGuid, DatasetSummary) ->
    BasicSummary = file_gui_gs_translator:translate_dataset_summary(DatasetSummary),
    BasicSummary#{
        <<"gri">> => gri:serialize(#gri{
            type = op_file, id = FileGuid, aspect = dataset_summary, scope = private
        }),
        <<"revision">> => 1
    }.


%% @private
-spec build_getfile_dataset_summary_prepare_rest_args_fun(file_id:file_guid()) ->
    onenv_api_test_runner:prepare_args_fun().
build_getfile_dataset_summary_prepare_rest_args_fun(FileGuid) ->
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),

    fun(#api_test_ctx{data = Data0}) ->
        Data1 = utils:ensure_defined(Data0, #{}),
        {Id, _} = api_test_utils:maybe_substitute_bad_id(FileObjectId, Data1),

        RestPath = <<"data/", Id/binary, "/dataset/summary">>,

        #rest_args{method = get, path = RestPath}
    end.


%% @private
-spec build_get_file_dataset_summary_prepare_gs_args_fun(file_id:file_guid()) ->
    onenv_api_test_runner:prepare_args_fun().
build_get_file_dataset_summary_prepare_gs_args_fun(FileGuid) ->
    fun(#api_test_ctx{data = Data0}) ->
        {GriId, Data1} = api_test_utils:maybe_substitute_bad_id(FileGuid, Data0),

        #gs_args{
            operation = get,
            gri = #gri{type = op_file, id = GriId, aspect = dataset_summary, scope = private},
            data = Data1
        }
    end.


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
    NewConfig = lfm_proxy:init(Config, false),

    SpaceId = oct_background:get_space_id(space_krk_par),

    SpaceDirDataset = case rand:uniform(2) of
        1 ->
            undefined;
        2 ->
            ct:pal("Establishing dataset for space root dir"),

            DatasetId = onenv_dataset_test_utils:set_up_and_sync_dataset(user3, SpaceId),

            DatasetDetails = #dataset_info{
                id = DatasetId,
                state = ?ATTACHED_DATASET,
                guid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
                path = filename:join(["/", ?SPACE_KRK_PAR]),
                type = ?DIRECTORY_TYPE,
                creation_time = time_test_utils:get_frozen_time_seconds(),
                protection_flags = [],
                parent = undefined
            },
            {?SPACE_KRK_PAR, DatasetId, DatasetDetails}
    end,
    FileTree = onenv_file_test_utils:create_and_sync_file_tree(
        user3, SpaceId, ?FILE_TREE_SPEC
    ),
    [{space_dir_dataset, SpaceDirDataset}, {file_tree, FileTree} | NewConfig].


end_per_group(_Group, Config) ->
    lfm_proxy:teardown(Config),
    time_test_utils:unfreeze_time(Config).


init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 10}),
    Config.


end_per_testcase(_Case, _Config) ->
    ok.
