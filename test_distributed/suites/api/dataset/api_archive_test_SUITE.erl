%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests concerning archives API (REST + gs).
%%% @end
%%%-------------------------------------------------------------------
-module(api_archive_test_SUITE).
-author("Jakub Kudzia").

-include("api_test_runner.hrl").
-include("onenv_test_utils.hrl").
-include("api_file_test_utils.hrl").
-include("modules/dataset/archive.hrl").
-include("modules/dataset/archivisation_tree.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/graph_sync/gri.hrl").
-include_lib("ctool/include/http/codes.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("inets/include/httpd.hrl").


-export([
    all/0, groups/0,
    init_per_suite/1, end_per_suite/1,
    init_per_group/2, end_per_group/2,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    create_archive/1,
    get_archive_info/1,
    modify_archive_description/1,
    get_dataset_archives/1,
    init_archive_purge_test/1
]).

%% httpd callback
-export([do/1]).


groups() -> [
    {all_tests, [parallel], [
        create_archive,
        get_archive_info,
        modify_archive_description,
        get_dataset_archives,
        init_archive_purge_test
    ]}
].

all() -> [
    {group, all_tests}
].

-define(ATTEMPTS, 120).
-define(NON_EXISTENT_ARCHIVE_ID, <<"NonExistentArchive">>).
-define(NON_EXISTENT_DATASET_ID, <<"NonExistentDataset">>).


-define(HTTP_SERVER_PORT, 8080).
-define(ARCHIVE_PRESERVED_PATH, "/archive_preserved").
-define(ARCHIVE_PURGED_PATH, "/archive_purged").

-define(ARCHIVE_PRESERVED_CALLBACK_URL(), ?CALLBACK_URL(?ARCHIVE_PRESERVED_PATH)).
-define(ARCHIVE_PURGED_CALLBACK_URL(), ?CALLBACK_URL(?ARCHIVE_PURGED_PATH)).
-define(CALLBACK_URL(Path), begin
    {ok, IpAddressBin} = ip_utils:to_binary(initializer:local_ip_v4()),
    str_utils:format_bin(<<"http://~s:~p~s">>, [IpAddressBin, ?HTTP_SERVER_PORT, Path])
end).

-define(CREATE_TEST_PROCESS, create_test_process).
-define(PURGE_TEST_PROCESS, purge_test_process).
-define(ARCHIVE_PERSISTED(ArchiveId, DatasetId), {archive_persisted, ArchiveId, DatasetId}).
-define(ARCHIVE_PURGED(ArchiveId, DatasetId), {archive_purged, ArchiveId, DatasetId}).
-define(SPACE, space_krk_par).

%%%===================================================================
%%% Archive dataset test functions
%%%===================================================================
create_archive(_Config) ->
    Providers = [krakow, paris],

    #object{
        dataset = #dataset_object{id = DatasetId}
    } = onenv_file_test_utils:create_and_sync_file_tree(user3, ?SPACE, #file_spec{dataset = #dataset_spec{}}),

    #object{
        dataset = #dataset_object{id = DetachedDatasetId}
    } = onenv_file_test_utils:create_and_sync_file_tree(user3, ?SPACE, #file_spec{dataset = #dataset_spec{state = ?DETACHED_DATASET}}),

    MemRef = api_test_memory:init(),

    true = register(?CREATE_TEST_PROCESS, self()),

    ?assert(onenv_api_test_runner:run_tests([
        #suite_spec{
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SPACE_KRK_PAR(?EPERM),
            randomly_select_scenarios = true,
            verify_fun = build_verify_archive_created_fun(MemRef, Providers),
            scenario_templates = [
                #scenario_template{
                    name = <<"Archive dataset using REST API">>,
                    type = rest,
                    prepare_args_fun = fun create_archive_prepare_rest_args_fun/1,
                    validate_result_fun = build_create_archive_validate_rest_call_result_fun(MemRef)
                },
                #scenario_template{
                    name = <<"Archive dataset using GS API">>,
                    type = gs,
                    prepare_args_fun = fun create_archive_prepare_gs_args_fun/1,
                    validate_result_fun = build_create_archive_validate_gs_call_result_fun(MemRef)
                }
            ],
            data_spec = #data_spec{
                required = [<<"datasetId">>],
                optional = [<<"config">>, <<"description">>, <<"preservedCallback">>, <<"purgedCallback">>],
                correct_values = #{
                    <<"datasetId">> => [DatasetId],
                    % pick only 4 random out of all possible configs
                    <<"config">> => lists_utils:random_sublist(generate_all_valid_configs(), 4, 4),
                    <<"description">> => [<<"Test description">>],
                    <<"preservedCallback">> => [?ARCHIVE_PRESERVED_CALLBACK_URL()],
                    <<"purgedCallback">> => [?ARCHIVE_PURGED_CALLBACK_URL()]
                },
                bad_values = [
                    {<<"datasetId">>, ?NON_EXISTENT_DATASET_ID, ?ERROR_FORBIDDEN},
                    {<<"datasetId">>, DetachedDatasetId,
                        ?ERROR_BAD_DATA(<<"datasetId">>, <<"Detached dataset cannot be modified.">>)},
                    {<<"config">>, #{<<"incremental">> => <<"not json">>}, ?ERROR_BAD_VALUE_JSON(<<"config.incremental">>)},
                    {<<"config">>, #{<<"incremental">> => #{<<"enabled">> => <<"not a boolean">>}}, ?ERROR_BAD_VALUE_BOOLEAN(<<"config.incremental.enabled">>)},
                    {<<"config">>, #{<<"incremental">> => #{<<"not_enable">> => true}}, ?ERROR_MISSING_REQUIRED_VALUE(<<"config.incremental.enabled">>)},
                    {<<"config">>, #{<<"incremental">> => #{<<"enabled">> => true, <<"basedOn">> => <<"invalid_id">>}}, ?ERROR_BAD_VALUE_IDENTIFIER(<<"config.incremental.basedOn">>)},
                    {<<"config">>, #{<<"includeDip">> => <<"not boolean">>}, ?ERROR_BAD_VALUE_BOOLEAN(<<"config.includeDip">>)},
                    {<<"config">>, #{<<"createNestedArchives">> => <<"not boolean">>},
                        ?ERROR_BAD_VALUE_BOOLEAN(<<"config.createNestedArchives">>)},
                    {<<"config">>, #{<<"layout">> => <<"not allowed layout">>},
                        ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"config.layout">>, ensure_binaries(?ARCHIVE_LAYOUTS))},
                    {<<"description">>, [123, 456], ?ERROR_BAD_VALUE_BINARY(<<"description">>)},
                    {<<"preservedCallback">>, <<"htp://wrong-url.org">>, ?ERROR_BAD_DATA(<<"preservedCallback">>)},
                    {<<"purgedCallback">>, <<"htp://wrong-url.org">>, ?ERROR_BAD_DATA(<<"purgedCallback">>)}
                ]
            }
        }
    ])).

%% @private
-spec generate_all_valid_configs() -> [archive_config:json()].
generate_all_valid_configs() ->
    LayoutValues = [undefined | ?ARCHIVE_LAYOUTS],
    IncrementalValues = lists:flatten([undefined | [#{<<"enabled">> => Enable} || Enable <- ?SUPPORTED_INCREMENTAL_ENABLED_VALUES]]),
    IncludeDipValues = [undefined | ?SUPPORTED_INCLUDE_DIP_VALUES],
    CreateNestedArchivesValues = [undefined, true, false],
    AllConfigsCombinations = [
        {Layout, Incremental, IncludeDip, CreateNestedArchives} ||
        Layout <- LayoutValues,
        Incremental <- IncrementalValues,
        IncludeDip <- IncludeDipValues,
        CreateNestedArchives <- CreateNestedArchivesValues
    ],
    lists:foldl(fun({L, I, ID, CNA}, Acc) ->
        Config = maps_utils:put_if_defined(#{}, <<"layout">>, L),
        Config2 = maps_utils:put_if_defined(Config, <<"incremental">>, I),
        Config3 = maps_utils:put_if_defined(Config2, <<"includeDip">>, ID),
        Config4 = maps_utils:put_if_defined(Config3, <<"createNestedArchives">>, CNA),
        [Config4 | Acc]
    end, [], AllConfigsCombinations).


%% @private
-spec create_archive_prepare_rest_args_fun(onenv_api_test_runner:api_test_ctx()) ->
    onenv_api_test_runner:rest_args().
create_archive_prepare_rest_args_fun(#api_test_ctx{data = Data}) ->
    #rest_args{
        method = post,
        path = <<"archives">>,
        headers = #{<<"content-type">> => <<"application/json">>},
        body = json_utils:encode(Data)
    }.


%% @private
-spec create_archive_prepare_gs_args_fun(onenv_api_test_runner:api_test_ctx()) ->
    onenv_api_test_runner:rest_args().
create_archive_prepare_gs_args_fun(#api_test_ctx{data = Data}) ->
    #gs_args{
        operation = create,
        gri = #gri{type = op_archive, aspect = instance, scope = private},
        data = Data
    }.


%% @private
-spec build_create_archive_validate_rest_call_result_fun(api_test_memory:mem_ref()) ->
    onenv_api_test_runner:validate_call_result_fun().
build_create_archive_validate_rest_call_result_fun(MemRef) ->
    fun(#api_test_ctx{node = TestNode}, Result) ->

        {ok, _, Headers, Body} = ?assertMatch(
            {ok, ?HTTP_201_CREATED, #{<<"Location">> := _}, #{<<"archiveId">> := _}},
            Result
        ),
        ArchiveId = maps:get(<<"archiveId">>, Body),
        api_test_memory:set(MemRef, archive_id, ArchiveId),

        ExpLocation = api_test_utils:build_rest_url(TestNode, [<<"archives">>, ArchiveId]),
        ?assertEqual(ExpLocation, maps:get(<<"Location">>, Headers))
    end.


%% @private
-spec build_create_archive_validate_gs_call_result_fun(api_test_memory:mem_ref()) ->
    onenv_api_test_runner:validate_call_result_fun().
build_create_archive_validate_gs_call_result_fun(MemRef) ->
    fun(#api_test_ctx{data = Data}, Result) ->
        DatasetId = maps:get(<<"datasetId">>, Data),

        {ok, #{<<"gri">> := ArchiveGri} = ArchiveData} = ?assertMatch({ok, _}, Result),

        #gri{id = ArchiveId} = ?assertMatch(
            #gri{type = op_archive, aspect = instance, scope = private},
            gri:deserialize(ArchiveGri)
        ),
        api_test_memory:set(MemRef, archive_id, ArchiveId),

        Config = archive_config:from_json(maps:get(<<"config">>, Data, #{})),
        Description = maps:get(<<"description">>, Data, ?DEFAULT_ARCHIVE_DESCRIPTION),
        PreservedCallback = maps:get(<<"preservedCallback">>, Data, undefined),
        PurgedCallback = maps:get(<<"purgedCallback">>, Data, undefined),

        ExpArchiveData = build_archive_gs_instance(ArchiveId, DatasetId, ?ARCHIVE_BUILDING, Config,
            Description, PreservedCallback, PurgedCallback, undefined),
        % state is removed from the map as it may be in pending, building or even preserved state when request is handled
        IgnoredKeys = [<<"state">>, <<"stats">>, <<"rootDir">>, <<"creationTime">>, <<"index">>, <<"baseArchive">>, <<"relatedDip">>],
        ExpArchiveData2 = maps:without(IgnoredKeys, ExpArchiveData),
        ArchiveData2 = maps:without(IgnoredKeys, ArchiveData),
        ?assertMatch(ExpArchiveData2, ArchiveData2),
        case archive_config:should_include_dip(Config) of
            false ->
                ?assertEqual(null, maps:get(<<"relatedDip">>, ArchiveData));
            true ->
                ?assertNotEqual(null, maps:get(<<"relatedDip">>, ArchiveData))
        end
    end.


%% @private
-spec build_verify_archive_created_fun(
    api_test_memory:mem_ref(),
    [oct_background:entity_selector()]
) ->
    onenv_api_test_runner:verify_fun().
build_verify_archive_created_fun(MemRef, Providers) ->
    fun
        (expected_success, #api_test_ctx{
            node = TestNode,
            client = ?USER(UserId),
            data = Data
        }) ->
            ArchiveId = api_test_memory:get(MemRef, archive_id),

            CreationTime = time_test_utils:global_seconds(TestNode),
            DatasetId = maps:get(<<"datasetId">>, Data),
            ConfigJson = maps:get(<<"config">>, Data, #{}),
            Description = maps:get(<<"description">>, Data, ?DEFAULT_ARCHIVE_DESCRIPTION),
            PreservedCallback = maps:get(<<"preservedCallback">>, Data, undefined),
            PurgedCallback = maps:get(<<"purgedCallback">>, Data, undefined),
            case PreservedCallback =/= undefined of
                true -> await_archive_preserved_callback_called(ArchiveId, DatasetId);
                false -> ok
            end,
            verify_archive(
                UserId, Providers, ArchiveId, DatasetId, CreationTime, ConfigJson,
                PreservedCallback, PurgedCallback, Description
            );
        (expected_failure, _) ->
            ok
    end.

%% @private
-spec await_archive_preserved_callback_called(archive:id(), dataset:id()) -> ok.
await_archive_preserved_callback_called(ArchiveId, DatasetId) ->
    Timeout = timer:minutes(3),
    receive
        ?ARCHIVE_PERSISTED(ArchiveId, DatasetId) -> ok
    after
        Timeout ->
            ct:fail("Archive ~p not created", [ArchiveId])
    end.

%%%===================================================================
%%% Get archive test functions
%%%===================================================================

get_archive_info(_Config) ->
    #object{dataset = #dataset_object{
        id = DatasetId,
        archives = [#archive_object{
            id = ArchiveId,
            config = Config,
            description = Description
        }]
    }} = onenv_file_test_utils:create_and_sync_file_tree(user3, ?SPACE,
        #file_spec{dataset = #dataset_spec{archives = 1}}
    ),

    ConfigJson = archive_config:to_json(Config),

    Providers = [krakow, paris],
    maybe_detach_dataset(Providers, DatasetId),

    ?assert(onenv_api_test_runner:run_tests([
        #suite_spec{
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SPACE_KRK_PAR(?EPERM),
            scenario_templates = [
                #scenario_template{
                    name = <<"Get archive using REST API">>,
                    type = rest,
                    prepare_args_fun = build_get_archive_prepare_rest_args_fun(ArchiveId),
                    validate_result_fun = fun(#api_test_ctx{}, {ok, RespCode, _, RespBody}) ->
                        RootDirGuid = get_root_dir_guid(ArchiveId),
                        {ok, DirObjectId} = file_id:guid_to_objectid(RootDirGuid),
                        ExpArchiveData = #{
                            <<"archiveId">> => ArchiveId,
                            <<"datasetId">> => DatasetId,
                            <<"state">> => atom_to_binary(?ARCHIVE_PRESERVED, utf8),
                            <<"rootDirectoryId">> => DirObjectId,
                            <<"description">> => Description,
                            <<"config">> => ConfigJson,
                            <<"preservedCallback">> => null,
                            <<"purgedCallback">> => null,
                            <<"stats">> => #{
                                <<"filesArchived">> => 1,
                                <<"filesFailed">> => 0,
                                <<"bytesArchived">> => 0
                            },
                            <<"relatedAipId">> => null
                        },
                        ?assertEqual(?HTTP_200_OK, RespCode),
                        % do not check baseArchive here as its value depends on previous tests
                        ?assertEqual(ExpArchiveData, maps:without([<<"baseArchiveId">>, <<"creationTime">>, <<"relatedDipId">>], RespBody)),
                        ?assertEqual(archive_config:should_include_dip(Config), maps:get(<<"relatedDipId">>, RespBody) =/= null)
                    end
                },
                #scenario_template{
                    name = <<"Get archive using GS API">>,
                    type = gs,
                    prepare_args_fun = build_get_archive_prepare_gs_args_fun(ArchiveId),
                    validate_result_fun = fun(#api_test_ctx{}, {ok, Result}) ->
                        DirGuid = get_root_dir_guid(ArchiveId),
                        ExpArchiveData = build_archive_gs_instance(ArchiveId, DatasetId, ?ARCHIVE_PRESERVED,
                            Config, Description, undefined, undefined, DirGuid),
                        ?assertEqual(ExpArchiveData, maps:without([<<"creationTime">>, <<"index">>, <<"relatedDip">>], Result)),
                        ?assertEqual(archive_config:should_include_dip(Config), maps:get(<<"relatedDip">>, Result) =/= null)
                    end
                }
            ],
            data_spec = #data_spec{
                bad_values = [{bad_id, ?NON_EXISTENT_ARCHIVE_ID, ?ERROR_NOT_FOUND}]
            }
        }
    ])).


%% @private
-spec build_get_archive_prepare_rest_args_fun(archive:id()) ->
    onenv_api_test_runner:prepare_args_fun().
build_get_archive_prepare_rest_args_fun(ArchiveId) ->
    fun(#api_test_ctx{data = Data}) ->
        {Id, _} = api_test_utils:maybe_substitute_bad_id(ArchiveId, Data),

        #rest_args{
            method = get,
            path = <<"archives/", Id/binary>>
        }
    end.


%% @private
-spec build_get_archive_prepare_gs_args_fun(archive:id()) ->
    onenv_api_test_runner:prepare_args_fun().
build_get_archive_prepare_gs_args_fun(ArchiveId) ->
    fun(#api_test_ctx{data = Data0}) ->
        {Id, Data1} = api_test_utils:maybe_substitute_bad_id(ArchiveId, Data0),

        #gs_args{
            operation = get,
            gri = #gri{type = op_archive, id = Id, aspect = instance, scope = private},
            data = Data1
        }
    end.


%%%===================================================================
%%% Update dataset test functions
%%%===================================================================

modify_archive_description(_Config) ->
    #object{dataset = #dataset_object{
        id = DatasetId,
        archives = ArchiveObjects
    }} = onenv_file_test_utils:create_and_sync_file_tree(user3, ?SPACE,
        #file_spec{dataset = #dataset_spec{archives = 30}}
    ),

    MemRef = api_test_memory:init(),
    api_test_memory:set(MemRef, archive_objects, ArchiveObjects),

    Providers = [krakow, paris],
    maybe_detach_dataset(Providers, DatasetId),

    ?assert(onenv_api_test_runner:run_tests([
        #suite_spec{
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SPACE_KRK_PAR(?EPERM),
            verify_fun = build_verify_modified_archive_description_fun(MemRef, Providers),
            scenario_templates = [
                #scenario_template{
                    name = <<"Modify archive description using REST API">>,
                    type = rest,
                    prepare_args_fun = build_update_archive_description_prepare_rest_args_fun(MemRef),
                    validate_result_fun = fun(#api_test_ctx{}, {ok, RespCode, _, RespBody}) ->
                        ?assertEqual({?HTTP_204_NO_CONTENT, #{}}, {RespCode, RespBody})
                    end
                },
                #scenario_template{
                    name = <<"Modify archive description using GS API">>,
                    type = gs,
                    prepare_args_fun = build_update_archive_description_prepare_gs_args_fun(MemRef),
                    validate_result_fun = fun(#api_test_ctx{}, Result) ->
                        ?assertEqual(ok, Result)
                    end
                }
            ],
            data_spec = #data_spec{
                optional = [<<"description">>],
                correct_values = #{
                    <<"description">> => [<<"">>, <<"NEW DESCRIPTION">>]
                },
                bad_values = [
                    {<<"description">>, 100, ?ERROR_BAD_VALUE_BINARY(<<"description">>)},
                    {bad_id, ?NON_EXISTENT_ARCHIVE_ID, ?ERROR_NOT_FOUND}
                ]
            }
        }
    ])).

%% @private
-spec build_update_archive_description_prepare_rest_args_fun(api_test_memory:mem_ref()) ->
    onenv_api_test_runner:prepare_args_fun().
build_update_archive_description_prepare_rest_args_fun(MemRef) ->
    fun(#api_test_ctx{data = Data0}) ->
        ArchiveObject = #archive_object{id = ArchiveId} = take_random_archive(MemRef),
        {Id, Data1} = api_test_utils:maybe_substitute_bad_id(ArchiveId, Data0),
        api_test_memory:set(MemRef, archive_to_modify, ArchiveObject#archive_object{id = Id}),

        #rest_args{
            method = patch,
            path = <<"archives/", Id/binary>>,
            headers = #{<<"content-type">> => <<"application/json">>},
            body = json_utils:encode(Data1)
        }
    end.


%% @private
-spec build_update_archive_description_prepare_gs_args_fun(api_test_memory:mem_ref()) ->
    onenv_api_test_runner:prepare_args_fun().
build_update_archive_description_prepare_gs_args_fun(MemRef) ->
    fun(#api_test_ctx{data = Data0}) ->
        ArchiveObject = #archive_object{id = ArchiveId} = take_random_archive(MemRef),
        {Id, Data1} = api_test_utils:maybe_substitute_bad_id(ArchiveId, Data0),
        api_test_memory:set(MemRef, archive_to_modify, ArchiveObject#archive_object{id = Id}),

        #gs_args{
            operation = update,
            gri = #gri{type = op_archive, id = Id, aspect = instance, scope = private},
            data = Data1
        }
    end.


%% @private
-spec build_verify_modified_archive_description_fun(
    api_test_memory:mem_ref(),
    [oct_background:entity_selector()]
) ->
    onenv_api_test_runner:verify_fun().
build_verify_modified_archive_description_fun(MemRef, Providers) ->
    fun(ExpResult, #api_test_ctx{data = Data}) ->
        case api_test_memory:get(MemRef, archive_to_modify) of
            #archive_object{id = ?NON_EXISTENT_ARCHIVE_ID} ->
                ok;
            #archive_object{id = ArchiveId, description = PrevDescription} ->

                ExpCurrentDescription = case ExpResult of
                    expected_failure ->
                        PrevDescription;
                    expected_success ->
                        PassedDescription = maps:get(<<"description">>, Data, undefined),
                        utils:ensure_defined(PassedDescription, PrevDescription)

                end,

                lists:foreach(fun(Provider) ->
                    Node = ?OCT_RAND_OP_NODE(Provider),
                    UserSessId = oct_background:get_user_session_id(user3, Provider),
                    ?assertMatch({ok, #archive_info{description = ExpCurrentDescription}},
                        lfm_proxy:get_archive_info(Node, UserSessId, ArchiveId), ?ATTEMPTS)
                end, Providers)

        end
    end.


%%%===================================================================
%%% Get dataset archives test functions
%%%===================================================================

get_dataset_archives(_Config) ->

    #object{dataset = #dataset_object{
        id = DatasetId,
        archives = ArchiveObjects
    }} = onenv_file_test_utils:create_and_sync_file_tree(user3, ?SPACE, #file_spec{dataset = #dataset_spec{
        % pick random count of archives
        archives = rand:uniform(1000)
    }}),

    Providers = [krakow, paris],
    RandomProvider = lists_utils:random_element(Providers),
    RandomProviderNode = ?OCT_RAND_OP_NODE(RandomProvider),
    UserSessId = oct_background:get_user_session_id(user3, RandomProvider),

    ArchiveInfos = lists:map(fun(#archive_object{id = ArchiveId}) ->
        {ok, ArchiveInfo} = lfm_proxy:get_archive_info(RandomProviderNode, UserSessId, ArchiveId),
        ArchiveInfo
    end, ArchiveObjects),

    % pick first and last index as token test values
    % list of archives is sorted descending by creation time
    #archive_info{index = LastIndex} = hd(ArchiveInfos),
    #archive_info{index = FirstIndex} = lists:last(ArchiveInfos),
    % pick  random value for index param
    #archive_info{index = RandomIndex} = lists_utils:random_element(ArchiveInfos),

    maybe_detach_dataset(Providers, DatasetId),

    ?assert(onenv_api_test_runner:run_tests([
        #suite_spec{
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SPACE_KRK_PAR(?EPERM),
            randomly_select_scenarios = true,
            scenario_templates = [
                #scenario_template{
                    name = <<"Get dataset archives using REST API">>,
                    type = rest,
                    prepare_args_fun = build_get_dataset_archives_prepare_rest_args_fun(DatasetId),
                    validate_result_fun = fun(#api_test_ctx{data = Data}, {ok, ?HTTP_200_OK, _, Response}) ->
                        validate_listed_archives(Response, Data, ArchiveInfos, rest)
                    end
                },
                #scenario_template{
                    name = <<"Get dataset archives using GS API">>,
                    type = gs,
                    prepare_args_fun = build_get_dataset_archives_prepare_gs_args_fun(DatasetId),
                    validate_result_fun = fun(#api_test_ctx{data = Data}, {ok, Result}) ->
                        validate_listed_archives(Result, Data, ArchiveInfos, graph_sync)
                    end
                }
            ],
            data_spec = #data_spec{
                optional = [<<"limit">>, <<"offset">>, <<"index">>, <<"token">>],
                correct_values = #{
                    <<"limit">> => [1, 100],
                    <<"offset">> => [1, 3, 10],
                    <<"index">> => [<<"null">>, null, RandomIndex, <<"zzzzzzzzzzzz">>],
                    <<"token">> => [<<"null">>, null | [http_utils:base64url_encode(Index) || Index <- [FirstIndex, RandomIndex, LastIndex]]]
                },
                bad_values = [
                    {bad_id, ?NON_EXISTENT_ARCHIVE_ID, ?ERROR_NOT_FOUND},
                    {<<"limit">>, true, ?ERROR_BAD_VALUE_INTEGER(<<"limit">>)},
                    {<<"limit">>, -100, ?ERROR_BAD_VALUE_NOT_IN_RANGE(<<"limit">>, 1, 1000)},
                    {<<"limit">>, 0, ?ERROR_BAD_VALUE_NOT_IN_RANGE(<<"limit">>, 1, 1000)},
                    {<<"limit">>, 1001, ?ERROR_BAD_VALUE_NOT_IN_RANGE(<<"limit">>, 1, 1000)},
                    {<<"offset">>, <<"abc">>, ?ERROR_BAD_VALUE_INTEGER(<<"offset">>)},
                    {<<"index">>, 1, {gs, ?ERROR_BAD_VALUE_BINARY(<<"index">>)}},
                    {<<"token">>, 1, {gs, ?ERROR_BAD_VALUE_BINARY(<<"token">>)}}
                ]
            }
        }
    ])).


%% @private
-spec build_get_dataset_archives_prepare_rest_args_fun(dataset:id()) ->
    onenv_api_test_runner:prepare_args_fun().
build_get_dataset_archives_prepare_rest_args_fun(ValidDatasetId) ->
    fun(#api_test_ctx{data = Data0}) ->
        Data1 = utils:ensure_defined(Data0, #{}),
        {Id, Data2} = api_test_utils:maybe_substitute_bad_id(ValidDatasetId, Data1),

        RestPath = <<"datasets/", Id/binary, "/archives">>,

        #rest_args{
            method = get,
            path = http_utils:append_url_parameters(
                RestPath,
                maps:with([<<"limit">>, <<"offset">>, <<"index">>, <<"token">>], Data2)
            )
        }
    end.

%% @private
-spec build_get_dataset_archives_prepare_gs_args_fun(dataset:id()) ->
    onenv_api_test_runner:prepare_args_fun().
build_get_dataset_archives_prepare_gs_args_fun(DatasetId) ->
    fun(#api_test_ctx{data = Data0}) ->
        {GriId, Data1} = api_test_utils:maybe_substitute_bad_id(DatasetId, Data0),

        #gs_args{
            operation = get,
            gri = #gri{type = op_dataset, id = GriId, aspect = archives_details, scope = private},
            data = Data1
        }
    end.


%% @private
-spec validate_listed_archives(
    ListingResult :: term(),
    Params :: map(),
    AllArchives :: [lfm_datasets:archive_info()],
    Format :: rest | graph_sync
) ->
    ok | no_return().
validate_listed_archives(ListingResult, Params, AllArchives, Format) ->
    Limit = maps:get(<<"limit">>, Params, 1000),
    Offset = maps:get(<<"offset">>, Params, 0),
    Index = case maps:get(<<"index">>, Params, undefined) of
        undefined -> <<>>;
        null when Format == rest -> <<"null">>;
        null -> <<>>;
        DefinedIndex -> DefinedIndex
    end,

    AllArchivesSorted = lists:sort(fun(#archive_info{index = Index1}, #archive_info{index = Index2}) ->
        Index1 =< Index2
    end, AllArchives),

    Token = case maps:get(<<"token">>, Params, undefined) of
        undefined -> undefined;
        null when Format == rest -> http_utils:base64url_decode(<<"null">>);
        null -> undefined;
        EncodedToken -> http_utils:base64url_decode(EncodedToken)
    end,

    StrippedArchives = lists:dropwhile(fun(#archive_info{index = ArchiveIndex}) ->
        case Token =:= undefined of
            true -> ArchiveIndex < Index;
            false -> ArchiveIndex =< Token
        end
    end, AllArchivesSorted),

    {ExpArchives1, IsLast} = case Offset >= length(StrippedArchives) of
        true ->
            {[], true};
        false ->
            SubList = lists:sublist(StrippedArchives, Offset + 1, Limit),
            {SubList, length(SubList) < Limit}
    end,

    ExpArchives2 = lists:map(fun(Info = #archive_info{id = ArchiveId, index = Index}) ->
        case Format of
            rest -> {Index, ArchiveId};
            graph_sync -> Info
        end
    end, ExpArchives1),

    ExpResult = case Format of
        graph_sync -> dataset_gui_gs_translator:translate_archives_details_list(ExpArchives2, IsLast);
        rest -> dataset_rest_translator:translate_archives_list(ExpArchives2, IsLast)
    end,
    ?assertEqual(ExpResult, ListingResult).


%%%===================================================================
%%% Init purge of archive test
%%%===================================================================

init_archive_purge_test(_Config) ->
    Providers = [krakow, paris],

    #object{dataset = #dataset_object{
        id = DatasetId,
        archives = ArchiveObjects
    }} = onenv_file_test_utils:create_and_sync_file_tree(user3, ?SPACE, #file_spec{dataset = #dataset_spec{
        archives = 30
    }}),

    MemRef = api_test_memory:init(),
    api_test_memory:set(MemRef, archive_objects, ArchiveObjects),

    maybe_detach_dataset(Providers, DatasetId),
    true = register(?PURGE_TEST_PROCESS, self()),

    ?assert(onenv_api_test_runner:run_tests([
        #suite_spec{
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SPACE_KRK_PAR(?EPERM),
            verify_fun = build_verify_archive_purged_fun(MemRef, Providers, DatasetId),
            scenario_templates = [
                #scenario_template{
                    name = <<"Init archive purge using REST API">>,
                    type = rest,
                    prepare_args_fun = build_init_purge_archive_prepare_rest_args_fun(MemRef),
                    validate_result_fun = fun(_, {ok, RespCode, _, RespBody}) ->
                        ?assertEqual({?HTTP_204_NO_CONTENT, #{}}, {RespCode, RespBody})
                    end
                },
                #scenario_template{
                    name = <<"Init archive purge using GS API">>,
                    type = gs,
                    prepare_args_fun = build_init_purge_archive_prepare_gs_args_fun(MemRef),
                    validate_result_fun = fun(_, Result) -> ?assertEqual(ok, Result) end
                }
            ],
            data_spec = #data_spec{
                optional = [<<"purgedCallback">>],
                correct_values = #{
                    <<"purgedCallback">> => [?ARCHIVE_PURGED_CALLBACK_URL()]
                },
                bad_values = [
                    {bad_id, ?NON_EXISTENT_ARCHIVE_ID, ?ERROR_NOT_FOUND},
                    {<<"purgedCallback">>, <<"htp://wrong-url.org">>, ?ERROR_BAD_DATA(<<"purgedCallback">>)}
                ]
            }
        }
    ])).


%% @private
-spec build_init_purge_archive_prepare_rest_args_fun(api_test_memory:mem_ref()) ->
    onenv_api_test_runner:prepare_args_fun().
build_init_purge_archive_prepare_rest_args_fun(MemRef) ->
    fun(#api_test_ctx{data = Data0}) ->
        ArchiveObject = #archive_object{id = ArchiveId} = take_random_archive(MemRef),
        {Id, _} = api_test_utils:maybe_substitute_bad_id(ArchiveId, Data0),
        api_test_memory:set(MemRef, archive_to_purge, ArchiveObject#archive_object{id = Id}),

        #rest_args{
            method = post,
            headers = #{<<"content-type">> => <<"application/json">>},
            path = <<"archives/", Id/binary, "/init_purge">>,
            body = json_utils:encode(Data0)
        }
    end.


%% @private
-spec build_init_purge_archive_prepare_gs_args_fun(api_test_memory:mem_ref()) ->
    onenv_api_test_runner:prepare_args_fun().
build_init_purge_archive_prepare_gs_args_fun(MemRef) ->
    fun(#api_test_ctx{data = Data0}) ->
        ArchiveObject = #archive_object{id = ArchiveId} = take_random_archive(MemRef),
        {Id, _} = api_test_utils:maybe_substitute_bad_id(ArchiveId, Data0),
        api_test_memory:set(MemRef, archive_to_purge, ArchiveObject#archive_object{id = Id}),

        #gs_args{
            operation = create,
            gri = #gri{type = op_archive, id = Id, aspect = purge, scope = private},
            data = Data0
        }
    end.

%% @private
-spec build_verify_archive_purged_fun(
    api_test_memory:mem_ref(),
    [oct_background:entity_selector()],
    dataset:id()
) ->
    onenv_api_test_runner:verify_fun().
build_verify_archive_purged_fun(MemRef, Providers, DatasetId) ->

    fun(ExpResult, #api_test_ctx{data = Data}) ->
        case api_test_memory:get(MemRef, archive_to_purge) of
            #archive_object{id = ?NON_EXISTENT_ARCHIVE_ID} ->
                ok;
            undefined ->
                ct:fail("undefined archive to remove");
            #archive_object{id = ArchiveId} ->

                case ExpResult of
                    expected_success ->
                        case maps:is_key(<<"purgedCallback">>, Data) of
                            true -> await_archive_purged_callback_called(ArchiveId, DatasetId);
                            false -> ok
                        end;
                    expected_failure ->
                        ok
                end,

                lists:foreach(fun(Provider) ->
                    Node = ?OCT_RAND_OP_NODE(Provider),
                    UserSessId = oct_background:get_user_session_id(user2, Provider),
                    ListOpts = #{offset => 0, limit => 1000},
                    ListArchiveFun = fun() ->
                        list_archive_ids(Node, UserSessId, DatasetId, ListOpts)
                    end,
                    GetArchiveInfo = fun() -> lfm_proxy:get_archive_info(Node, UserSessId, ArchiveId) end,

                    case ExpResult of
                        expected_success ->
                            ?assertEqual({error, ?ENOENT}, GetArchiveInfo(), ?ATTEMPTS),
                            ?assertEqual(false, lists:member(ArchiveId, ListArchiveFun()), ?ATTEMPTS);
                        expected_failure ->
                            ?assertMatch({ok, _}, GetArchiveInfo(), ?ATTEMPTS),
                            ?assertEqual(true, lists:member(ArchiveId, ListArchiveFun()), ?ATTEMPTS)
                    end
                end, Providers)
        end
    end.

%% @private
-spec await_archive_purged_callback_called(archive:id(), dataset:id()) -> ok.
await_archive_purged_callback_called(ArchiveId, DatasetId) ->
    Timeout = timer:seconds(?ATTEMPTS),
    receive
        ?ARCHIVE_PURGED(ArchiveId, DatasetId) -> ok
    after
        Timeout ->
            ct:fail("Archive ~p not purged", [ArchiveId])
    end.

%%%===================================================================
%%% Common archive test utils
%%%===================================================================

%% @private
-spec verify_archive(
    od_user:id(), [oct_background:entity_selector()], archive:id(), dataset:id(), archive:timestamp(),
    json_utils:json_map(), archive:callback(), archive:callback(), archive:description()
) ->
    ok.
verify_archive(
    UserId, Providers, ArchiveId, DatasetId, CreationTime, ConfigJson,
    PreservedCallback, PurgedCallback, Description
) ->
    lists:foreach(fun(Provider) ->
        Node = ?OCT_RAND_OP_NODE(Provider),
        UserSessId = oct_background:get_user_session_id(UserId, Provider),
        ListOpts = #{offset => 0, limit => 1000},
        GetDatasetsFun =  fun() -> list_archive_ids(Node, UserSessId, DatasetId, ListOpts) end,
        ?assertEqual(true, lists:member(ArchiveId, GetDatasetsFun()), ?ATTEMPTS),
        RootDirGuid = get_root_dir_guid(ArchiveId),
        Config = archive_config:from_json(ConfigJson),
        ExpArchiveInfo = #archive_info{
            id = ArchiveId,
            dataset_id = DatasetId,
            state = ?ARCHIVE_PRESERVED,
            root_dir_guid = RootDirGuid,
            creation_time = CreationTime,
            config = Config,
            preserved_callback = PreservedCallback,
            purged_callback = PurgedCallback,
            description = Description,
            index = archives_list:index(ArchiveId, CreationTime),
            stats = archive_stats:new(1, 0, 0)
        },
        GetArchiveInfoFun = fun() ->
            case lfm_proxy:get_archive_info(Node, UserSessId, ArchiveId) of
                {ok, ActualArchiveInfo} ->
                    ?assertEqual(archive_config:should_include_dip(Config), ActualArchiveInfo#archive_info.related_dip =/= undefined),
                    ActualArchiveInfo#archive_info{
                        % baseArchiveId is the id of the last successfully preserved, so it depends on previous test cases.
                        base_archive_id = undefined,
                        % DIP is created alongside AIP archive, so value of `relatedDip` field is not know beforehand. 
                        related_dip = undefined
                    };
                {error, _} = Error  ->
                    Error
            end
        end,
        ?assertEqual(ExpArchiveInfo, GetArchiveInfoFun(), ?ATTEMPTS)
    end, Providers).


%% @private
-spec list_archive_ids(node(), session:id(), dataset:id(), dataset_api:listing_opts()) ->
    [archive:id()].
list_archive_ids(Node, UserSessId, DatasetId, ListOpts) ->
    {ok, Datasets, _} = lfm_proxy:list_archives(Node, UserSessId, DatasetId, ListOpts),
    lists:map(fun({_, ArchiveId}) -> ArchiveId end, Datasets).


%% @private
-spec build_archive_gs_instance(archive:id(), dataset:id(), archive:state(), archive:config(),
    archive:description(), archive:callback(), archive:callback(), file_id:file_guid()) -> json_utils:json_term().
build_archive_gs_instance(ArchiveId, DatasetId, State, Config, Description, PreservedCallback, PurgedCallback,
    RootDirGuid
) ->
    BasicInfo = archive_gui_gs_translator:translate_archive_info(#archive_info{
        id = ArchiveId,
        dataset_id = DatasetId,
        state = str_utils:to_binary(State),
        root_dir_guid = RootDirGuid,
        config = Config,
        description = Description,
        preserved_callback = PreservedCallback,
        purged_callback = PurgedCallback,
        stats = archive_stats:new(1, 0, 0)
    }),
    maps:without([<<"creationTime">>, <<"index">>, <<"relatedDip">>], BasicInfo#{<<"revision">> => 1}).


-spec take_random_archive(api_test_memory:mem_ref()) -> onenv_archive_test_utils:archive_object().
take_random_archive(MemRef) ->
    case lists_utils:shuffle(api_test_memory:get(MemRef, archive_objects)) of
        [ArchiveObject | RestArchiveIds] ->
            api_test_memory:set(MemRef, archive_objects, RestArchiveIds),
            ArchiveObject;
        [] ->
            ct:fail("List of created archives is empty")
    end.


-spec get_root_dir_guid(archive:id()) -> file_id:file_guid().
get_root_dir_guid(ArchiveId) ->
    file_id:pack_guid(?ARCHIVE_DIR_UUID(ArchiveId), oct_background:get_space_id(?SPACE)).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    oct_background:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "api_tests",
        envs = [{op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}],
        posthook = fun(NewConfig) ->
            SpaceId = oct_background:get_space_id(?SPACE),
            ozw_test_rpc:space_set_user_privileges(SpaceId, ?OCT_USER_ID(user3), [
                ?SPACE_MANAGE_DATASETS, ?SPACE_VIEW_ARCHIVES, ?SPACE_CREATE_ARCHIVES,
                ?SPACE_REMOVE_ARCHIVES, ?SPACE_RECALL_ARCHIVES | privileges:space_member()
            ]),
            ozw_test_rpc:space_set_user_privileges(
                SpaceId, ?OCT_USER_ID(user4), privileges:space_member() -- [?SPACE_VIEW]
            ),
            start_http_server(),
            NewConfig
        end
    }).


end_per_suite(_Config) ->
    stop_http_server(),
    oct_background:end_per_suite().

init_per_group(_Group, Config) ->
    time_test_utils:freeze_time(Config),
    lfm_proxy:init(Config, false).

end_per_group(_Group, Config) ->
    onenv_dataset_test_utils:cleanup_all_datasets(?SPACE),
    lfm_proxy:teardown(Config),
    time_test_utils:unfreeze_time(Config).

init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 20}),
    Config.


end_per_testcase(_Case, _Config) ->
    ok.

%%%===================================================================
%%% HTTP server used for checking HTTP callbacks
%%%===================================================================

start_http_server() ->
    inets:start(),
    {ok, _} = inets:start(httpd, [
        {port, ?HTTP_SERVER_PORT},
        {server_name, "httpd_test"},
        {server_root, "/tmp"},
        {document_root, "/tmp"},
        {modules, [?MODULE]}
    ]).


stop_http_server() ->
    inets:stop().

do(#mod{method = "POST", request_uri = ?ARCHIVE_PRESERVED_PATH, entity_body = Body}) ->
    handle_callback_message(fun() ->
        #{<<"archiveId">> := ArchiveId, <<"datasetId">> := DatasetId} = json_utils:decode(Body),
        ?CREATE_TEST_PROCESS ! ?ARCHIVE_PERSISTED(ArchiveId, DatasetId)
    end);
do(#mod{method = "POST", request_uri = ?ARCHIVE_PURGED_PATH, entity_body = Body}) ->
    handle_callback_message(fun() ->
        #{<<"archiveId">> := ArchiveId,  <<"datasetId">> := DatasetId} = json_utils:decode(Body),
        ?PURGE_TEST_PROCESS ! ?ARCHIVE_PURGED(ArchiveId, DatasetId)
    end).


-spec handle_callback_message(function()) -> tuple().
handle_callback_message(HandleFun) ->
    ResponseCode = case rand:uniform(4) of
        N when N =< 3 ->
            HandleFun(),
            ?HTTP_204_NO_CONTENT;
        4 ->
            ?HTTP_500_INTERNAL_SERVER_ERROR
    end,
    {proceed, [{response,{ResponseCode, []}}]}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec ensure_binaries([term()]) -> [binary()].
ensure_binaries(List) ->
    [str_utils:to_binary(Element) || Element <- List].


-spec maybe_detach_dataset([oct_background:entity_selector()], dataset:id()) -> ok.
maybe_detach_dataset(Providers, DatasetId) ->
    [Provider | OtherProviders] = lists_utils:shuffle(Providers),
    Node = oct_background:get_random_provider_node(Provider),
    UserSessId = oct_background:get_user_session_id(user3, Provider),
    case rand:uniform(2) of
        1 ->
            ok;
        2 ->
            ok = lfm_proxy:detach_dataset(Node, UserSessId, DatasetId),
            lists_utils:pforeach(fun(P) ->
                N = oct_background:get_random_provider_node(P),
                S = oct_background:get_user_session_id(user3, P),
                ?assertMatch({ok, #dataset_info{state = ?DETACHED_DATASET}},
                    lfm_proxy:get_dataset_info(N, S, DatasetId), ?ATTEMPTS)
            end, OtherProviders)
    end.