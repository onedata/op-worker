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
-include("modules/archive/archive.hrl").
-include("proto/oneprovider/provider_messages.hrl").
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
    archive_dataset/1,
    get_archive_info/1,
    modify_archive_attrs/1,
    get_dataset_archives/1,
    init_archive_purge_test/1
]).

%% httpd callback
-export([do/1]).


groups() -> [
    {all_tests, [parallel], [
        archive_dataset,
        get_archive_info,
        modify_archive_attrs,
        get_dataset_archives,
        init_archive_purge_test
    ]}
].

all() -> [
    {group, all_tests}
].

-define(ATTEMPTS, 30).
-define(TEST_TIMESTAMP, 1111111111).
-define(NON_EXISTENT_ARCHIVE_ID, <<"NonExistentArchive">>).
-define(NON_EXISTENT_DATASET_ID, <<"NonExistentDataset">>).


-define(HTTP_SERVER_PORT, 8080).
-define(PURGED_ARCHIVE_PATH, "/archive_purged").
-define(CALLBACK_URL(), begin
    {ok, IpAddressBin} = ip_utils:to_binary(initializer:local_ip_v4()),
    str_utils:format_bin(<<"http://~s:~p~s">>, [IpAddressBin, ?HTTP_SERVER_PORT, ?PURGED_ARCHIVE_PATH])
end).

-define(TEST_PROCESS, test_process).
-define(ARCHIVE_PURGED(ArchiveId), {archive_purged, ArchiveId}).

%%%===================================================================
%%% Archive dataset test functions
%%%===================================================================

archive_dataset(_Config) ->
    Providers = [krakow, paris],

    #object{
        dataset = #dataset_object{id = DatasetId}
    } = onenv_file_test_utils:create_and_sync_file_tree(user3, space_krk_par, #file_spec{dataset = #dataset_spec{}}),

    #object{
        dataset = #dataset_object{id = DetachedDatasetId}
    } = onenv_file_test_utils:create_and_sync_file_tree(user3, space_krk_par, #file_spec{dataset = #dataset_spec{state = ?DETACHED_DATASET}}),

    MemRef = api_test_memory:init(),

    ?assert(onenv_api_test_runner:run_tests([
        #suite_spec{
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SPACE_KRK_PAR(?EPERM),
            randomly_select_scenarios = true,
            verify_fun = build_verify_archive_dataset_fun(MemRef, Providers),
            scenario_templates = [
                #scenario_template{
                    name = <<"Archive dataset using REST API">>,
                    type = rest,
                    prepare_args_fun = fun archive_dataset_prepare_rest_args_fun/1,
                    validate_result_fun = build_archive_dataset_validate_rest_call_result_fun(MemRef)
                },
                #scenario_template{
                    name = <<"Archive dataset using GS API">>,
                    type = gs,
                    prepare_args_fun = fun archive_datasets_prepare_gs_args_fun/1,
                    validate_result_fun = build_archive_dataset_validate_gs_call_result_fun(MemRef)
                }
            ],
            data_spec = #data_spec{
                required = [<<"datasetId">>, <<"type">>, <<"character">>, <<"dataStructure">>, <<"metadataStructure">>],
                optional = [<<"description">>],
                correct_values = #{
                    <<"datasetId">> => [DatasetId],
                    <<"type">> => ?ARCHIVE_TYPES,
                    <<"character">> => ?ARCHIVE_CHARACTERS,
                    <<"dataStructure">> => ?ARCHIVE_DATA_STRUCTURES,
                    <<"metadataStructure">> => ?ARCHIVE_METADATA_STRUCTURES,
                    <<"description">> => [<<"Test description">>]
                },
                bad_values = [
                    {<<"datasetId">>, ?NON_EXISTENT_DATASET_ID, ?ERROR_FORBIDDEN},
                    {<<"datasetId">>, DetachedDatasetId,
                        ?ERROR_BAD_DATA(<<"datasetId">>, <<"Detached dataset cannot be modified.">>)},
                    {<<"type">>, <<"not allowed type">>,
                        ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"type">>, ensure_binaries(?ARCHIVE_TYPES))},
                    {<<"character">>, <<"not allowed character">>,
                        ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"character">>, ensure_binaries(?ARCHIVE_CHARACTERS))},
                    {<<"dataStructure">>, <<"not allowed dataStructure">>,
                        ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"dataStructure">>, ensure_binaries(?ARCHIVE_DATA_STRUCTURES))},
                    {<<"metadataStructure">>, <<"not allowed metadataStructure">>,
                        ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"metadataStructure">>, ensure_binaries(?ARCHIVE_METADATA_STRUCTURES))},
                    {<<"description">>, [123, 456], ?ERROR_BAD_VALUE_BINARY(<<"description">>)}
                ]
            }
        }
    ])).


%% @private
-spec archive_dataset_prepare_rest_args_fun(onenv_api_test_runner:api_test_ctx()) ->
    onenv_api_test_runner:rest_args().
archive_dataset_prepare_rest_args_fun(#api_test_ctx{data = Data}) ->
    #rest_args{
        method = post,
        path = <<"archives">>,
        headers = #{<<"content-type">> => <<"application/json">>},
        body = json_utils:encode(Data)
    }.


%% @private
-spec archive_datasets_prepare_gs_args_fun(onenv_api_test_runner:api_test_ctx()) ->
    onenv_api_test_runner:rest_args().
archive_datasets_prepare_gs_args_fun(#api_test_ctx{data = Data}) ->
    #gs_args{
        operation = create,
        gri = #gri{type = op_archive, aspect = instance, scope = private},
        data = Data
    }.


%% @private
-spec build_archive_dataset_validate_rest_call_result_fun(api_test_memory:mem_ref()) ->
    onenv_api_test_runner:validate_call_result_fun().
build_archive_dataset_validate_rest_call_result_fun(MemRef) ->
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
-spec build_archive_dataset_validate_gs_call_result_fun(api_test_memory:mem_ref()) ->
    onenv_api_test_runner:validate_call_result_fun().
build_archive_dataset_validate_gs_call_result_fun(MemRef) ->
    fun(#api_test_ctx{node = TestNode, data = Data}, Result) ->
        CreationTime = time_test_utils:global_seconds(TestNode),
        DatasetId = maps:get(<<"datasetId">>, Data),

        {ok, #{<<"gri">> := ArchiveGri} = ArchiveData} = ?assertMatch({ok, _}, Result),

        #gri{id = ArchiveId} = ?assertMatch(
            #gri{type = op_archive, aspect = instance, scope = private},
            gri:deserialize(ArchiveGri)
        ),
        api_test_memory:set(MemRef, archive_id, ArchiveId),

        Params = archive_params:from_json(Data),
        Attrs = archive_attrs:from_json(Data),
        
        ExpArchiveData = build_archive_gs_instance(ArchiveId, DatasetId, CreationTime, Params, Attrs),
        ?assertEqual(ExpArchiveData, ArchiveData)
    end.


%% @private
-spec build_verify_archive_dataset_fun(
    api_test_memory:mem_ref(),
    [oct_background:entity_selector()]
) ->
    onenv_api_test_runner:verify_fun().
build_verify_archive_dataset_fun(MemRef, Providers) ->
    fun
        (expected_success, #api_test_ctx{
            node = TestNode,
            client = ?USER(UserId),
            data = Data
        }) ->
            ArchiveId = api_test_memory:get(MemRef, archive_id),

            CreationTime = time_test_utils:global_seconds(TestNode),
            DatasetId = maps:get(<<"datasetId">>, Data),
            Type = maps:get(<<"type">>, Data),
            Character = maps:get(<<"character">>, Data),
            DataStructure = maps:get(<<"dataStructure">>, Data),
            MetadataStructure = maps:get(<<"metadataStructure">>, Data),
            Description = maps:get(<<"description">>, Data, undefined),

            verify_archive(
                UserId, Providers, ArchiveId, DatasetId, CreationTime,
                Type, Character, DataStructure, MetadataStructure, Description
            );
        (expected_failure, _) ->
            ok
    end.

%%%===================================================================
%%% Get archive test functions
%%%===================================================================

get_archive_info(_Config) ->
    #object{dataset = #dataset_object{
        id = DatasetId,
        archives = [#archive_object{
            id = ArchiveId,
            params = ArchiveParams,
            attrs = ArchiveAttrs
        }]
    }} = onenv_file_test_utils:create_and_sync_file_tree(user3, space_krk_par,
        #file_spec{dataset = #dataset_spec{archives = 1}}
    ),

    ParamsJson = archive_params:to_json(ArchiveParams),
    AttrsJson = archive_attrs:to_json(ArchiveAttrs),
    ParamsAndAttrsJson = maps:merge(ParamsJson, AttrsJson),

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
                    validate_result_fun = fun(#api_test_ctx{node = TestNode}, {ok, RespCode, _, RespBody}) ->
                        CreationTime = time_test_utils:global_seconds(TestNode),
                        ExpArchiveData = ParamsAndAttrsJson#{
                            <<"archiveId">> => ArchiveId,
                            <<"datasetId">> => DatasetId,
                            <<"state">> => atom_to_binary(?EMPTY, utf8),
                            <<"rootDirectoryId">> => null,
                            <<"creationTime">> => CreationTime
                        },
                        ?assertEqual({?HTTP_200_OK, ExpArchiveData}, {RespCode, RespBody})
                    end
                },
                #scenario_template{
                    name = <<"Get archive using GS API">>,
                    type = gs,
                    prepare_args_fun = build_get_archive_prepare_gs_args_fun(ArchiveId),
                    validate_result_fun = fun(#api_test_ctx{node = TestNode}, {ok, Result}) ->
                        CreationTime = time_test_utils:global_seconds(TestNode),
                        ExpArchiveData = build_archive_gs_instance(ArchiveId, DatasetId, CreationTime, ArchiveParams, ArchiveAttrs),
                        ?assertEqual(ExpArchiveData, Result)
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

modify_archive_attrs(_Config) ->
    #object{dataset = #dataset_object{
        id = DatasetId,
        archives = ArchiveObjects
    }} = onenv_file_test_utils:create_and_sync_file_tree(user3, space_krk_par,
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
            verify_fun = build_verify_modified_archive_attrs_fun(MemRef, Providers),
            scenario_templates = [
                #scenario_template{
                    name = <<"Modify archive attrs using REST API">>,
                    type = rest,
                    prepare_args_fun = build_update_archive_prepare_rest_args_fun(MemRef),
                    validate_result_fun = fun(#api_test_ctx{}, {ok, RespCode, _, RespBody}) ->
                        ?assertEqual({?HTTP_204_NO_CONTENT, #{}}, {RespCode, RespBody})
                    end
                },
                #scenario_template{
                    name = <<"Modify archive attrs using GS API">>,
                    type = gs,
                    prepare_args_fun = build_update_archive_prepare_gs_args_fun(MemRef),
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
-spec build_update_archive_prepare_rest_args_fun(api_test_memory:mem_ref()) ->
    onenv_api_test_runner:prepare_args_fun().
build_update_archive_prepare_rest_args_fun(MemRef) ->
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
-spec build_update_archive_prepare_gs_args_fun(api_test_memory:mem_ref()) ->
    onenv_api_test_runner:prepare_args_fun().
build_update_archive_prepare_gs_args_fun(MemRef) ->
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
-spec build_verify_modified_archive_attrs_fun(
    api_test_memory:mem_ref(),
    [oct_background:entity_selector()]
) ->
    onenv_api_test_runner:verify_fun().
build_verify_modified_archive_attrs_fun(MemRef, Providers) ->
    fun(ExpResult, #api_test_ctx{data = Data}) ->
        case api_test_memory:get(MemRef, archive_to_modify) of
            #archive_object{id = ?NON_EXISTENT_ARCHIVE_ID} ->
                ok;
            #archive_object{id = ArchiveId, attrs = PrevAttrs} ->

                ExpCurrentAttrs = case ExpResult of
                    expected_failure ->
                        PrevAttrs;
                    expected_success ->
                        PassedDescription = maps:get(<<"description">>, Data, undefined),
                        PrevAttrs#archive_attrs{
                            description = utils:ensure_defined(PassedDescription, PrevAttrs#archive_attrs.description)
                        }
                end,

                lists:foreach(fun(Provider) ->
                    Node = ?OCT_RAND_OP_NODE(Provider),
                    UserSessId = oct_background:get_user_session_id(user3, Provider),
                    ?assertMatch({ok, #archive_info{attrs = ExpCurrentAttrs}},
                        lfm_proxy:get_archive_info(Node, UserSessId, ArchiveId), ?ATTEMPTS)
                end, Providers),
                api_test_memory:set(MemRef, archive_attrs, ExpCurrentAttrs)

        end
    end.


%%%===================================================================
%%% Get dataset archives test functions
%%%===================================================================

get_dataset_archives(_Config) ->

    #object{dataset = #dataset_object{
        id = DatasetId,
        archives = ArchiveObjects
    }} = onenv_file_test_utils:create_and_sync_file_tree(user3, space_krk_par, #file_spec{dataset = #dataset_spec{
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
    Limit = maps:get(<<"limit">>, Params, 100),
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
    }} = onenv_file_test_utils:create_and_sync_file_tree(user3, space_krk_par, #file_spec{dataset = #dataset_spec{
        archives = 20
    }}),

    MemRef = api_test_memory:init(),
    api_test_memory:set(MemRef, archive_objects, ArchiveObjects),

    maybe_detach_dataset(Providers, DatasetId),
    true = register(?TEST_PROCESS, self()),

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
                bad_values = [
                    {bad_id, ?NON_EXISTENT_ARCHIVE_ID, ?ERROR_NOT_FOUND},
                    {<<"callback">>, <<"htp:/wrong-url.org">>, ?ERROR_BAD_DATA(<<"callback">>)}
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

        Body = #{<<"callback">> => maps:get(<<"callback">>, Data0, ?CALLBACK_URL())},

        #rest_args{
            method = post,
            headers = #{<<"content-type">> => <<"application/json">>},
            path = <<"archives/", Id/binary, "/init_purge">>,
            body = json_utils:encode(Body)
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

        Data1 = maps:merge(#{<<"callback">> => ?CALLBACK_URL()}, Data0),

        #gs_args{
            operation = create,
            gri = #gri{type = op_archive, id = Id, aspect = purge, scope = private},
            data = Data1
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

    fun(ExpResult, _) ->
        case api_test_memory:get(MemRef, archive_to_purge) of
            #archive_object{id = ?NON_EXISTENT_ARCHIVE_ID} ->
                ok;
            undefined ->
                ct:fail("undefined archive to remove");
            #archive_object{id = ArchiveId} ->

                case ExpResult of
                    expected_success ->
                        Timeout = timer:seconds(?ATTEMPTS),
                        receive
                            ?ARCHIVE_PURGED(ArchiveId) -> ok
                        after
                            Timeout ->
                                ct:fail("Archive ~p not purged", [ArchiveId])
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

%%%===================================================================
%%% Common archive test utils
%%%===================================================================

%% @private
-spec verify_archive(
    od_user:id(), [oct_background:entity_selector()], archive:id(), dataset:id(),
    archive:timestamp(), archive:type(), archive:character(), archive:data_structure(), archive:metadata_structure(),
    archive:description()
) ->
    ok.
verify_archive(
    UserId, Providers, ArchiveId, DatasetId, CreationTime,
    Type, Character, DataStructure, MetadataStructure, Description
) ->
    lists:foreach(fun(Provider) ->
        Node = ?OCT_RAND_OP_NODE(Provider),
        UserSessId = oct_background:get_user_session_id(UserId, Provider),
        ListOpts = #{offset => 0, limit => 1000},
        GetDatasetsFun =  fun() -> list_archive_ids(Node, UserSessId, DatasetId, ListOpts) end,
        ?assertEqual(true, lists:member(ArchiveId, GetDatasetsFun()), ?ATTEMPTS),

        ExpArchiveInfo = #archive_info{
            id = ArchiveId,
            dataset_id = DatasetId,
            state = ?EMPTY,
            root_dir_guid = undefined,
            creation_time = CreationTime,
            params = #archive_params{
                type = Type,
                character = Character,
                data_structure = DataStructure,
                metadata_structure = MetadataStructure
            },
            attrs = #archive_attrs{description = Description},
            index = archives_list:index(ArchiveId, CreationTime)
        },
        ?assertEqual({ok, ExpArchiveInfo}, lfm_proxy:get_archive_info(Node, UserSessId, ArchiveId), ?ATTEMPTS)
    end, Providers).


%% @private
-spec list_archive_ids(node(), session:id(), dataset:id(), dataset_api:listing_opts()) ->
    [archive:id()].
list_archive_ids(Node, UserSessId, DatasetId, ListOpts) ->
    {ok, Datasets, _} = lfm_proxy:list_archives(Node, UserSessId, DatasetId, ListOpts),
    lists:map(fun({_, ArchiveId}) -> ArchiveId end, Datasets).


%% @private
-spec build_archive_gs_instance(archive:id(), dataset:id(), archive:timestamp(), archive:params(), 
    archive:attrs()) -> json_utils:json_term().
build_archive_gs_instance(ArchiveId, DatasetId, CreationTime, Params, Attrs) ->
    BasicInfo = archive_gui_gs_translator:translate_archive_info(#archive_info{
        id = ArchiveId,
        dataset_id = DatasetId,
        state = ?EMPTY,
        root_dir_guid = undefined,
        creation_time = CreationTime,
        params = Params,
        attrs = Attrs,
        index = archives_list:index(ArchiveId, CreationTime)
    }),
    BasicInfo#{<<"revision">> => 1}.


-spec take_random_archive(api_test_memory:mem_ref()) -> onenv_archive_test_utils:archive_object().
take_random_archive(MemRef) ->
    case lists_utils:shuffle(api_test_memory:get(MemRef, archive_objects)) of
        [ArchiveObject | RestArchiveIds] ->
            api_test_memory:set(MemRef, archive_objects, RestArchiveIds),
            ArchiveObject;
        [] ->
            ct:fail("List of created archives is empty")
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
    time_test_utils:set_current_time_seconds(?TEST_TIMESTAMP),
    lfm_proxy:init(Config, false).

end_per_group(_Group, Config) ->
    onenv_dataset_test_utils:cleanup_all_datasets(space_krk_par),
    lfm_proxy:teardown(Config),
    time_test_utils:unfreeze_time(Config).

init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 10}),
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


do(#mod{method = "POST", request_uri = ?PURGED_ARCHIVE_PATH, entity_body = Body}) ->
    #{<<"archiveId">> := ArchiveId} = json_utils:decode(Body),
    ?TEST_PROCESS ! ?ARCHIVE_PURGED(ArchiveId),
    done.


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