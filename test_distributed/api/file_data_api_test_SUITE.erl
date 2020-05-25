%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests concerning file data basic API (REST + gs).
%%% @end
%%%-------------------------------------------------------------------
-module(file_data_api_test_SUITE).
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
    get_dir_children_test/1,
    get_shared_dir_children_test/1,
    get_file_children_test/1,
    get_user_root_dir_children_test/1,
    get_dir_children_on_provider_not_supporting_space_test/1,

    get_file_attrs_test/1,
    get_shared_file_attrs_test/1,
    get_attrs_on_provider_not_supporting_space_test/1,

    set_file_mode_test/1,
    set_shared_file_mode_test/1,
    set_mode_on_provider_not_supporting_space_test/1
]).

all() ->
    ?ALL([
        get_dir_children_test,
        get_shared_dir_children_test,
        get_file_children_test,
        get_user_root_dir_children_test,
        get_dir_children_on_provider_not_supporting_space_test,

        get_file_attrs_test,
        get_shared_file_attrs_test,
        get_attrs_on_provider_not_supporting_space_test,

        set_file_mode_test,
        set_shared_file_mode_test,
        set_mode_on_provider_not_supporting_space_test
    ]).


%%%===================================================================
%%% List/Get children test functions
%%%===================================================================


get_dir_children_test(Config) ->
    {DirPath, DirGuid, _ShareId, Files} = create_get_children_tests_env(Config, normal_mode),
    {ok, DirObjectId} = file_id:guid_to_objectid(DirGuid),

    ?assert(api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = ?config(op_worker_nodes, Config),
            client_spec = ?CLIENT_SPEC_FOR_SPACE_2_SCENARIOS(Config),
            scenario_templates = [
                #scenario_template{
                    name = <<"List normal dir using /data/ rest endpoint">>,
                    type = rest,
                    prepare_args_fun = create_prepare_new_id_get_children_rest_args_fun(DirObjectId),
                    validate_result_fun = fun(#api_test_ctx{data = Data}, {ok, ?HTTP_200_OK, Response}) ->
                        validate_listed_files(Response, rest, undefined, Data, Files)
                    end
                },
                #scenario_template{
                    name = <<"List normal dir using /files/ rest endpoint">>,
                    type = rest_with_file_path,
                    prepare_args_fun = create_prepare_deprecated_path_get_children_rest_args_fun(DirPath),
                    validate_result_fun = fun(#api_test_ctx{data = Data}, {ok, ?HTTP_200_OK, Response}) ->
                        validate_listed_files(Response, deprecated_rest, undefined, Data, Files)
                    end
                },
                #scenario_template{
                    name = <<"List normal dir using /files-id/ rest endpoint">>,
                    type = rest,
                    prepare_args_fun = create_prepare_deprecated_id_get_children_rest_args_fun(DirObjectId),
                    validate_result_fun = fun(#api_test_ctx{data = Data}, {ok, ?HTTP_200_OK, Response}) ->
                        validate_listed_files(Response, deprecated_rest, undefined, Data, Files)
                    end
                },
                #scenario_template{
                    name = <<"List normal dir using gs api">>,
                    type = gs,
                    prepare_args_fun = create_prepare_get_children_gs_args_fun(DirGuid, private),
                    validate_result_fun = fun(#api_test_ctx{data = Data}, {ok, Result}) ->
                        validate_listed_files(Result, gs, undefined, Data, Files)
                    end
                }
            ],
            data_spec = api_test_utils:add_bad_file_id_and_path_error_values(
                get_children_data_spec(), ?SPACE_2, undefined
            )
        }
    ])).


get_shared_dir_children_test(Config) ->
    {_DirPath, DirGuid, ShareId, Files} = create_get_children_tests_env(Config, share_mode),
    ShareDirGuid = file_id:guid_to_share_guid(DirGuid, ShareId),
    {ok, ShareDirObjectId} = file_id:guid_to_objectid(ShareDirGuid),

    ?assert(api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = ?config(op_worker_nodes, Config),
            client_spec = ?CLIENT_SPEC_FOR_SHARE_SCENARIOS(Config),
            scenario_templates = [
                #scenario_template{
                    name = <<"List shared dir using /data/ rest endpoint">>,
                    type = rest,
                    prepare_args_fun = create_prepare_new_id_get_children_rest_args_fun(ShareDirObjectId),
                    validate_result_fun = fun(#api_test_ctx{data = Data}, {ok, ?HTTP_200_OK, Response}) ->
                        validate_listed_files(Response, rest, ShareId, Data, Files)
                    end
                },
                % Old endpoint returns "id" and "path" - for now get_path is forbidden
                % for shares so this method returns ?ERROR_NOT_SUPPORTED in case of
                % listing share dir
                #scenario_template{
                    name = <<"List shared dir using /files-id/ rest endpoint">>,
                    type = rest_not_supported,
                    prepare_args_fun = create_prepare_deprecated_id_get_children_rest_args_fun(ShareDirObjectId),
                    validate_result_fun = fun(_TestCaseCtx, {ok, ?HTTP_400_BAD_REQUEST, Response}) ->
                        ?assertEqual(?REST_ERROR(?ERROR_NOT_SUPPORTED), Response)
                    end
                },
                #scenario_template{
                    name = <<"List shared dir using gs api with public scope">>,
                    type = gs,
                    prepare_args_fun = create_prepare_get_children_gs_args_fun(ShareDirGuid, public),
                    validate_result_fun = fun(#api_test_ctx{data = Data}, {ok, Result}) ->
                        validate_listed_files(Result, gs, ShareId, Data, Files)
                    end
                },
                % 'private' scope is forbidden for shares even if user would be able to
                % list children using normal guid
                #scenario_template{
                    name = <<"List shared dir using gs api with private scope">>,
                    type = gs_with_shared_guid_and_aspect_private,
                    prepare_args_fun = create_prepare_get_children_gs_args_fun(ShareDirGuid, private),
                    validate_result_fun = fun(_TestCaseCtx, Result) ->
                        ?assertEqual(?ERROR_UNAUTHORIZED, Result)
                    end
                }
            ],
            data_spec = api_test_utils:add_bad_file_id_and_path_error_values(
                get_children_data_spec(), ?SPACE_2, ShareId
            )
        }
    ])).


%% @private
create_get_children_tests_env(Config, TestMode) ->
    [P2, P1] = ?config(op_worker_nodes, Config),
    SessIdP1 = ?USER_IN_BOTH_SPACES_SESS_ID(P1, Config),
    SessIdP2 = ?USER_IN_BOTH_SPACES_SESS_ID(P2, Config),

    DirName = ?RANDOM_FILE_NAME(),
    DirPath = filename:join(["/", ?SPACE_2, DirName]),
    {ok, DirGuid} = lfm_proxy:mkdir(P2, SessIdP2, DirPath, 8#777),

    ShareId = case TestMode of
        normal_mode ->
            undefined;
        share_mode ->
            {ok, ShId} = lfm_proxy:create_share(P2, SessIdP2, {guid, DirGuid}, <<"share">>),
            ShId
    end,

    Files = [{FileGuid1, _FileName1, _FilePath1} | _] = lists:map(fun(Num) ->
        FileName = <<"file", Num>>,
        {ok, FileGuid} = lfm_proxy:create(P2, SessIdP2, DirGuid, FileName, 8#777),
        {FileGuid, FileName, filename:join([DirPath, FileName])}
    end, [$0, $1, $2, $3, $4]),

    api_test_utils:wait_for_file_sync(P1, SessIdP1, FileGuid1),

    {DirPath, DirGuid, ShareId, Files}.


get_file_children_test(Config) ->
    [P2, P1] = Providers = ?config(op_worker_nodes, Config),
    SessIdP1 = ?USER_IN_BOTH_SPACES_SESS_ID(P1, Config),
    SessIdP2 = ?USER_IN_BOTH_SPACES_SESS_ID(P2, Config),

    FileName = <<"get_file_children_test">>,
    FilePath = filename:join(["/", ?SPACE_2, FileName]),
    {ok, FileGuid} = lfm_proxy:create(P2, SessIdP2, FilePath, 8#777),
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),

    api_test_utils:wait_for_file_sync(P1, SessIdP1, FileGuid),

    % Listing file result in returning this file info only - index/limit parameters are ignored.
    ?assert(api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SPACE_2_SCENARIOS(Config),
            scenario_templates = [
                #scenario_template{
                    name = <<"List file using /data/ rest endpoint">>,
                    type = rest,
                    prepare_args_fun = create_prepare_new_id_get_children_rest_args_fun(FileObjectId),
                    validate_result_fun = fun(_TestCaseCtx, {ok, ?HTTP_200_OK, Response}) ->
                        ?assertEqual(#{<<"children">> => [#{
                            <<"id">> => FileObjectId,
                            <<"name">> => FileName
                        }]}, Response)
                    end
                },
                #scenario_template{
                    name = <<"List file using /files/ rest endpoint">>,
                    type = rest_with_file_path,
                    prepare_args_fun = create_prepare_deprecated_path_get_children_rest_args_fun(FilePath),
                    validate_result_fun = fun(_TestCaseCtx, {ok, ?HTTP_200_OK, Response}) ->
                        ?assertEqual(
                            [#{<<"id">> => FileObjectId, <<"path">> => FilePath}],
                            Response
                        )
                    end
                },
                #scenario_template{
                    name = <<"List file using /files-id/ rest endpoint">>,
                    type = rest,
                    prepare_args_fun = create_prepare_deprecated_id_get_children_rest_args_fun(FileObjectId),
                    validate_result_fun = fun(_TestCaseCtx, {ok, ?HTTP_200_OK, Response}) ->
                        ?assertEqual(
                            [#{<<"id">> => FileObjectId, <<"path">> => FilePath}],
                            Response
                        )
                    end
                },
                #scenario_template{
                    name = <<"List file using gs api">>,
                    type = gs,
                    prepare_args_fun = create_prepare_get_children_gs_args_fun(FileGuid, private),
                    validate_result_fun = fun(_TestCaseCtx, {ok, Result}) ->
                        ?assertEqual(#{<<"children">> => [FileGuid]}, Result)
                    end
                }
            ],
            data_spec = api_test_utils:add_bad_file_id_and_path_error_values(
                get_children_data_spec(), ?SPACE_2, undefined
            )
        }
    ])).


get_user_root_dir_children_test(Config) ->
    Providers = ?config(op_worker_nodes, Config),

    Space1Guid = fslogic_uuid:spaceid_to_space_dir_guid(?SPACE_1),
    Space2Guid = fslogic_uuid:spaceid_to_space_dir_guid(?SPACE_2),

    Spaces = [
        {Space1Guid, ?SPACE_1, <<"/", ?SPACE_1/binary>>},
        {Space2Guid, ?SPACE_2, <<"/", ?SPACE_2/binary>>}
    ],
    UserInBothSpacesRootDirGuid = fslogic_uuid:user_root_dir_guid(?USER_IN_BOTH_SPACES),
    {ok, UserInBothSpacesRootDirObjectId} = file_id:guid_to_objectid(UserInBothSpacesRootDirGuid),

    DataSpec = get_children_data_spec(),

    ?assert(api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = Providers,
            client_spec = #client_spec{
                correct = [?USER_IN_BOTH_SPACES_AUTH],
                unauthorized = [?NOBODY],
                forbidden = [?USER_IN_SPACE_1_AUTH, ?USER_IN_SPACE_2_AUTH],
                supported_clients_per_node = ?SUPPORTED_CLIENTS_PER_NODE(Config)
            },
            scenario_templates = [
                #scenario_template{
                    name = <<"List user root dir using /data/ rest endpoint">>,
                    type = rest,
                    prepare_args_fun = create_prepare_new_id_get_children_rest_args_fun(UserInBothSpacesRootDirObjectId),
                    validate_result_fun = fun(#api_test_ctx{data = Data}, {ok, ?HTTP_200_OK, Response}) ->
                        validate_listed_files(Response, rest, undefined, Data, Spaces)
                    end
                },
                #scenario_template{
                    name = <<"List user root dir using /files-id/ rest endpoint">>,
                    type = rest,
                    prepare_args_fun = create_prepare_deprecated_id_get_children_rest_args_fun(UserInBothSpacesRootDirObjectId),
                    validate_result_fun = fun(#api_test_ctx{data = Data}, {ok, ?HTTP_200_OK, Response}) ->
                        validate_listed_files(Response, deprecated_rest, undefined, Data, Spaces)
                    end
                },
                #scenario_template{
                    name = <<"List user root dir using gs api">>,
                    type = gs,
                    prepare_args_fun = create_prepare_get_children_gs_args_fun(UserInBothSpacesRootDirGuid, private),
                    validate_result_fun = fun(#api_test_ctx{data = Data}, {ok, Result}) ->
                        validate_listed_files(Result, gs, undefined, Data, Spaces)
                    end
                }
            ],
            data_spec = DataSpec
        },
        % Special case - listing files using path '/' works for all users but
        % returns only their own spaces
        #scenario_spec{
            name = <<"List user root dir using /files/ rest endpoint">>,
            type = rest_with_file_path,
            target_nodes = Providers,
            client_spec = #client_spec{
                correct = [?USER_IN_SPACE_1_AUTH, ?USER_IN_SPACE_2_AUTH, ?USER_IN_BOTH_SPACES_AUTH],
                unauthorized = [?NOBODY],
                forbidden = [],
                supported_clients_per_node = ?SUPPORTED_CLIENTS_PER_NODE(Config)
            },
            prepare_args_fun = create_prepare_deprecated_path_get_children_rest_args_fun(<<"/">>),
            validate_result_fun = fun(#api_test_ctx{client = Client, data = Data}, {ok, ?HTTP_200_OK, Response}) ->
                ClientSpaces = case Client of
                    ?USER_IN_SPACE_1_AUTH ->
                        [{Space1Guid, ?SPACE_1, <<"/", ?SPACE_1/binary>>}];
                    ?USER_IN_SPACE_2_AUTH ->
                        [{Space2Guid, ?SPACE_2, <<"/", ?SPACE_2/binary>>}];
                    ?USER_IN_BOTH_SPACES_AUTH ->
                        Spaces
                end,
                validate_listed_files(Response, deprecated_rest, undefined, Data, ClientSpaces)
            end,
            data_spec = DataSpec
        }
    ])).


get_dir_children_on_provider_not_supporting_space_test(Config) ->
    [P2, _P1] = Providers = ?config(op_worker_nodes, Config),
    Provider2DomainBin = ?GET_DOMAIN_BIN(P2),

    Space1Guid = fslogic_uuid:spaceid_to_space_dir_guid(?SPACE_1),
    {ok, Space1ObjectId} = file_id:guid_to_objectid(Space1Guid),

    ValidateRestListedFilesOnProvidersNotSupportingSpace = fun(ExpSuccessResult) -> fun
        (#api_test_ctx{node = Node, client = Client}, {ok, ?HTTP_400_BAD_REQUEST, Response}) when
            Node == P2,
            Client == ?USER_IN_BOTH_SPACES_AUTH
        ->
            ?assertEqual(?REST_ERROR(?ERROR_SPACE_NOT_SUPPORTED_BY(Provider2DomainBin)), Response);
        (_TestCaseCtx, {ok, ?HTTP_200_OK, Response}) ->
            ?assertEqual(ExpSuccessResult, Response)
    end end,

    ?assert(api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SPACE_1_SCENARIOS(Config),
            scenario_templates = [
                #scenario_template{
                    name = <<"List dir on provider not supporting user using /data/ rest endpoint">>,
                    type = rest,
                    prepare_args_fun = create_prepare_new_id_get_children_rest_args_fun(Space1ObjectId),
                    validate_result_fun = ValidateRestListedFilesOnProvidersNotSupportingSpace(#{<<"children">> => []})
                },
                #scenario_template{
                    name = <<"List dir on provider not supporting user using /files/ rest endpoint">>,
                    type = rest_with_file_path,
                    prepare_args_fun = create_prepare_deprecated_path_get_children_rest_args_fun(<<"/", ?SPACE_1/binary>>),
                    validate_result_fun = ValidateRestListedFilesOnProvidersNotSupportingSpace([])
                },
                #scenario_template{
                    name = <<"List dir on provider not supporting user using /files-id/ rest endpoint">>,
                    type = rest,
                    prepare_args_fun = create_prepare_deprecated_id_get_children_rest_args_fun(Space1ObjectId),
                    validate_result_fun = ValidateRestListedFilesOnProvidersNotSupportingSpace([])
                },
                #scenario_template{
                    name = <<"List dir on provider not supporting user using gs api">>,
                    type = gs,
                    prepare_args_fun = create_prepare_get_children_gs_args_fun(Space1Guid, private),
                    validate_result_fun = fun
                        (#api_test_ctx{node = Node, client = Client}, Result) when
                            Node == P2,
                            Client == ?USER_IN_BOTH_SPACES_AUTH
                            ->
                            ?assertEqual(?ERROR_SPACE_NOT_SUPPORTED_BY(Provider2DomainBin), Result);
                        (_TestCaseCtx, {ok, Result}) ->
                            ?assertEqual(#{<<"children">> => []}, Result)
                    end
                }
            ],
            data_spec = get_children_data_spec()
        }
    ])).


%% @private
create_prepare_new_id_get_children_rest_args_fun(FileObjectId) ->
    create_prepare_get_children_rest_args_fun(new_id, FileObjectId).


%% @private
create_prepare_deprecated_path_get_children_rest_args_fun(FilePath) ->
    create_prepare_get_children_rest_args_fun(deprecated_path, FilePath).


%% @private
create_prepare_deprecated_id_get_children_rest_args_fun(FileObjectId) ->
    create_prepare_get_children_rest_args_fun(deprecated_id, FileObjectId).


%% @private
create_prepare_get_children_rest_args_fun(Endpoint, ValidId) ->
    fun(#api_test_ctx{data = Data0}) ->
        Data1 = utils:ensure_defined(Data0, undefined, #{}),

        Id = case maps:find(bad_id, Data1) of
            {ok, BadId} -> BadId;
            error -> ValidId
        end,
        RestPath = case Endpoint of
            new_id -> <<"data/", Id/binary, "/children">>;
            deprecated_path -> <<"files", Id/binary>>;
            deprecated_id -> <<"files-id/", Id/binary>>
        end,
        #rest_args{
            method = get,
            path = http_utils:append_url_parameters(
                RestPath,
                maps:with([<<"limit">>, <<"offset">>], Data1)
            )
        }
    end.


%% @private
create_prepare_get_children_gs_args_fun(FileGuid, Scope) ->
    fun(#api_test_ctx{data = Data0}) ->
        {GriId, Data1} = case maps:take(bad_id, utils:ensure_defined(Data0, undefined, #{})) of
            {BadId, Data2} when map_size(Data2) == 0 -> {BadId, undefined};
            {BadId, Data2} -> {BadId, Data2};
            error -> {FileGuid, Data0}
        end,
        #gs_args{
            operation = get,
            gri = #gri{type = op_file, id = GriId, aspect = children, scope = Scope},
            data = Data1
        }
    end.


%% @private
-spec validate_listed_files(
    ListedChildren :: term(),
    Format :: gs | rest | deprecated_rest,
    ShareId :: undefined | od_share:id(),
    Params :: map(),
    AllFiles :: [{file_id:file_guid(), Name :: binary(), Path :: binary()}]
) ->
    ok | no_return().
validate_listed_files(ListedChildren, Format, ShareId, Params, AllFiles) ->
    Limit = maps:get(<<"limit">>, Params, 1000),
    Offset = maps:get(<<"offset">>, Params, 0),

    ExpFiles1 = case Offset >= length(AllFiles) of
        true ->
            [];
        false ->
            lists:sublist(AllFiles, Offset+1, Limit)
    end,

    ExpFiles2 = lists:map(fun({Guid, Name, Path}) ->
        {file_id:guid_to_share_guid(Guid, ShareId), Name, Path}
    end, ExpFiles1),

    ExpFiles3 = case Format of
        gs ->
            #{<<"children">> => lists:map(fun({Guid, _Name, _Path}) ->
                Guid
            end, ExpFiles2)};
        rest ->
            #{<<"children">> => lists:map(fun({Guid, Name, _Path}) ->
                {ok, ObjectId} = file_id:guid_to_objectid(Guid),
                #{
                    <<"id">> => ObjectId,
                    <<"name">> => Name
                }
            end, ExpFiles2)};
        deprecated_rest ->
            lists:map(fun({Guid, _Name, Path}) ->
                {ok, ObjectId} = file_id:guid_to_objectid(Guid),
                #{
                    <<"id">> => ObjectId,
                    <<"path">> => Path
                }
            end, ExpFiles2)
    end,

    ?assertEqual(ExpFiles3, ListedChildren).


%% @private
get_children_data_spec() ->
    #data_spec{
        optional = [<<"limit">>, <<"offset">>],
        correct_values = #{
            <<"limit">> => [1, 100],
            <<"offset">> => [1, 10]
        },
        bad_values = [
            {<<"limit">>, true, ?ERROR_BAD_VALUE_INTEGER(<<"limit">>)},
            {<<"limit">>, -100, ?ERROR_BAD_VALUE_NOT_IN_RANGE(<<"limit">>, 1, 1000)},
            {<<"limit">>, 0, ?ERROR_BAD_VALUE_NOT_IN_RANGE(<<"limit">>, 1, 1000)},
            {<<"limit">>, 1001, ?ERROR_BAD_VALUE_NOT_IN_RANGE(<<"limit">>, 1, 1000)},
            {<<"offset">>, <<"abc">>, ?ERROR_BAD_VALUE_INTEGER(<<"offset">>)}
        ]
    }.


%%%===================================================================
%%% Get attrs test functions
%%%===================================================================


get_file_attrs_test(Config) ->
    [P2, P1] = Providers =  ?config(op_worker_nodes, Config),
    SessIdP1 = ?USER_IN_BOTH_SPACES_SESS_ID(P1, Config),
    SessIdP2 = ?USER_IN_BOTH_SPACES_SESS_ID(P2, Config),

    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    FilePath = filename:join(["/", ?SPACE_2, ?RANDOM_FILE_NAME()]),
    {ok, FileGuid} = api_test_utils:create_file(FileType, P1, SessIdP1, FilePath),
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),

    {ok, FileAttrs} = ?assertMatch({ok, _}, lfm_proxy:stat(P2, SessIdP2, {guid, FileGuid}), ?ATTEMPTS),
    JsonAttrs = attrs_to_json(undefined, FileAttrs),

    ?assert(api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SPACE_2_SCENARIOS(Config),
            scenario_templates = [
                #scenario_template{
                    name = <<"Get attrs from ", FileType/binary, " using /data/ rest endpoint">>,
                    type = rest,
                    prepare_args_fun = create_prepare_new_id_get_attrs_rest_args_fun(FileObjectId),
                    validate_result_fun = create_validate_get_attrs_rest_call_fun(JsonAttrs, undefined)
                },
                #scenario_template{
                    name = <<"Get attrs from ", FileType/binary, " using /files/ rest endpoint">>,
                    type = rest_with_file_path,
                    prepare_args_fun = create_prepare_deprecated_path_get_attrs_rest_args_fun(FilePath),
                    validate_result_fun = create_validate_get_attrs_rest_call_fun(JsonAttrs, undefined)
                },
                #scenario_template{
                    name = <<"Get attrs from ", FileType/binary, " using /files-id/ rest endpoint">>,
                    type = rest,
                    prepare_args_fun = create_prepare_deprecated_id_get_attrs_rest_args_fun(FileObjectId),
                    validate_result_fun = create_validate_get_attrs_rest_call_fun(JsonAttrs, undefined)
                },
                #scenario_template{
                    name = <<"Get attrs from ", FileType/binary, " using gs api">>,
                    type = gs,
                    prepare_args_fun = create_prepare_get_attrs_gs_args_fun(FileGuid, private),
                    validate_result_fun = create_validate_get_attrs_gs_call_fun(JsonAttrs, undefined)
                }
            ],
            data_spec = api_test_utils:add_bad_file_id_and_path_error_values(
                get_attrs_data_spec(normal_mode), ?SPACE_2, undefined
            )
        }
    ])).


get_shared_file_attrs_test(Config) ->
    [P2, P1] = Providers = ?config(op_worker_nodes, Config),
    SessIdP1 = ?USER_IN_BOTH_SPACES_SESS_ID(P1, Config),
    SessIdP2 = ?USER_IN_BOTH_SPACES_SESS_ID(P2, Config),

    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    FilePath = filename:join(["/", ?SPACE_2, ?RANDOM_FILE_NAME()]),
    {ok, FileGuid} = api_test_utils:create_file(FileType, P2, SessIdP2, FilePath),

    {ok, ShareId1} = lfm_proxy:create_share(P2, SessIdP2, {guid, FileGuid}, <<"share1">>),
    {ok, ShareId2} = lfm_proxy:create_share(P2, SessIdP2, {guid, FileGuid}, <<"share2">>),

    ShareGuid = file_id:guid_to_share_guid(FileGuid, ShareId1),
    {ok, ShareObjectId} = file_id:guid_to_objectid(ShareGuid),

    {ok, FileAttrs} = ?assertMatch(
        {ok, #file_attr{shares = [ShareId2, ShareId1]}},
        lfm_proxy:stat(P1, SessIdP1, {guid, FileGuid}),
        ?ATTEMPTS
    ),
    JsonAttrs = attrs_to_json(ShareId1, FileAttrs),

    ?assert(api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SHARE_SCENARIOS(Config),
            scenario_templates = [
                #scenario_template{
                    name = <<"Get attrs from shared ", FileType/binary, " using /data/ rest endpoint">>,
                    type = rest,
                    prepare_args_fun = create_prepare_new_id_get_attrs_rest_args_fun(ShareObjectId),
                    validate_result_fun = create_validate_get_attrs_rest_call_fun(JsonAttrs, ShareId1)
                },
                #scenario_template{
                    name = <<"Get attrs from shared ", FileType/binary, " using /files-id/ rest endpoint">>,
                    type = rest,
                    prepare_args_fun = create_prepare_deprecated_id_get_attrs_rest_args_fun(ShareObjectId),
                    validate_result_fun = create_validate_get_attrs_rest_call_fun(JsonAttrs, ShareId1)
                },
                #scenario_template{
                    name = <<"Get attrs from shared ", FileType/binary, " using gs public api">>,
                    type = gs,
                    prepare_args_fun = create_prepare_get_attrs_gs_args_fun(ShareGuid, public),
                    validate_result_fun = create_validate_get_attrs_gs_call_fun(JsonAttrs, ShareId1)
                }
            ],
            data_spec = api_test_utils:add_bad_file_id_and_path_error_values(
                get_attrs_data_spec(share_mode), ?SPACE_2, ShareId1
            )
        },
        #scenario_spec{
            name = <<"Get attrs from shared ", FileType/binary, " using private gs api">>,
            type = gs_with_shared_guid_and_aspect_private,
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SHARE_SCENARIOS(Config),
            prepare_args_fun = create_prepare_get_attrs_gs_args_fun(ShareGuid, private),
            validate_result_fun = fun(_, Result) ->
                ?assertEqual(?ERROR_UNAUTHORIZED, Result)
            end,
            data_spec = api_test_utils:add_bad_file_id_and_path_error_values(
                get_attrs_data_spec(normal_mode), ?SPACE_2, ShareId1
            )
        }
    ])).


get_attrs_on_provider_not_supporting_space_test(Config) ->
    [P2, P1] = Providers = ?config(op_worker_nodes, Config),
    SessIdP1 = ?USER_IN_BOTH_SPACES_SESS_ID(P1, Config),

    Space1Guid = fslogic_uuid:spaceid_to_space_dir_guid(?SPACE_1),
    {ok, Space1ObjectId} = file_id:guid_to_objectid(Space1Guid),
    {ok, Attrs} = lfm_proxy:stat(P1, SessIdP1, {guid, Space1Guid}),
    JsonAttrs = attrs_to_json(undefined, Attrs),

    ?assert(api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SPACE_1_SCENARIOS(Config),
            scenario_templates = [
                #scenario_template{
                    name = <<"Get attrs from ", ?SPACE_1/binary, " on provider not supporting user using /data/ rest endpoint">>,
                    type = rest,
                    prepare_args_fun = create_prepare_new_id_get_attrs_rest_args_fun(Space1ObjectId),
                    validate_result_fun = create_validate_get_attrs_rest_call_fun(JsonAttrs, undefined, P2)
                },
                #scenario_template{
                    name = <<"Get attrs from ", ?SPACE_1/binary, " on provider not supporting user using /files/ rest endpoint">>,
                    type = rest_with_file_path,
                    prepare_args_fun = create_prepare_deprecated_path_get_attrs_rest_args_fun(<<"/", ?SPACE_1/binary>>),
                    validate_result_fun = create_validate_get_attrs_rest_call_fun(JsonAttrs, undefined, P2)
                },
                #scenario_template{
                    name = <<"Get attrs from ", ?SPACE_1/binary, " on provider not supporting user using /files-id/ rest endpoint">>,
                    type = rest,
                    prepare_args_fun = create_prepare_deprecated_id_get_attrs_rest_args_fun(Space1ObjectId),
                    validate_result_fun = create_validate_get_attrs_rest_call_fun(JsonAttrs, undefined, P2)
                },
                #scenario_template{
                    name = <<"Get attrs from ", ?SPACE_1/binary, " on provider not supporting user using gs api">>,
                    type = gs,
                    prepare_args_fun = create_prepare_get_attrs_gs_args_fun(Space1Guid, private),
                    validate_result_fun = create_validate_get_attrs_gs_call_fun(JsonAttrs, undefined, P2)
                }
            ],
            data_spec = get_attrs_data_spec(normal_mode)
        }
    ])).


%% @private
-spec attrs_to_json(od_share:id(), #file_attr{}) -> map().
attrs_to_json(ShareId, #file_attr{
    guid = Guid,
    name = Name,
    mode = Mode,
    uid = Uid,
    gid = Gid,
    atime = ATime,
    mtime = MTime,
    ctime = CTime,
    type = Type,
    size = Size,
    shares = Shares,
    provider_id = ProviderId,
    owner_id = OwnerId
}) ->
    PublicAttrs = #{
        <<"name">> => Name,
        <<"atime">> => ATime,
        <<"mtime">> => MTime,
        <<"ctime">> => CTime,
        <<"type">> => case Type of
            ?REGULAR_FILE_TYPE -> <<"reg">>;
            ?DIRECTORY_TYPE -> <<"dir">>;
            ?SYMLINK_TYPE -> <<"lnk">>
        end,
        <<"size">> => Size
    },

    case ShareId of
        undefined ->
            {ok, ObjectId} = file_id:guid_to_objectid(Guid),

            PublicAttrs#{
                <<"file_id">> => ObjectId,
                <<"mode">> => <<"0", (integer_to_binary(Mode, 8))/binary>>,
                <<"storage_user_id">> => Uid,
                <<"storage_group_id">> => Gid,
                <<"shares">> => Shares,
                <<"provider_id">> => ProviderId,
                <<"owner_id">> => OwnerId
            };
        _ ->
            ShareGuid = file_id:guid_to_share_guid(Guid, ShareId),
            {ok, ShareObjectId} = file_id:guid_to_objectid(ShareGuid),

            PublicAttrs#{
                <<"file_id">> => ShareObjectId,
                <<"mode">> => <<"0", (integer_to_binary(2#111 band Mode, 8))/binary>>,
                <<"storage_user_id">> => ?SHARE_UID,
                <<"storage_group_id">> => ?SHARE_GID,
                <<"shares">> => case lists:member(ShareId, Shares) of
                    true -> [ShareId];
                    false -> []
                end,
                <<"provider_id">> => <<"unknown">>,
                <<"owner_id">> => <<"unknown">>
            }
    end.


%% @private
create_prepare_new_id_get_attrs_rest_args_fun(FileObjectId) ->
    create_prepare_get_attrs_rest_args_fun(new_id, FileObjectId).


%% @private
create_prepare_deprecated_path_get_attrs_rest_args_fun(FilePath) ->
    create_prepare_get_attrs_rest_args_fun(deprecated_path, FilePath).


%% @private
create_prepare_deprecated_id_get_attrs_rest_args_fun(FileObjectId) ->
    create_prepare_get_attrs_rest_args_fun(deprecated_id, FileObjectId).


%% @private
create_prepare_get_attrs_rest_args_fun(Endpoint, ValidId) ->
    fun(#api_test_ctx{data = Data0}) ->
        Data1 = utils:ensure_defined(Data0, undefined, #{}),

        Id = case maps:find(bad_id, Data1) of
            {ok, BadId} -> BadId;
            error -> ValidId
        end,
        RestPath = case Endpoint of
            new_id -> <<"data/", Id/binary>>;
            deprecated_path -> <<"metadata/attrs", Id/binary>>;
            deprecated_id -> <<"metadata-id/attrs/", Id/binary>>
        end,
        #rest_args{
            method = get,
            path = http_utils:append_url_parameters(
                RestPath,
                maps:with([<<"attribute">>], Data1)
            )
        }
    end.


%% @private
create_prepare_get_attrs_gs_args_fun(FileGuid, Scope) ->
    fun(#api_test_ctx{data = Data0}) ->
        {GriId, Data1} = case maps:take(bad_id, utils:ensure_defined(Data0, undefined, #{})) of
            {BadId, Data2} when map_size(Data2) == 0 -> {BadId, undefined};
            {BadId, Data2} -> {BadId, Data2};
            error -> {FileGuid, Data0}
        end,
        #gs_args{
            operation = get,
            gri = #gri{type = op_file, id = GriId, aspect = attrs, scope = Scope},
            data = Data1
        }
    end.


%% @private
create_validate_get_attrs_rest_call_fun(JsonAttrs, ShareId) ->
    create_validate_get_attrs_rest_call_fun(JsonAttrs, ShareId, undefined).


%% @private
create_validate_get_attrs_rest_call_fun(JsonAttrs, ShareId, ProviderNotSupportingSpace) ->
    fun
        (#api_test_ctx{node = TestNode}, {ok, RespCode, RespBody}) when TestNode == ProviderNotSupportingSpace ->
            ProviderDomain = ?GET_DOMAIN_BIN(ProviderNotSupportingSpace),
            ExpError = ?REST_ERROR(?ERROR_SPACE_NOT_SUPPORTED_BY(ProviderDomain)),
            ?assertEqual({?HTTP_400_BAD_REQUEST, ExpError}, {RespCode, RespBody});
        (TestCtx, {ok, RespCode, RespBody}) ->
            case get_attrs_exp_result(TestCtx, JsonAttrs, ShareId) of
                {ok, ExpAttrs} ->
                    ?assertEqual({?HTTP_200_OK, ExpAttrs}, {RespCode, RespBody});
                {error, _} = Error ->
                    ExpRestError = {errors:to_http_code(Error), ?REST_ERROR(Error)},
                    ?assertEqual(ExpRestError, {RespCode, RespBody})
            end
    end.


%% @private
create_validate_get_attrs_gs_call_fun(JsonAttrs, ShareId) ->
    create_validate_get_attrs_gs_call_fun(JsonAttrs, ShareId, undefined).


%% @private
create_validate_get_attrs_gs_call_fun(JsonAttrs, ShareId, ProviderNotSupportingSpace) ->
    fun
        (#api_test_ctx{node = TestNode}, Result) when TestNode == ProviderNotSupportingSpace ->
            ProviderDomain = ?GET_DOMAIN_BIN(ProviderNotSupportingSpace),
            ExpError = ?ERROR_SPACE_NOT_SUPPORTED_BY(ProviderDomain),
            ?assertEqual(ExpError, Result);
        (TestCtx, Result) ->
            case get_attrs_exp_result(TestCtx, JsonAttrs, ShareId) of
                {ok, ExpAttrs} ->
                    ?assertEqual({ok, #{<<"attributes">> => ExpAttrs}}, Result);
                {error, _} = ExpError ->
                    ?assertEqual(ExpError, Result)
            end
    end.


get_attrs_exp_result(#api_test_ctx{data = Data}, JsonAttrs, ShareId) ->
    RequestedAttributes = case maps:get(<<"attribute">>, Data, undefined) of
        undefined ->
            case ShareId of
                undefined -> ?PRIVATE_BASIC_ATTRIBUTES;
                _ -> ?PUBLIC_BASIC_ATTRIBUTES
            end;
        Attr ->
            [Attr]
    end,
    {ok, maps:with(RequestedAttributes, JsonAttrs)}.


get_attrs_data_spec(normal_mode) ->
    #data_spec{
        optional = [<<"attribute">>],
        correct_values = #{<<"attribute">> => ?PRIVATE_BASIC_ATTRIBUTES},
        bad_values = [
            {<<"attribute">>, true, ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"attribute">>, ?PRIVATE_BASIC_ATTRIBUTES)},
            {<<"attribute">>, 10, {gs, ?ERROR_BAD_VALUE_BINARY(<<"attribute">>)}},
            {<<"attribute">>, <<"NaN">>, ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"attribute">>, ?PRIVATE_BASIC_ATTRIBUTES)}
        ]
    };
get_attrs_data_spec(share_mode) ->
    #data_spec{
        optional = [<<"attribute">>],
        correct_values = #{<<"attribute">> => ?PUBLIC_BASIC_ATTRIBUTES},
        bad_values = [
            {<<"attribute">>, true, ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"attribute">>, ?PUBLIC_BASIC_ATTRIBUTES)},
            {<<"attribute">>, 10, {gs, ?ERROR_BAD_VALUE_BINARY(<<"attribute">>)}},
            {<<"attribute">>, <<"NaN">>, ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"attribute">>, ?PUBLIC_BASIC_ATTRIBUTES)},
            {<<"attribute">>, <<"owner_id">>, ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"attribute">>, ?PUBLIC_BASIC_ATTRIBUTES)}
        ]
    }.


%%%===================================================================
%%% Set mode test functions
%%%===================================================================


set_file_mode_test(Config) ->
    [P2, P1] = Providers = ?config(op_worker_nodes, Config),
    SessIdP1 = ?USER_IN_BOTH_SPACES_SESS_ID(P1, Config),
    SessIdP2 = ?USER_IN_BOTH_SPACES_SESS_ID(P2, Config),

    GetSessionFun = fun(Node) ->
        ?config({session_id, {?USER_IN_BOTH_SPACES, ?GET_DOMAIN(Node)}}, Config)
    end,

    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    FilePath = filename:join(["/", ?SPACE_2, ?RANDOM_FILE_NAME()]),
    {ok, FileGuid} = api_test_utils:create_file(FileType, P1, SessIdP1, FilePath, 8#777),
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),

    api_test_utils:wait_for_file_sync(P2, SessIdP2, FileGuid),

    GetExpectedResultFun = fun
        (#api_test_ctx{client = ?USER_IN_BOTH_SPACES_AUTH}) -> ok;
        (_) -> ?ERROR_POSIX(?EACCES)
    end,
    ValidateRestSuccessfulCallFun =  fun(TestCtx, {ok, RespCode, RespBody}) ->
        {ExpCode, ExpBody} = case GetExpectedResultFun(TestCtx) of
            ok ->
                {?HTTP_204_NO_CONTENT, #{}};
            {error, _} = ExpError ->
                {errors:to_http_code(ExpError), ?REST_ERROR(ExpError)}
        end,
        ?assertEqual({ExpCode, ExpBody}, {RespCode, RespBody})
    end,
    GetMode = fun(Node) ->
        {ok, #file_attr{mode = Mode}} = ?assertMatch(
            {ok, _},
            lfm_proxy:stat(Node, GetSessionFun(Node), {guid, FileGuid})
        ),
        Mode
    end,

    ?assert(api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SPACE_2_SCENARIOS(Config),
            verify_fun = fun
                (expected_failure, #api_test_ctx{node = TestNode}) ->
                    ?assertMatch(8#777, GetMode(TestNode), ?ATTEMPTS),
                    true;
                (expected_success, #api_test_ctx{client = ?USER_IN_SPACE_2_AUTH, node = TestNode}) ->
                    ?assertMatch(8#777, GetMode(TestNode), ?ATTEMPTS),
                    true;
                (expected_success, #api_test_ctx{client = ?USER_IN_BOTH_SPACES_AUTH, data = #{<<"mode">> := ModeBin}}) ->
                    Mode = binary_to_integer(ModeBin, 8),
                    lists:foreach(fun(Node) -> ?assertMatch(Mode, GetMode(Node), ?ATTEMPTS) end, Providers),
                    true
            end,
            scenario_templates = [
                #scenario_template{
                    name = <<"Set mode for ", FileType/binary, " using /data/ rest endpoint">>,
                    type = rest,
                    prepare_args_fun = create_prepare_new_id_set_mode_rest_args_fun(FileObjectId),
                    validate_result_fun = ValidateRestSuccessfulCallFun
                },
                #scenario_template{
                    name = <<"Set mode for ", FileType/binary, " using /files/ rest endpoint">>,
                    type = rest_with_file_path,
                    prepare_args_fun = create_prepare_deprecated_path_set_mode_rest_args_fun(FilePath),
                    validate_result_fun = ValidateRestSuccessfulCallFun
                },
                #scenario_template{
                    name = <<"Set mode for ", FileType/binary, " using /files-id/ rest endpoint">>,
                    type = rest,
                    prepare_args_fun = create_prepare_deprecated_id_set_mode_rest_args_fun(FileObjectId),
                    validate_result_fun = ValidateRestSuccessfulCallFun
                },
                #scenario_template{
                    name = <<"Set mode for ", FileType/binary, " using gs api">>,
                    type = gs,
                    prepare_args_fun = create_prepare_set_mode_gs_args_fun(FileGuid, private),
                    validate_result_fun = fun(TestCtx, Result) ->
                        case GetExpectedResultFun(TestCtx) of
                            ok ->
                                ?assertEqual({ok, undefined}, Result);
                            {error, _} = ExpError ->
                                ?assertEqual(ExpError, Result)
                        end
                    end
                }
            ],
            data_spec = api_test_utils:add_bad_file_id_and_path_error_values(
                set_mode_data_spec(), ?SPACE_2, undefined
            )
        }
    ])).


set_shared_file_mode_test(Config) ->
    [P2, P1] = Providers = ?config(op_worker_nodes, Config),
    SessIdP1 = ?USER_IN_BOTH_SPACES_SESS_ID(P1, Config),
    SessIdP2 = ?USER_IN_BOTH_SPACES_SESS_ID(P2, Config),

    GetSessionFun = fun(Node) ->
        ?config({session_id, {?USER_IN_BOTH_SPACES, ?GET_DOMAIN(Node)}}, Config)
    end,

    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    FilePath = filename:join(["/", ?SPACE_2, ?RANDOM_FILE_NAME()]),
    {ok, FileGuid} = api_test_utils:create_file(FileType, P1, SessIdP1, FilePath, 8#777),

    {ok, ShareId} = lfm_proxy:create_share(P1, SessIdP1, {guid, FileGuid}, <<"share">>),
    ShareGuid = file_id:guid_to_share_guid(FileGuid, ShareId),
    {ok, ShareObjectId} = file_id:guid_to_objectid(ShareGuid),

    api_test_utils:wait_for_file_sync(P2, SessIdP2, FileGuid),

    ValidateRestOperationNotSupportedFun = fun(_, {ok, ?HTTP_400_BAD_REQUEST, Response}) ->
        ?assertEqual(?REST_ERROR(?ERROR_NOT_SUPPORTED), Response)
    end,
    GetMode = fun(Node) ->
        {ok, #file_attr{mode = Mode}} = ?assertMatch(
            {ok, _},
            lfm_proxy:stat(Node, GetSessionFun(Node), {guid, FileGuid})
        ),
        Mode
    end,

    ?assert(api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SHARE_SCENARIOS(Config),
            verify_fun = fun(_, #api_test_ctx{node = Node}) ->
                ?assertMatch(8#777, GetMode(Node)),
                true
            end,
            scenario_templates = [
                #scenario_template{
                    name = <<"Set mode for shared ", FileType/binary, " using /data/ rest endpoint">>,
                    type = rest_not_supported,
                    prepare_args_fun = create_prepare_new_id_set_mode_rest_args_fun(ShareObjectId),
                    validate_result_fun = ValidateRestOperationNotSupportedFun
                },
                #scenario_template{
                    name = <<"Set mode for shared ", FileType/binary, " using /files-id/ rest endpoint">>,
                    type = rest_not_supported,
                    prepare_args_fun = create_prepare_deprecated_id_set_mode_rest_args_fun(ShareObjectId),
                    validate_result_fun = ValidateRestOperationNotSupportedFun
                },
                #scenario_template{
                    name = <<"Set mode for shared ", FileType/binary, " using gs public api">>,
                    type = gs_not_supported,
                    prepare_args_fun = create_prepare_set_mode_gs_args_fun(ShareGuid, public),
                    validate_result_fun = fun(_TestCaseCtx, Result) ->
                        ?assertEqual(?ERROR_NOT_SUPPORTED, Result)
                    end
                },
                #scenario_template{
                    name = <<"Set mode for shared ", FileType/binary, " using private gs api">>,
                    type = gs_with_shared_guid_and_aspect_private,
                    prepare_args_fun = create_prepare_set_mode_gs_args_fun(ShareGuid, private),
                    validate_result_fun = fun(_, Result) ->
                        ?assertEqual(?ERROR_UNAUTHORIZED, Result)
                    end
                }
            ],
            data_spec = api_test_utils:add_bad_file_id_and_path_error_values(
                set_mode_data_spec(), ?SPACE_2, ShareId
            )
        }
    ])).


set_mode_on_provider_not_supporting_space_test(Config) ->
    [P2, P1] = ?config(op_worker_nodes, Config),

    GetSessionFun = fun(Node) ->
        ?config({session_id, {?USER_IN_BOTH_SPACES, ?GET_DOMAIN(Node)}}, Config)
    end,

    Space1RootDirPath = filename:join(["/", ?SPACE_1, ?SCENARIO_NAME]),
    {ok, Space1RootDirGuid} = lfm_proxy:mkdir(P1, GetSessionFun(P1), Space1RootDirPath, 8#777),
    {ok, Space1RootObjectId} = file_id:guid_to_objectid(Space1RootDirGuid),

    GetMode = fun(Node) ->
        {ok, #file_attr{mode = Mode}} = ?assertMatch(
            {ok, _},
            lfm_proxy:stat(Node, GetSessionFun(Node), {guid, Space1RootDirGuid})
        ),
        Mode
    end,

    Provider2DomainBin = ?GET_DOMAIN_BIN(P2),

    ValidateRestSetMetadataOnProvidersNotSupportingUserFun = fun(_TestCtx, {ok, RespCode, RespBody}) ->
        ExpCode = ?HTTP_400_BAD_REQUEST,
        ExpBody = ?REST_ERROR(?ERROR_SPACE_NOT_SUPPORTED_BY(Provider2DomainBin)),
        ?assertEqual({ExpCode, ExpBody}, {RespCode, RespBody})
    end,

    ?assert(api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = [P2],
            client_spec = ?CLIENT_SPEC_FOR_SPACE_1_SCENARIOS(Config),
            verify_fun = fun(_, #api_test_ctx{node = Node}) ->
                ?assertMatch(8#777, GetMode(Node), ?ATTEMPTS),
                true
            end,
            scenario_templates = [
                #scenario_template{
                    name = <<"Set mode for root dir in ", ?SPACE_1/binary, " on provider not supporting user using /data/ rest endpoint">>,
                    type = rest,
                    prepare_args_fun = create_prepare_new_id_set_mode_rest_args_fun(Space1RootObjectId),
                    validate_result_fun = ValidateRestSetMetadataOnProvidersNotSupportingUserFun
                },
                #scenario_template{
                    name = <<"Set mode for root dir in ", ?SPACE_1/binary, " on provider not supporting user using /files/ rest endpoint">>,
                    type = rest_with_file_path,
                    prepare_args_fun = create_prepare_deprecated_path_set_mode_rest_args_fun(Space1RootDirPath),
                    validate_result_fun = ValidateRestSetMetadataOnProvidersNotSupportingUserFun
                },
                #scenario_template{
                    name = <<"Set mode for root dir in ", ?SPACE_1/binary, " on provider not supporting user using /files-id/ rest endpoint">>,
                    type = rest,
                    prepare_args_fun = create_prepare_deprecated_id_set_mode_rest_args_fun(Space1RootObjectId),
                    validate_result_fun = ValidateRestSetMetadataOnProvidersNotSupportingUserFun
                },
                #scenario_template{
                    name = <<"Set mode for root dir in ", ?SPACE_1/binary, " on provider not supporting user using gs api">>,
                    type = gs,
                    prepare_args_fun = create_prepare_set_mode_gs_args_fun(Space1RootDirGuid, private),
                    validate_result_fun = fun(_TestCtx, Result) ->
                        ?assertEqual(?ERROR_SPACE_NOT_SUPPORTED_BY(Provider2DomainBin), Result)
                    end
                }
            ],
            data_spec = set_mode_data_spec()
        }
    ])).


%% @private
create_prepare_new_id_set_mode_rest_args_fun(FileObjectId) ->
    create_prepare_set_mode_rest_args_fun(new_id, FileObjectId).


%% @private
create_prepare_deprecated_path_set_mode_rest_args_fun(FilePath) ->
    create_prepare_set_mode_rest_args_fun(deprecated_path, FilePath).


%% @private
create_prepare_deprecated_id_set_mode_rest_args_fun(FileObjectId) ->
    create_prepare_set_mode_rest_args_fun(deprecated_id, FileObjectId).


%% @private
create_prepare_set_mode_rest_args_fun(Endpoint, ValidId) ->
    fun(#api_test_ctx{data = Data0}) ->
        {Id, Data1} = case maps:take(bad_id, Data0) of
            {BadId, Data2} -> {BadId, Data2};
            error -> {ValidId, Data0}
        end,
        RestPath = case Endpoint of
            new_id -> <<"data/", Id/binary>>;
            deprecated_path -> <<"metadata/attrs", Id/binary>>;
            deprecated_id -> <<"metadata-id/attrs/", Id/binary>>
        end,
        #rest_args{
            method = put,
            path = http_utils:append_url_parameters(
                RestPath,
                maps:with([<<"attribute">>], Data1)
            ),
            headers = #{<<"content-type">> => <<"application/json">>},
            body = json_utils:encode(Data1)
        }
    end.


%% @private
create_prepare_set_mode_gs_args_fun(FileGuid, Scope) ->
    fun(#api_test_ctx{data = Data0}) ->
        {GriId, Data1} = case maps:take(bad_id, Data0) of
            {BadId, Data2} -> {BadId, Data2};
            error -> {FileGuid, Data0}
        end,
        #gs_args{
            operation = create,
            gri = #gri{type = op_file, id = GriId, aspect = attrs, scope = Scope},
            data = Data1
        }
    end.


set_mode_data_spec() ->
    #data_spec{
        required = [<<"mode">>],
        correct_values = #{<<"mode">> => [<<"0000">>, <<"0111">>, <<"0777">>]},
        bad_values = [
            {<<"mode">>, true, ?ERROR_BAD_VALUE_INTEGER(<<"mode">>)},
            {<<"mode">>, <<"integer">>, ?ERROR_BAD_VALUE_INTEGER(<<"mode">>)}
        ]
    }.


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
