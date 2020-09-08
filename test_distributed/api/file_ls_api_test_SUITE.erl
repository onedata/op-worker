%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests concerning file ls API (REST + gs).
%%% @end
%%%-------------------------------------------------------------------
-module(file_ls_api_test_SUITE).
-author("Bartosz Walkowicz").

-include("file_metadata_api_test_utils.hrl").
-include("modules/fslogic/file_details.hrl").
-include_lib("ctool/include/graph_sync/gri.hrl").
-include_lib("ctool/include/http/codes.hrl").

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
    get_dir_children_on_provider_not_supporting_space_test/1
]).

all() -> [
    get_dir_children_test,
    get_shared_dir_children_test,
    get_file_children_test,
    get_user_root_dir_children_test,
    get_dir_children_on_provider_not_supporting_space_test
].

-type files() :: [{
    FileGuid :: file_id:file_guid(),
    FileName :: file_meta:name(),
    FilePath :: file_meta:path(),
    FileDetails :: #file_details{}
}].


-define(ATTEMPTS, 30).


%%%===================================================================
%%% List/Get children test functions
%%%===================================================================


get_dir_children_test(Config) ->
    {DirPath, DirGuid, _ShareId, Files} = create_get_children_tests_env(Config, normal_mode),

    {ok, DirObjectId} = file_id:guid_to_objectid(DirGuid),

    ?assert(onenv_api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = ?config(op_worker_nodes, Config),
            client_spec = ?CLIENT_SPEC_FOR_SPACE_2,
            scenario_templates = [
                #scenario_template{
                    name = <<"List normal dir using /data/ rest endpoint">>,
                    type = rest,
                    prepare_args_fun = build_get_children_prepare_new_id_rest_args_fun(DirObjectId),
                    validate_result_fun = fun(#api_test_ctx{data = Data}, {ok, ?HTTP_200_OK, _, Response}) ->
                        validate_listed_files(Response, rest, undefined, Data, Files)
                    end
                },
                #scenario_template{
                    name = <<"List normal dir using deprecated /files/ rest endpoint">>,
                    type = rest_with_file_path,
                    prepare_args_fun = build_get_children_prepare_deprecated_path_rest_args_fun(DirPath),
                    validate_result_fun = fun(#api_test_ctx{data = Data}, {ok, ?HTTP_200_OK, _, Response}) ->
                        validate_listed_files(Response, deprecated_rest, undefined, Data, Files)
                    end
                },
                #scenario_template{
                    name = <<"List normal dir using deprecated /files-id/ rest endpoint">>,
                    type = rest,
                    prepare_args_fun = build_get_children_prepare_deprecated_id_rest_args_fun(DirObjectId),
                    validate_result_fun = fun(#api_test_ctx{data = Data}, {ok, ?HTTP_200_OK, _, Response}) ->
                        validate_listed_files(Response, deprecated_rest, undefined, Data, Files)
                    end
                },
                #scenario_template{
                    name = <<"List normal dir using gs api">>,
                    type = gs,
                    prepare_args_fun = build_get_children_prepare_gs_args_fun(DirGuid, private),
                    validate_result_fun = fun(#api_test_ctx{data = Data}, {ok, Result}) ->
                        validate_listed_files(Result, gs, undefined, Data, Files)
                    end
                },
                #scenario_template{
                    name = <<"List normal dir children details using gs api">>,
                    type = gs,
                    prepare_args_fun = build_get_children_details_prepare_gs_args_fun(DirGuid, private),
                    validate_result_fun = fun(#api_test_ctx{data = Data}, {ok, Result}) ->
                        validate_listed_files(Result, gs_with_details, undefined, Data, Files)
                    end
                }
            ],
            randomly_select_scenarios = true,
            data_spec = api_test_utils:add_file_id_errors_for_operations_available_in_share_mode(
                DirGuid, undefined, get_children_data_spec()
            )
        }
    ])).


get_shared_dir_children_test(Config) ->
    {_DirPath, DirGuid, ShareId, Files} = create_get_children_tests_env(Config, share_mode),

    ShareDirGuid = file_id:guid_to_share_guid(DirGuid, ShareId),
    {ok, ShareDirObjectId} = file_id:guid_to_objectid(ShareDirGuid),

    ?assert(onenv_api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = ?config(op_worker_nodes, Config),
            client_spec = ?CLIENT_SPEC_FOR_SHARES,
            scenario_templates = [
                #scenario_template{
                    name = <<"List shared dir using /data/ rest endpoint">>,
                    type = rest,
                    prepare_args_fun = build_get_children_prepare_new_id_rest_args_fun(ShareDirObjectId),
                    validate_result_fun = fun(#api_test_ctx{data = Data}, {ok, ?HTTP_200_OK, _, Response}) ->
                        validate_listed_files(Response, rest, ShareId, Data, Files)
                    end
                },
                % Old endpoint returns "id" and "path" - for now get_path is forbidden
                % for shares so this method returns ?ERROR_NOT_SUPPORTED in case of
                % listing share dir
                #scenario_template{
                    name = <<"List shared dir using deprecated /files-id/ rest endpoint">>,
                    type = rest_not_supported,
                    prepare_args_fun = build_get_children_prepare_deprecated_id_rest_args_fun(ShareDirObjectId),
                    validate_result_fun = fun(_TestCaseCtx, {ok, ?HTTP_400_BAD_REQUEST, _, Response}) ->
                        ?assertEqual(?REST_ERROR(?ERROR_NOT_SUPPORTED), Response)
                    end
                },
                #scenario_template{
                    name = <<"List shared dir using gs api with public scope">>,
                    type = gs,
                    prepare_args_fun = build_get_children_prepare_gs_args_fun(ShareDirGuid, public),
                    validate_result_fun = fun(#api_test_ctx{data = Data}, {ok, Result}) ->
                        validate_listed_files(Result, gs, ShareId, Data, Files)
                    end
                },
                #scenario_template{
                    name = <<"List shared dir children details using gs api with public scope">>,
                    type = gs,
                    prepare_args_fun = build_get_children_details_prepare_gs_args_fun(ShareDirGuid, public),
                    validate_result_fun = fun(#api_test_ctx{data = Data}, {ok, Result}) ->
                        validate_listed_files(Result, gs_with_details, ShareId, Data, Files)
                    end
                },
                % 'private' scope is forbidden for shares even if user would be able to
                % list children using normal guid
                #scenario_template{
                    name = <<"List shared dir using gs api with private scope">>,
                    type = gs_with_shared_guid_and_aspect_private,
                    prepare_args_fun = build_get_children_prepare_gs_args_fun(ShareDirGuid, private),
                    validate_result_fun = fun(_TestCaseCtx, Result) ->
                        ?assertEqual(?ERROR_UNAUTHORIZED, Result)
                    end
                },
                #scenario_template{
                    name = <<"List shared dir children details using gs api with private scope">>,
                    type = gs_with_shared_guid_and_aspect_private,
                    prepare_args_fun = build_get_children_details_prepare_gs_args_fun(ShareDirGuid, private),
                    validate_result_fun = fun(_TestCaseCtx, Result) ->
                        ?assertEqual(?ERROR_UNAUTHORIZED, Result)
                    end
                }
            ],
            data_spec = api_test_utils:add_file_id_errors_for_operations_available_in_share_mode(
                DirGuid, ShareId, get_children_data_spec()
            )
        }
    ])).


%% @private
-spec create_get_children_tests_env(api_test_runner:config(), TestMode :: normal_mode | share_mode) ->
    {
        DirPath :: file_meta:path(),
        DirGuid :: file_id:file_guid(),
        ShareId :: undefined | od_share:id(),
        Files :: files()
    }.
create_get_children_tests_env(Config, TestMode) ->
    [P1Node] = api_test_env:get_provider_nodes(p1, Config),

    UserSessIdP1 = api_test_env:get_user_session_id(user3, p1, Config),
    SpaceOwnerSessIdP1 = api_test_env:get_user_session_id(user2, p1, Config),

    DirName = ?RANDOM_FILE_NAME(),
    DirPath = filename:join(["/", ?SPACE_2, DirName]),
    {ok, DirGuid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, DirPath, 8#707),
    HasParentQos = api_test_utils:randomly_add_qos(P1Node, DirGuid, <<"key=value1">>, 2),

    ShareId = case TestMode of
        normal_mode ->
            undefined;
        share_mode ->
            {ok, ShId} = lfm_proxy:create_share(
                P1Node, SpaceOwnerSessIdP1, {guid, DirGuid}, <<"share">>
            ),
            ShId
    end,

    Files = lists_utils:pmap(fun(Num) ->
        {_FileType, FilePath, FileGuid, #file_details{
            file_attr = #file_attr{
                guid = FileGuid,
                name = FileName
            }
        } = FileDetails} = api_test_utils:create_file_in_space2_with_additional_metadata(
            DirPath, HasParentQos, <<"file_or_dir_", Num>>, Config
        ),
        {FileGuid, FileName, FilePath, FileDetails}
    end, [$0, $1, $2, $3, $4]),

    {DirPath, DirGuid, ShareId, Files}.


get_file_children_test(Config) ->
    {_FileType, FilePath, FileGuid, #file_details{
        file_attr = #file_attr{
            guid = FileGuid,
            name = FileName
        }
    } = FileDetails} = api_test_utils:create_file_in_space2_with_additional_metadata(
        <<"/", ?SPACE_2/binary>>, false, <<"file">>, ?RANDOM_FILE_NAME(), Config
    ),
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),

    % Listing file result in returning this file info only - index/limit parameters are ignored.
    ?assert(onenv_api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = ?config(op_worker_nodes, Config),
            client_spec = #client_spec{
                correct = [
                    user2,  % space owner - doesn't need any perms
                    user3,  % files owner (see fun create_file_in_space2_with_additional_metadata/1)
                    user4   % space member - any space member can see file stats (as long as he can
                            %                traverse to it) no matter perms set on this file
                ],
                unauthorized = [nobody],
                forbidden_not_in_space = [user1]
            },
            scenario_templates = [
                #scenario_template{
                    name = <<"List file using /data/ rest endpoint">>,
                    type = rest,
                    prepare_args_fun = build_get_children_prepare_new_id_rest_args_fun(FileObjectId),
                    validate_result_fun = fun(_TestCaseCtx, {ok, ?HTTP_200_OK, _, Response}) ->
                        ?assertEqual(#{<<"children">> => [#{
                            <<"id">> => FileObjectId,
                            <<"name">> => FileName
                        }]}, Response)
                    end
                },
                #scenario_template{
                    name = <<"List file using deprecated /files/ rest endpoint">>,
                    type = rest_with_file_path,
                    prepare_args_fun = build_get_children_prepare_deprecated_path_rest_args_fun(FilePath),
                    validate_result_fun = fun(_TestCaseCtx, {ok, ?HTTP_200_OK, _, Response}) ->
                        ?assertEqual(
                            [#{<<"id">> => FileObjectId, <<"path">> => FilePath}],
                            Response
                        )
                    end
                },
                #scenario_template{
                    name = <<"List file using deprecated /files-id/ rest endpoint">>,
                    type = rest,
                    prepare_args_fun = build_get_children_prepare_deprecated_id_rest_args_fun(FileObjectId),
                    validate_result_fun = fun(_TestCaseCtx, {ok, ?HTTP_200_OK, _, Response}) ->
                        ?assertEqual(
                            [#{<<"id">> => FileObjectId, <<"path">> => FilePath}],
                            Response
                        )
                    end
                },
                #scenario_template{
                    name = <<"List file using gs api">>,
                    type = gs,
                    prepare_args_fun = build_get_children_prepare_gs_args_fun(FileGuid, private),
                    validate_result_fun = fun(_TestCaseCtx, {ok, Result}) ->
                        ?assertEqual(#{<<"children">> => [FileGuid]}, Result)
                    end
                },
                #scenario_template{
                    name = <<"List file details using gs api">>,
                    type = gs,
                    prepare_args_fun = build_get_children_details_prepare_gs_args_fun(FileGuid, private),
                    validate_result_fun = fun(#api_test_ctx{data = _Data}, {ok, Result}) ->
                        ?assertEqual(#{
                            <<"children">> => [file_details_to_gs_json(undefined, FileDetails)]
                        }, Result)
                    end
                }
            ],
            randomly_select_scenarios = true,
            data_spec = api_test_utils:add_file_id_errors_for_operations_available_in_share_mode(
                FileGuid, undefined, get_children_data_spec()
            )
        }
    ])).


get_user_root_dir_children_test(Config) ->
    [P1Node] = api_test_env:get_provider_nodes(p1, Config),
    [P2Node] = api_test_env:get_provider_nodes(p2, Config),
    Providers = [P2Node, P1Node],

    % Space dir docs are not synchronized between providers but kept locally. Because of that
    % file attrs differs between responses from various providers and it is necessary to get attrs
    % corresponding to concrete provider.
    GetSpaceInfoFun = fun(SpacePlaceholder, Node) ->
        SpaceId = api_test_env:get_space_id(SpacePlaceholder, Config),
        SpaceName = atom_to_binary(SpacePlaceholder, utf8),
        SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
        {SpaceGuid, SpaceName, <<"/", SpaceName/binary>>, get_space_dir_details(
            Node, SpaceGuid, SpaceName
        )}
    end,
    GetAllSpacesInfoFun = fun(Node) ->
        lists:sort([GetSpaceInfoFun(space1, Node), GetSpaceInfoFun(space2, Node)])
    end,

    User1Id = api_test_env:get_user_id(user1, Config),
    User2Id = api_test_env:get_user_id(user2, Config),
    User4Id = api_test_env:get_user_id(user4, Config),

    User4RootDirGuid = fslogic_uuid:user_root_dir_guid(User4Id),
    {ok, User4RootDirObjectId} = file_id:guid_to_objectid(User4RootDirGuid),

    DataSpec = get_children_data_spec(),

    ?assert(onenv_api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = Providers,
            client_spec = #client_spec{
                correct = [user4],  % only specific user can list his root dir
                unauthorized = [nobody],
                forbidden_not_in_space = [user1, user2, user3]
            },
            scenario_templates = [
                #scenario_template{
                    name = <<"List user4 root dir using deprecated /data/ rest endpoint">>,
                    type = rest,
                    prepare_args_fun = build_get_children_prepare_new_id_rest_args_fun(User4RootDirObjectId),
                    validate_result_fun = fun(#api_test_ctx{node = Node, data = Data}, {ok, ?HTTP_200_OK, _, Response}) ->
                        validate_listed_files(Response, rest, undefined, Data, GetAllSpacesInfoFun(Node))
                    end
                },
                #scenario_template{
                    name = <<"List user4 root dir using deprecated /files-id/ rest endpoint">>,
                    type = rest,
                    prepare_args_fun = build_get_children_prepare_deprecated_id_rest_args_fun(User4RootDirObjectId),
                    validate_result_fun = fun(#api_test_ctx{node = Node, data = Data}, {ok, ?HTTP_200_OK, _, Response}) ->
                        validate_listed_files(Response, deprecated_rest, undefined, Data, GetAllSpacesInfoFun(Node))
                    end
                },
                #scenario_template{
                    name = <<"List user4 root dir using gs api">>,
                    type = gs,
                    prepare_args_fun = build_get_children_prepare_gs_args_fun(User4RootDirGuid, private),
                    validate_result_fun = fun(#api_test_ctx{node = Node, data = Data}, {ok, Result}) ->
                        validate_listed_files(Result, gs, undefined, Data, GetAllSpacesInfoFun(Node))
                    end
                },
                #scenario_template{
                    name = <<"List user4 root dir children details using gs api">>,
                    type = gs,
                    prepare_args_fun = build_get_children_details_prepare_gs_args_fun(User4RootDirGuid, private),
                    validate_result_fun = fun(#api_test_ctx{node = Node, data = Data}, {ok, Result}) ->
                        validate_listed_files(Result, gs_with_details, undefined, Data, GetAllSpacesInfoFun(Node))
                    end
                }
            ],
            randomly_select_scenarios = true,
            data_spec = DataSpec
        },
        % Special case - listing files using path '/' works for all users but
        % returns only their own spaces
        #scenario_spec{
            name = <<"List user root dir using /files/ rest endpoint">>,
            type = rest_with_file_path,
            target_nodes = Providers,
            client_spec = #client_spec{
                correct = [user1, user2, user3, user4],
                unauthorized = [nobody]
            },
            prepare_args_fun = build_get_children_prepare_deprecated_path_rest_args_fun(<<"/">>),
            validate_result_fun = fun(#api_test_ctx{
                node = TestNode,
                client = Client,
                data = Data
            }, {ok, ?HTTP_200_OK, _, Response}) ->
                ClientSpaces = case Client of
                    ?USER(User1Id) -> [GetSpaceInfoFun(space1, TestNode)];
                    ?USER(User2Id) -> [GetSpaceInfoFun(space2, TestNode)];
                    _ -> GetAllSpacesInfoFun(TestNode)
                end,
                validate_listed_files(Response, deprecated_rest, undefined, Data, ClientSpaces)
            end,
            data_spec = DataSpec
        }
    ])).


%% @private
-spec get_space_dir_details(node(), file_id:file_guid(), od_space:name()) -> #file_details{}.
get_space_dir_details(Node, SpaceDirGuid, SpaceName) ->
    {ok, SpaceAttrs} = api_test_utils:get_file_attrs(Node, SpaceDirGuid),

    #file_details{
        file_attr = SpaceAttrs#file_attr{name = SpaceName},
        index_startid = file_id:guid_to_space_id(SpaceDirGuid),
        active_permissions_type = posix,
        has_metadata = false,
        has_direct_qos = false,
        has_eff_qos = false
    }.


get_dir_children_on_provider_not_supporting_space_test(Config) ->
    P2Id = api_test_env:get_provider_id(p2, Config),
    [P2Node] = api_test_env:get_provider_nodes(p2, Config),

    Space1Id = api_test_env:get_space_id(space1, Config),
    Space1Guid = fslogic_uuid:spaceid_to_space_dir_guid(Space1Id),
    {ok, Space1ObjectId} = file_id:guid_to_objectid(Space1Guid),

    ValidateRestListedFilesOnProvidersNotSupportingSpaceFun = fun(_, {ok, RespCode, _, RespBody}) ->
        ?assertEqual(
            {?HTTP_400_BAD_REQUEST, ?REST_ERROR(?ERROR_SPACE_NOT_SUPPORTED_BY(P2Id))},
            {RespCode, RespBody}
        )
    end,
    ValidateGsListedFilesOnProvidersNotSupportingSpaceFun = fun(_, Response) ->
        ?assertEqual(?ERROR_SPACE_NOT_SUPPORTED_BY(P2Id), Response)
    end,

    ?assert(onenv_api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = [P2Node],
            client_spec = ?CLIENT_SPEC_FOR_SPACE_1,
            scenario_templates = [
                #scenario_template{
                    name = <<"List dir on provider not supporting space using /data/ rest endpoint">>,
                    type = rest,
                    prepare_args_fun = build_get_children_prepare_new_id_rest_args_fun(Space1ObjectId),
                    validate_result_fun = ValidateRestListedFilesOnProvidersNotSupportingSpaceFun
                },
                #scenario_template{
                    name = <<"List dir on provider not supporting space using /files/ rest endpoint">>,
                    type = rest_with_file_path,
                    prepare_args_fun = build_get_children_prepare_deprecated_path_rest_args_fun(<<"/", ?SPACE_1/binary>>),
                    validate_result_fun = ValidateRestListedFilesOnProvidersNotSupportingSpaceFun
                },
                #scenario_template{
                    name = <<"List dir on provider not supporting space using /files-id/ rest endpoint">>,
                    type = rest,
                    prepare_args_fun = build_get_children_prepare_deprecated_id_rest_args_fun(Space1ObjectId),
                    validate_result_fun = ValidateRestListedFilesOnProvidersNotSupportingSpaceFun
                },
                #scenario_template{
                    name = <<"List dir on provider not supporting space using gs api">>,
                    type = gs,
                    prepare_args_fun = build_get_children_prepare_gs_args_fun(Space1Guid, private),
                    validate_result_fun = ValidateGsListedFilesOnProvidersNotSupportingSpaceFun
                },
                #scenario_template{
                    name = <<"List dir children details on provider not supporting space using gs api">>,
                    type = gs,
                    prepare_args_fun = build_get_children_details_prepare_gs_args_fun(Space1Guid, private),
                    validate_result_fun = ValidateGsListedFilesOnProvidersNotSupportingSpaceFun
                }
            ],
            randomly_select_scenarios = true,
            data_spec = get_children_data_spec()
        }
    ])).


%% @private
-spec get_children_data_spec() -> onenv_api_test_runner:data_spec().
get_children_data_spec() ->
    #data_spec{
        optional = [<<"limit">>, <<"offset">>],
        correct_values = #{
            <<"limit">> => [1, 100],
            <<"offset">> => [1, 3, 10]
        },
        bad_values = [
            {<<"limit">>, true, ?ERROR_BAD_VALUE_INTEGER(<<"limit">>)},
            {<<"limit">>, -100, ?ERROR_BAD_VALUE_NOT_IN_RANGE(<<"limit">>, 1, 1000)},
            {<<"limit">>, 0, ?ERROR_BAD_VALUE_NOT_IN_RANGE(<<"limit">>, 1, 1000)},
            {<<"limit">>, 1001, ?ERROR_BAD_VALUE_NOT_IN_RANGE(<<"limit">>, 1, 1000)},
            {<<"offset">>, <<"abc">>, ?ERROR_BAD_VALUE_INTEGER(<<"offset">>)}
        ]
    }.


%% @private
-spec build_get_children_prepare_new_id_rest_args_fun(file_id:objectid()) ->
    onenv_api_test_runner:prepare_args_fun().
build_get_children_prepare_new_id_rest_args_fun(FileObjectId) ->
    build_get_children_prepare_rest_args_fun(new_id, FileObjectId).


%% @private
-spec build_get_children_prepare_deprecated_path_rest_args_fun(file_meta:path()) ->
    onenv_api_test_runner:prepare_args_fun().
build_get_children_prepare_deprecated_path_rest_args_fun(FilePath) ->
    build_get_children_prepare_rest_args_fun(deprecated_path, FilePath).


%% @private
-spec build_get_children_prepare_deprecated_id_rest_args_fun(file_id:objectid()) ->
    onenv_api_test_runner:prepare_args_fun().
build_get_children_prepare_deprecated_id_rest_args_fun(FileObjectId) ->
    build_get_children_prepare_rest_args_fun(deprecated_id, FileObjectId).


%% @private
-spec build_get_children_prepare_rest_args_fun(
    Endpoint :: new_id | deprecated_path | deprecated_id,
    ValidId :: file_id:objectid() | file_meta:path()
) ->
    onenv_api_test_runner:prepare_args_fun().
build_get_children_prepare_rest_args_fun(Endpoint, ValidId) ->
    fun(#api_test_ctx{data = Data0}) ->
        Data1 = utils:ensure_defined(Data0, #{}),
        {Id, Data2} = api_test_utils:maybe_substitute_bad_id(ValidId, Data1),

        RestPath = case Endpoint of
            new_id -> <<"data/", Id/binary, "/children">>;
            deprecated_path -> <<"files", Id/binary>>;
            deprecated_id -> <<"files-id/", Id/binary>>
        end,
        #rest_args{
            method = get,
            path = http_utils:append_url_parameters(
                RestPath,
                maps:with([<<"limit">>, <<"offset">>], Data2)
            )
        }
    end.


%% @private
-spec build_get_children_prepare_gs_args_fun(file_id:file_guid(), gri:scope()) ->
    onenv_api_test_runner:prepare_args_fun().
build_get_children_prepare_gs_args_fun(FileGuid, Scope) ->
    build_prepare_gs_args_fun(FileGuid, children, Scope).


%% @private
-spec build_get_children_details_prepare_gs_args_fun(file_id:file_guid(), gri:scope()) ->
    onenv_api_test_runner:prepare_args_fun().
build_get_children_details_prepare_gs_args_fun(FileGuid, Scope) ->
    build_prepare_gs_args_fun(FileGuid, children_details, Scope).


%% @private
-spec build_prepare_gs_args_fun(file_id:file_guid(), gri:aspect(), gri:scope()) ->
    onenv_api_test_runner:prepare_args_fun().
build_prepare_gs_args_fun(FileGuid, Aspect, Scope) ->
    fun(#api_test_ctx{data = Data0}) ->
        {GriId, Data1} = api_test_utils:maybe_substitute_bad_id(FileGuid, Data0),

        #gs_args{
            operation = get,
            gri = #gri{type = op_file, id = GriId, aspect = Aspect, scope = Scope},
            data = Data1
        }
    end.


%% @private
-spec validate_listed_files(
    ListedChildren :: term(),
    Format :: gs | rest | deprecated_rest | gs_with_details,
    ShareId :: undefined | od_share:id(),
    Params :: map(),
    AllFiles :: files()
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

    ExpFiles2 = lists:map(fun({Guid, Name, Path, Details}) ->
        {file_id:guid_to_share_guid(Guid, ShareId), Name, Path, Details}
    end, ExpFiles1),

    ExpFiles3 = case Format of
        gs ->
            #{<<"children">> => lists:map(fun({Guid, _Name, _Path, _Details}) ->
                Guid
            end, ExpFiles2)};

        rest ->
            #{<<"children">> => lists:map(fun({Guid, Name, _Path, _Details}) ->
                {ok, ObjectId} = file_id:guid_to_objectid(Guid),
                #{
                    <<"id">> => ObjectId,
                    <<"name">> => Name
                }
            end, ExpFiles2)};

        deprecated_rest ->
            lists:map(fun({Guid, _Name, Path, _Details}) ->
                {ok, ObjectId} = file_id:guid_to_objectid(Guid),
                #{
                    <<"id">> => ObjectId,
                    <<"path">> => Path
                }
            end, ExpFiles2);

        gs_with_details ->
            #{<<"children">> => lists:map(fun({Guid, _Name, _Path, Details}) ->
                file_details_to_gs_json(ShareId, Details)
            end, ExpFiles2)}
    end,

    ?assertEqual(ExpFiles3, ListedChildren).


%% @private
-spec file_details_to_gs_json(undefined | od_share:id(), #file_details{}) -> map().
file_details_to_gs_json(undefined, #file_details{
    file_attr = #file_attr{
        guid = FileGuid,
        parent_uuid = ParentGuid,
        name = FileName,
        type = Type,
        mode = Mode,
        size = Size,
        mtime = MTime,
        shares = Shares,
        owner_id = OwnerId,
        provider_id = ProviderId
    },
    index_startid = IndexStartId,
    active_permissions_type = ActivePermissionsType,
    has_metadata = HasMetadata,
    has_direct_qos = HasDirectQos,
    has_eff_qos = HasEffQos
}) ->
    {DisplayedType, DisplayedSize} = case Type of
        ?DIRECTORY_TYPE ->
            {<<"dir">>, null};
        _ ->
            {<<"file">>, Size}
    end,

    #{
        <<"hasMetadata">> => HasMetadata,
        <<"guid">> => FileGuid,
        <<"name">> => FileName,
        <<"index">> => IndexStartId,
        <<"posixPermissions">> => list_to_binary(string:right(integer_to_list(Mode, 8), 3, $0)),
        % For space dir gs returns null as parentId instead of user root dir
        % (gui doesn't know about user root dir)
        <<"parentId">> => case fslogic_uuid:is_space_dir_guid(FileGuid) of
            true -> null;
            false -> ParentGuid
        end,
        <<"mtime">> => MTime,
        <<"type">> => DisplayedType,
        <<"size">> => DisplayedSize,
        <<"shares">> => Shares,
        <<"activePermissionsType">> => atom_to_binary(ActivePermissionsType, utf8),
        <<"providerId">> => ProviderId,
        <<"ownerId">> => OwnerId,
        <<"hasDirectQos">> => HasDirectQos,
        <<"hasEffQos">> => HasEffQos
    };
file_details_to_gs_json(ShareId, #file_details{
    file_attr = #file_attr{
        guid = FileGuid,
        parent_uuid = ParentGuid,
        name = FileName,
        type = Type,
        mode = Mode,
        size = Size,
        mtime = MTime,
        shares = Shares
    },
    index_startid = IndexStartId,
    active_permissions_type = ActivePermissionsType,
    has_metadata = HasMetadata
}) ->
    {DisplayedType, DisplayedSize} = case Type of
        ?DIRECTORY_TYPE ->
            {<<"dir">>, null};
        _ ->
            {<<"file">>, Size}
    end,

    #{
        <<"hasMetadata">> => HasMetadata,
        <<"guid">> => file_id:guid_to_share_guid(FileGuid, ShareId),
        <<"name">> => FileName,
        <<"index">> => IndexStartId,
        <<"posixPermissions">> => list_to_binary(string:right(integer_to_list(Mode band 2#111, 8), 3, $0)),
        <<"parentId">> => file_id:guid_to_share_guid(ParentGuid, ShareId),
        <<"mtime">> => MTime,
        <<"type">> => DisplayedType,
        <<"size">> => DisplayedSize,
        <<"shares">> => case lists:member(ShareId, Shares) of
            true -> [ShareId];
            false -> []
        end,
        <<"activePermissionsType">> => atom_to_binary(ActivePermissionsType, utf8)
    }.


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    application:start(ssl),
    hackney:start(),
    api_test_env:init_per_suite(Config, #onenv_test_config{envs = [
        {op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}
    ]}).


end_per_suite(_Config) ->
    hackney:stop(),
    application:stop(ssl).


init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 10}),
    lfm_proxy:init(Config).


end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).
