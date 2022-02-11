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
-module(api_file_ls_test_SUITE).
-author("Bartosz Walkowicz").

-include("api_file_test_utils.hrl").
-include("modules/dataset/dataset.hrl").
-include("modules/fslogic/file_details.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/graph_sync/gri.hrl").
-include_lib("ctool/include/http/codes.hrl").

-export([
    groups/0, all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_group/2, end_per_group/2,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    get_dir_children_test/1,
    get_shared_dir_children_test/1,
    get_file_children_test/1,
    get_shared_file_children_test/1,
    get_user_root_dir_children_test/1,
    get_dir_children_on_provider_not_supporting_space_test/1
]).

groups() -> [
    {all_tests, [parallel], [
        get_dir_children_test,
        get_shared_dir_children_test,
        get_file_children_test,
        get_shared_file_children_test,
        get_user_root_dir_children_test,
        get_dir_children_on_provider_not_supporting_space_test
    ]}
].

all() -> [
    {group, all_tests}
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
    {_DirPath, DirGuid, _ShareId, Files} = create_get_children_tests_env(normal_mode),

    {ok, DirObjectId} = file_id:guid_to_objectid(DirGuid),

    ValidateGdPublicApiCallResultFun = fun(#api_test_ctx{client = Client}, Result) ->
        case Client of
            ?NOBODY -> ?assertEqual(?ERROR_UNAUTHORIZED, Result);
            _ -> ?assertEqual(?ERROR_FORBIDDEN, Result)
        end
    end,

    ?assert(onenv_api_test_runner:run_tests([
        #suite_spec{
            target_nodes = ?config(op_worker_nodes, Config),
            client_spec = ?CLIENT_SPEC_FOR_SPACE_KRK_PAR,
            scenario_templates = [
                #scenario_template{
                    name = <<"List normal dir children details using gs private api">>,
                    type = gs,
                    prepare_args_fun = build_get_children_details_prepare_gs_args_fun(DirGuid, private),
                    validate_result_fun = fun(#api_test_ctx{data = Data}, {ok, Result}) ->
                        validate_listed_files(Result, gs_with_details, undefined, Data, Files)
                    end
                }
            ],
            randomly_select_scenarios = true,
            data_spec = api_test_utils:add_file_id_errors_for_operations_available_in_share_mode(
                DirGuid, undefined, get_children_data_spec(gs, private)
            )
        },
        #suite_spec{
            target_nodes = ?config(op_worker_nodes, Config),
            client_spec = ?CLIENT_SPEC_FOR_SPACE_KRK_PAR,
            scenario_templates = [
                #scenario_template{
                    name = <<"List normal dir using /data/ rest endpoint">>,
                    type = rest,
                    prepare_args_fun = build_get_children_prepare_rest_args_fun(DirObjectId),
                    validate_result_fun = fun(#api_test_ctx{data = Data}, {ok, ?HTTP_200_OK, _, Response}) ->
                        validate_listed_files(Response, rest, undefined, Data, Files)
                    end
                }
            ],
            randomly_select_scenarios = true,
            data_spec = api_test_utils:add_file_id_errors_for_operations_available_in_share_mode(
                DirGuid, undefined, get_children_data_spec(rest, private)
            )
        },
        #suite_spec{
            target_nodes = ?config(op_worker_nodes, Config),
            client_spec = ?CLIENT_SPEC_FOR_SHARES,
            scenario_templates = [
                #scenario_template{
                    name = <<"List normal dir children details using gs public api">>,
                    type = gs,
                    prepare_args_fun = build_get_children_details_prepare_gs_args_fun(DirGuid, public),
                    validate_result_fun = ValidateGdPublicApiCallResultFun
                }
            ]
        }
    ])).


get_shared_dir_children_test(Config) ->
    {_DirPath, DirGuid, ShareId, Files} = create_get_children_tests_env(share_mode),

    ShareDirGuid = file_id:guid_to_share_guid(DirGuid, ShareId),
    {ok, ShareDirObjectId} = file_id:guid_to_objectid(ShareDirGuid),

    ?assert(onenv_api_test_runner:run_tests([
        #suite_spec{
            target_nodes = ?config(op_worker_nodes, Config),
            client_spec = ?CLIENT_SPEC_FOR_SHARES,
            scenario_templates = [
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
                    name = <<"List shared dir children details using gs api with private scope">>,
                    type = gs_with_shared_guid_and_aspect_private,
                    prepare_args_fun = build_get_children_details_prepare_gs_args_fun(ShareDirGuid, private),
                    validate_result_fun = fun(_TestCaseCtx, Result) ->
                        ?assertEqual(?ERROR_UNAUTHORIZED, Result)
                    end
                }
            ],
            data_spec = api_test_utils:add_file_id_errors_for_operations_available_in_share_mode(
                DirGuid, ShareId, get_children_data_spec(gs, public)
            )
        },
        #suite_spec{
            target_nodes = ?config(op_worker_nodes, Config),
            client_spec = ?CLIENT_SPEC_FOR_SHARES,
            scenario_templates = [
                #scenario_template{
                    name = <<"List shared dir using /data/ rest endpoint">>,
                    type = {rest_with_shared_guid, file_id:guid_to_space_id(DirGuid)},
                    prepare_args_fun = build_get_children_prepare_rest_args_fun(ShareDirObjectId),
                    validate_result_fun = fun(#api_test_ctx{data = Data}, {ok, ?HTTP_200_OK, _, Response}) ->
                        validate_listed_files(Response, rest, ShareId, Data, Files)
                    end
                }
            ],
            data_spec = api_test_utils:add_file_id_errors_for_operations_available_in_share_mode(
                DirGuid, ShareId, get_children_data_spec(rest, public)
            )
        }
    ])).


%% @private
-spec create_get_children_tests_env(TestMode :: normal_mode | share_mode) ->
    {
        DirPath :: file_meta:path(),
        DirGuid :: file_id:file_guid(),
        ShareId :: undefined | od_share:id(),
        Files :: files()
    }.
create_get_children_tests_env(TestMode) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),

    UserSessIdP1 = oct_background:get_user_session_id(user3, krakow),
    SpaceOwnerSessIdP1 = oct_background:get_user_session_id(user2, krakow),

    DirName = ?RANDOM_FILE_NAME(),
    DirPath = filename:join(["/", ?SPACE_KRK_PAR, DirName]),
    {ok, DirGuid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, DirPath, 8#707),
    HasParentQos = api_test_utils:randomly_add_qos([P1Node], DirGuid, <<"key=value1">>, 2),

    ShareId = case TestMode of
        normal_mode ->
            undefined;
        share_mode ->
            {ok, ShId} = lfm_proxy:create_share(
                P1Node, SpaceOwnerSessIdP1, ?FILE_REF(DirGuid), <<"share">>
            ),
            ShId
    end,

    Files = lists_utils:pmap(fun(Num) ->
        {_FileType, FilePath, FileGuid, #file_details{
            file_attr = #file_attr{
                guid = FileGuid,
                name = FileName
            }
        } = FileDetails} = api_test_utils:create_file_in_space_krk_par_with_additional_metadata(
            DirPath, HasParentQos, <<"file_or_dir_", Num>>
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
    } = FileDetails} = api_test_utils:create_file_in_space_krk_par_with_additional_metadata(
        <<"/", ?SPACE_KRK_PAR/binary>>, false, <<"file">>, ?RANDOM_FILE_NAME()
    ),
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),

    ValidateGdPublicApiCallResultFun = fun(#api_test_ctx{client = Client}, Result) ->
        case Client of
            ?NOBODY -> ?assertEqual(?ERROR_UNAUTHORIZED, Result);
            _ -> ?assertEqual(?ERROR_FORBIDDEN, Result)
        end
    end,

    % Listing file result in returning this file info only - index/limit parameters are ignored.
    ?assert(onenv_api_test_runner:run_tests([
        #suite_spec{
            target_nodes = ?config(op_worker_nodes, Config),
            client_spec = #client_spec{
                correct = [
                    user2,  % space owner - doesn't need any perms
                    user3,  % files owner (see fun create_file_in_space_krk_par_with_additional_metadata/1)
                    user4   % space member - any space member can see file stats (as long as he can
                    %                traverse to it) no matter perms set on this file
                ],
                unauthorized = [nobody],
                forbidden_not_in_space = [user1]
            },
            scenario_templates = [
                #scenario_template{
                    name = <<"List file details using gs private api">>,
                    type = gs,
                    prepare_args_fun = build_get_children_details_prepare_gs_args_fun(FileGuid, private),
                    validate_result_fun = fun(#api_test_ctx{data = _Data}, {ok, Result}) ->
                        ?assertEqual(#{
                            <<"children">> => [api_test_utils:file_details_to_gs_json(undefined, FileDetails)],
                            <<"isLast">> => true
                        }, Result)
                    end
                }
            ],
            randomly_select_scenarios = true,
            data_spec = api_test_utils:add_file_id_errors_for_operations_available_in_share_mode(
                FileGuid, undefined, get_children_data_spec(gs, private)
            )
        },
        #suite_spec{
            target_nodes = ?config(op_worker_nodes, Config),
            client_spec = #client_spec{
                correct = [
                    user2,  % space owner - doesn't need any perms
                    user3,  % files owner (see fun create_file_in_space_krk_par_with_additional_metadata/1)
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
                    prepare_args_fun = build_get_children_prepare_rest_args_fun(FileObjectId),
                    validate_result_fun = fun(#api_test_ctx{data = Data}, {ok, ?HTTP_200_OK, _, Response}) ->
                        validate_listed_files(Response, rest, undefined, Data, [{FileGuid, FileName, FilePath, FileDetails}])
                    end
                }
            ],
            randomly_select_scenarios = true,
            data_spec = api_test_utils:add_file_id_errors_for_operations_available_in_share_mode(
                FileGuid, undefined, get_children_data_spec(rest, private)
            )
        },
        #suite_spec{
            target_nodes = ?config(op_worker_nodes, Config),
            client_spec = ?CLIENT_SPEC_FOR_SHARES,
            scenario_templates = [
                #scenario_template{
                    name = <<"List file using gs public api">>,
                    type = gs,
                    prepare_args_fun = build_get_children_prepare_gs_args_fun(FileGuid, public),
                    validate_result_fun = ValidateGdPublicApiCallResultFun
                },
                #scenario_template{
                    name = <<"List file details using gs public api">>,
                    type = gs,
                    prepare_args_fun = build_get_children_details_prepare_gs_args_fun(FileGuid, public),
                    validate_result_fun = ValidateGdPublicApiCallResultFun
                }
            ]
        }
    ])).


get_shared_file_children_test(Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    Providers = [P1Node, P2Node],

    SpaceOwnerSessIdP1 = oct_background:get_user_session_id(user2, krakow),

    {_FileType, FilePath, FileGuid, #file_details{
        file_attr = FileAttrs = #file_attr{
            guid = FileGuid,
            name = FileName,
            shares = Shares
        }
    } = FileDetails0} = api_test_utils:create_file_in_space_krk_par_with_additional_metadata(
        <<"/", ?SPACE_KRK_PAR/binary>>, false, <<"file">>, ?RANDOM_FILE_NAME()
    ),

    ShareId = api_test_utils:share_file_and_sync_file_attrs(
        P1Node, SpaceOwnerSessIdP1, Providers, FileGuid
    ),
    ShareFileGuid = file_id:guid_to_share_guid(FileGuid, ShareId),
    {ok, ShareFileObjectId} = file_id:guid_to_objectid(ShareFileGuid),

    FileDetails1 = FileDetails0#file_details{
        file_attr = FileAttrs#file_attr{shares = [ShareId | Shares]}
    },

    % Listing file result in returning this file info only - index/limit parameters are ignored.
    ?assert(onenv_api_test_runner:run_tests([
        #suite_spec{
            target_nodes = ?config(op_worker_nodes, Config),
            client_spec = ?CLIENT_SPEC_FOR_SHARES,
            scenario_templates = [
                #scenario_template{
                    name = <<"List shared file using /data/ rest endpoint">>,
                    type = {rest_with_shared_guid, file_id:guid_to_space_id(FileGuid)},
                    prepare_args_fun = build_get_children_prepare_rest_args_fun(ShareFileObjectId),
                    validate_result_fun = fun(#api_test_ctx{data = Data}, {ok, ?HTTP_200_OK, _, Response}) ->
                        validate_listed_files(Response, rest, ShareId, Data, [{FileGuid, FileName, FilePath, FileDetails1}])
                    end
                }
            ],
            randomly_select_scenarios = true,
            data_spec = api_test_utils:add_file_id_errors_for_operations_available_in_share_mode(
                FileGuid, ShareId, get_children_data_spec(rest, public)
            )
        },
        #suite_spec{
            target_nodes = ?config(op_worker_nodes, Config),
            client_spec = ?CLIENT_SPEC_FOR_SHARES,
            scenario_templates = [
                #scenario_template{
                    name = <<"List shared file details using gs private api">>,
                    type = gs,
                    prepare_args_fun = build_get_children_details_prepare_gs_args_fun(ShareFileGuid, private),
                    validate_result_fun = fun(_TestCaseCtx, {ok, Result}) ->
                        ?assertEqual(#{
                            <<"children">> => [api_test_utils:file_details_to_gs_json(ShareId, FileDetails1)],
                            <<"isLast">> => true
                        }, Result)
                    end
                }
                % 'private' scope is forbidden for shares even if user would be able to
                % list children using normal guid
                #scenario_template{
                    name = <<"List shared file children details using gs api with private scope">>,
                    type = gs_with_shared_guid_and_aspect_private,
                    prepare_args_fun = build_get_children_details_prepare_gs_args_fun(ShareFileGuid, private),
                    validate_result_fun = fun(_TestCaseCtx, Result) ->
                        ?assertEqual(?ERROR_UNAUTHORIZED, Result)
                    end
                }
            ],
            randomly_select_scenarios = true,
            data_spec = api_test_utils:add_file_id_errors_for_operations_available_in_share_mode(
                FileGuid, ShareId, get_children_data_spec(gs, public)
            )
        }
    ])).


get_user_root_dir_children_test(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    Providers = [P2Node, P1Node],
    
    User4Id = oct_background:get_user_id(user4),
    User4RootDirGuid = fslogic_uuid:user_root_dir_guid(User4Id),
    {ok, User4RootDirObjectId} = file_id:guid_to_objectid(User4RootDirGuid),
    
    % Space dir docs are not synchronized between providers but kept locally. Because of that
    % file attrs differs between responses from various providers and it is necessary to get attrs
    % corresponding to specific provider.
    GetSpaceInfoFun = fun(SpacePlaceholder, Node) ->
        SpaceId = oct_background:get_space_id(SpacePlaceholder),
        SpaceName = atom_to_binary(SpacePlaceholder, utf8),
        SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
        {SpaceGuid, SpaceName, <<"/", SpaceName/binary>>, get_space_dir_details(
            Node, SpaceGuid, SpaceName, User4RootDirGuid
        )}
    end,
    GetAllSpacesInfoFun = fun(Node) ->
        [GetSpaceInfoFun(space_krk, Node), GetSpaceInfoFun(space_krk_par, Node)]
    end,

    ?assert(onenv_api_test_runner:run_tests([
        #suite_spec{
            target_nodes = Providers,
            client_spec = #client_spec{
                correct = [user4],  % only specific user can list his root dir
                unauthorized = [nobody],
                forbidden_not_in_space = [user1, user2, user3]
            },
            scenario_templates = [
                #scenario_template{
                    name = <<"List user4 root dir using /data/ rest endpoint">>,
                    type = rest,
                    prepare_args_fun = build_get_children_prepare_rest_args_fun(User4RootDirObjectId),
                    validate_result_fun = fun(#api_test_ctx{node = Node, data = Data}, {ok, ?HTTP_200_OK, _, Response}) ->
                        validate_listed_files(Response, rest, undefined, Data, GetAllSpacesInfoFun(Node))
                    end
                }
            ],
            randomly_select_scenarios = true,
            data_spec = get_children_data_spec(rest, private)
        },
        #suite_spec{
            target_nodes = Providers,
            client_spec = #client_spec{
                correct = [user4],  % only specific user can list his root dir
                unauthorized = [nobody],
                forbidden_not_in_space = [user1, user2, user3]
            },
            scenario_templates = [
                #scenario_template{
                    name = <<"List user4 root dir children details using gs api">>,
                    type = gs,
                    prepare_args_fun = build_get_children_details_prepare_gs_args_fun(User4RootDirGuid, private),
                    validate_result_fun = fun(_, Result) ->
                        % Listing children details for user root dir is not supported
                        ?assertEqual(?ERROR_POSIX(?ENOTSUP), Result)
                    end
                }
            ],
            randomly_select_scenarios = true,
            data_spec = get_children_data_spec(gs, private)
        }
    ])).


%% @private
-spec get_space_dir_details(node(), file_id:file_guid(), od_space:name(), file_id:file_guid()) -> 
    #file_details{}.
get_space_dir_details(Node, SpaceDirGuid, SpaceName, ParentGuid) ->
    {ok, SpaceAttrs} = ?assertMatch(
        {ok, _}, file_test_utils:get_attrs(Node, SpaceDirGuid), ?ATTEMPTS
    ),
    #file_details{
        file_attr = SpaceAttrs#file_attr{name = SpaceName, parent_guid = ParentGuid},
        index_startid = file_id:guid_to_space_id(SpaceDirGuid),
        active_permissions_type = posix,
        eff_protection_flags = ?no_flags_mask,
        eff_qos_membership = ?NONE_MEMBERSHIP,
        eff_dataset_membership = ?NONE_MEMBERSHIP,
        has_metadata = false
    }.


get_dir_children_on_provider_not_supporting_space_test(_Config) ->
    P2Id = oct_background:get_provider_id(paris),
    [P2Node] = oct_background:get_provider_nodes(paris),

    Space1Id = oct_background:get_space_id(space_krk),
    Space1Guid = fslogic_uuid:spaceid_to_space_dir_guid(Space1Id),
    {ok, Space1ObjectId} = file_id:guid_to_objectid(Space1Guid),

    ValidateRestListedFilesOnProvidersNotSupportingSpaceFun = fun(_, {ok, RespCode, _, RespBody}) ->
        ?assertEqual(
            {?HTTP_400_BAD_REQUEST, ?REST_ERROR(?ERROR_SPACE_NOT_SUPPORTED_BY(Space1Id, P2Id))},
            {RespCode, RespBody}
        )
    end,
    ValidateGsListedFilesOnProvidersNotSupportingSpaceFun = fun(_, Response) ->
        ?assertEqual(?ERROR_SPACE_NOT_SUPPORTED_BY(Space1Id, P2Id), Response)
    end,

    ?assert(onenv_api_test_runner:run_tests([
        #suite_spec{
            target_nodes = [P2Node],
            client_spec = ?CLIENT_SPEC_FOR_SPACE_KRK,
            scenario_templates = [
                #scenario_template{
                    name = <<"List dir on provider not supporting space using /data/ rest endpoint">>,
                    type = rest,
                    prepare_args_fun = build_get_children_prepare_rest_args_fun(Space1ObjectId),
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
            ]
        }
    ])).


%% @private
-spec get_children_data_spec(gs | rest, public | private) -> onenv_api_test_runner:data_spec().
get_children_data_spec(gs, _Scope) ->
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
    };
get_children_data_spec(rest, Scope) ->
    {AllowedAttrs, ScopeAttrsToCheck} = case Scope of
        public -> {?PUBLIC_BASIC_ATTRIBUTES, []};
        private -> {?PRIVATE_BASIC_ATTRIBUTES, [<<"hardlinks_count">>]}
    end,
    #data_spec{
        optional = [<<"limit">>, <<"attribute">>],
        correct_values = #{
            <<"limit">> => [1, 100],
            <<"attribute">> => [
                lists_utils:random_sublist(AllowedAttrs), 
                [<<"shares">>, <<"mode">>, <<"parent_id">>],
                [<<"file_id">>, <<"name">>],
                <<"ctime">>
            ] ++ ScopeAttrsToCheck
        },
        bad_values = [
            {<<"limit">>, true, ?ERROR_BAD_VALUE_INTEGER(<<"limit">>)},
            {<<"limit">>, -100, ?ERROR_BAD_VALUE_NOT_IN_RANGE(<<"limit">>, 1, 1000)},
            {<<"limit">>, 0, ?ERROR_BAD_VALUE_NOT_IN_RANGE(<<"limit">>, 1, 1000)},
            {<<"limit">>, 1001, ?ERROR_BAD_VALUE_NOT_IN_RANGE(<<"limit">>, 1, 1000)},
            {<<"attribute">>, <<"abc">>, ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"attribute">>, AllowedAttrs)},
            {<<"attribute">>, [<<"name">>, 8], ?ERROR_BAD_VALUE_LIST_NOT_ALLOWED(<<"attribute">>, AllowedAttrs)}
        ]
    }.


%% @private
-spec build_get_children_prepare_rest_args_fun(file_id:objectid() | file_meta:path()) ->
    onenv_api_test_runner:prepare_args_fun().
build_get_children_prepare_rest_args_fun(ValidId) ->
    fun(#api_test_ctx{data = Data0}) ->
        Data1 = utils:ensure_defined(Data0, #{}),
        {Id, Data2} = api_test_utils:maybe_substitute_bad_id(ValidId, Data1),

        RestPath = <<"data/", Id/binary, "/children">>,
        RestPathWithAttributes = lists:foldl(fun(Attr, TmpRestPath) ->
            http_utils:append_url_parameters(TmpRestPath, #{<<"attribute">> => Attr})
        end, RestPath, utils:ensure_list(maps:get(<<"attribute">>, Data2, []))),

        #rest_args{
            method = get,
            path = http_utils:append_url_parameters(
                RestPathWithAttributes,
                maps:with([<<"limit">>], Data2)
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
    Format :: rest | gs_with_details,
    ShareId :: undefined | od_share:id(),
    Params :: map(),
    AllFiles :: files()
) ->
    ok | no_return().
validate_listed_files(ListedChildren, Format, ShareId, Params, AllFiles) ->
    Limit = maps:get(<<"limit">>, Params, 1000),
    Offset = maps:get(<<"offset">>, Params, 0),
    Attributes = maps:get(<<"attribute">>, Params, undefined),

    ExpFiles1 = case Offset >= length(AllFiles) of
        true ->
            [];
        false ->
            lists:sublist(AllFiles, Offset + 1, Limit)
    end,

    ExpFiles2 = lists:map(fun({Guid, Name, Path, Details}) ->
        {file_id:guid_to_share_guid(Guid, ShareId), Name, Path, Details}
    end, ExpFiles1),

    IsLast = Limit + Offset >= length(AllFiles),

    ExpFiles3 = case Format of
        rest ->
            #{
                <<"children">> => lists:map(fun({Guid, Name, _Path, Details}) ->
                    {ok, ObjectId} = file_id:guid_to_objectid(Guid),
                    case Attributes of
                        undefined ->
                            #{
                                <<"file_id">> => ObjectId,
                                <<"name">> => Name
                            };
                        [] ->
                            #{
                                <<"file_id">> => ObjectId,
                                <<"name">> => Name
                            };
                        _ ->
                             maps:with(utils:ensure_list(Attributes),
                                api_test_utils:file_attrs_to_json(ShareId, Details#file_details.file_attr))
                    end
                end, ExpFiles2),
                <<"isLast">> => IsLast
            };

        gs_with_details ->
            #{
                <<"children">> => lists:map(fun({_Guid, _Name, _Path, Details}) ->
                    api_test_utils:file_details_to_gs_json(ShareId, Details)
                end, ExpFiles2),
                <<"isLast">> => IsLast
            }
    end,

    case Format of
        rest ->
            ?assertMatch(#{<<"nextPageToken">> := _}, ListedChildren);
        _ ->
             ok
    end,
    ?assertEqual(ExpFiles3, maps:remove(<<"nextPageToken">>, ListedChildren)).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    oct_background:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "api_tests",
        envs = [{op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}]
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_group(_Group, Config) ->
    time_test_utils:freeze_time(Config),
    lfm_proxy:init(Config, false).


end_per_group(_Group, Config) ->
    time_test_utils:unfreeze_time(Config),
    lfm_proxy:teardown(Config).


init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 10}),
    Config.


end_per_testcase(_Case, _Config) ->
    ok.
