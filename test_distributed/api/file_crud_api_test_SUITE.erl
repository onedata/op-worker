%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests concerning file crud API (REST + gs).
%%% @end
%%%-------------------------------------------------------------------
-module(file_crud_api_test_SUITE).
-author("Bartosz Walkowicz").

-include("file_api_test_utils.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/fslogic/file_details.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/graph_sync/gri.hrl").
-include_lib("ctool/include/http/codes.hrl").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    get_file_instance_test/1,
    get_shared_file_instance_test/1,
    get_file_instance_on_provider_not_supporting_space_test/1,

    update_file_instance_test/1,
    update_file_instance_on_provider_not_supporting_space_test/1,

    delete_file_instance_test/1,
    delete_file_instance_on_provider_not_supporting_space_test/1
]).

all() -> [
    get_file_instance_test,
    get_shared_file_instance_test,
    get_file_instance_on_provider_not_supporting_space_test,

    update_file_instance_test,
    update_file_instance_on_provider_not_supporting_space_test,

    delete_file_instance_test,
    delete_file_instance_on_provider_not_supporting_space_test
].


-define(ATTEMPTS, 30).


%%%===================================================================
%%% Get file instance test functions
%%%===================================================================


get_file_instance_test(Config) ->
    [P1Node] = api_test_env:get_provider_nodes(p1, Config),
    [P2Node] = api_test_env:get_provider_nodes(p2, Config),
    Providers = [P1Node, P2Node],

    {FileType, _FilePath, FileGuid, #file_details{
        file_attr = #file_attr{
            guid = FileGuid
        }
    } = FileDetails} = api_test_utils:create_file_in_space2_with_additional_metadata(
        <<"/", ?SPACE_2/binary>>, false, ?RANDOM_FILE_NAME(), Config
    ),
    ExpJsonFileDetails = file_details_to_gs_json(undefined, FileDetails),

    SpaceId = api_test_env:get_space_id(space2, Config),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    SpaceDetails = get_space_dir_details(P2Node, SpaceGuid, ?SPACE_2),
    ExpJsonSpaceDetails = file_details_to_gs_json(undefined, SpaceDetails),

    ClientSpec = #client_spec{
        correct = [
            user2,  % space owner - doesn't need any perms
            user3,  % files owner
            user4   % space member - should succeed as getting attrs doesn't require any perms
                    % TODO VFS-6766 revoke ?SPACE_VIEW priv and see that list of shares is empty
        ],
        unauthorized = [nobody],
        forbidden_not_in_space = [user1]
    },

    ?assert(onenv_api_test_runner:run_tests(Config, [
        #scenario_spec{
            name = str_utils:format("Get instance for ~s using gs private api", [FileType]),
            type = gs,
            target_nodes = Providers,
            client_spec = ClientSpec,
            prepare_args_fun = build_get_instance_prepare_gs_args_fun(FileGuid, private),
            validate_result_fun = build_get_instance_validate_gs_call_fun(ExpJsonFileDetails),
            data_spec = api_test_utils:add_file_id_errors_for_operations_available_in_share_mode(
                FileGuid, undefined, undefined
            )
        },
        #scenario_spec{
            name = str_utils:format("Get instance for ~s using gs public api", [FileType]),
            type = gs,
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SHARES,
            prepare_args_fun = build_get_instance_prepare_gs_args_fun(FileGuid, public),
            validate_result_fun = fun(#api_test_ctx{client = Client}, Result) ->
                case Client of
                    ?NOBODY -> ?assertEqual(?ERROR_UNAUTHORIZED, Result);
                    _ -> ?assertEqual(?ERROR_FORBIDDEN, Result)
                end
            end
        },
        #scenario_spec{
            name = str_utils:format("Get instance for ?SPACE_2 using gs private api"),
            type = gs,
            target_nodes = Providers,
            client_spec = ClientSpec,
            prepare_args_fun = build_get_instance_prepare_gs_args_fun(SpaceGuid, private),
            validate_result_fun = build_get_instance_validate_gs_call_fun(ExpJsonSpaceDetails)
        }
    ])).


get_shared_file_instance_test(Config) ->
    [P1] = api_test_env:get_provider_nodes(p1, Config),
    [P2] = api_test_env:get_provider_nodes(p2, Config),
    Providers = [P1, P2],

    SpaceOwnerSessId = api_test_env:get_user_session_id(user2, p1, Config),

    {FileType, _FilePath, FileGuid, #file_details{
        file_attr = FileAttr = #file_attr{
            guid = FileGuid,
            shares = OriginalShares
        }
    } = OriginalFileDetails} = api_test_utils:create_file_in_space2_with_additional_metadata(
        <<"/", ?SPACE_2/binary>>, false, ?RANDOM_FILE_NAME(), Config
    ),

    FileShareId1 = api_test_utils:share_file_and_sync_file_attrs(P1, SpaceOwnerSessId, Providers, FileGuid),
    FileShareId2 = api_test_utils:share_file_and_sync_file_attrs(P1, SpaceOwnerSessId, Providers, FileGuid),

    FileDetailsWithShares = OriginalFileDetails#file_details{
        file_attr = FileAttr#file_attr{shares = [FileShareId2, FileShareId1 | OriginalShares]}
    },

    ShareRootFileGuid = file_id:guid_to_share_guid(FileGuid, FileShareId1),
    ExpJsonShareRootFileDetails = file_details_to_gs_json(FileShareId1, FileDetailsWithShares),

    SpaceId = api_test_env:get_space_id(space2, Config),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    SpaceShareId = api_test_utils:share_file_and_sync_file_attrs(P1, SpaceOwnerSessId, Providers, SpaceGuid),
    ShareSpaceGuid = file_id:guid_to_share_guid(SpaceGuid, SpaceShareId),

    ShareSpaceDetails = get_space_dir_details(P2, SpaceGuid, ?SPACE_2),
    ExpJsonShareSpaceDetails = file_details_to_gs_json(SpaceShareId, ShareSpaceDetails),

    ShareFileGuid = file_id:guid_to_share_guid(FileGuid, SpaceShareId),
    ExpJsonShareFileDetails = file_details_to_gs_json(SpaceShareId, FileDetailsWithShares),

    ?assert(onenv_api_test_runner:run_tests(Config, [
        #scenario_spec{
            name = str_utils:format("Get instance for directly shared ~s using gs public api", [FileType]),
            type = gs,
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SHARES,
            prepare_args_fun = build_get_instance_prepare_gs_args_fun(ShareRootFileGuid, public),
            validate_result_fun = build_get_instance_validate_gs_call_fun(ExpJsonShareRootFileDetails),
            data_spec = api_test_utils:add_file_id_errors_for_operations_available_in_share_mode(
                FileGuid, FileShareId1, undefined
            )
        },
        #scenario_spec{
            name = str_utils:format("Get instance for directly shared ~s using gs private api", [FileType]),
            type = gs_with_shared_guid_and_aspect_private,
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SHARES,
            prepare_args_fun = build_get_instance_prepare_gs_args_fun(ShareRootFileGuid, private),
            validate_result_fun = fun(_, Result) -> ?assertEqual(?ERROR_UNAUTHORIZED, Result) end
        },
        #scenario_spec{
            name = str_utils:format("Get instance for indirectly shared ~s using gs public api", [FileType]),
            type = gs,
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SHARES,
            prepare_args_fun = build_get_instance_prepare_gs_args_fun(ShareFileGuid, public),
            validate_result_fun = build_get_instance_validate_gs_call_fun(ExpJsonShareFileDetails)
        },
        #scenario_spec{
            name = str_utils:format("Get instance for shared ?SPACE_2 using gs public api"),
            type = gs,
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SHARES,
            prepare_args_fun = build_get_instance_prepare_gs_args_fun(ShareSpaceGuid, public),
            validate_result_fun = build_get_instance_validate_gs_call_fun(ExpJsonShareSpaceDetails)
        }
    ])).


get_file_instance_on_provider_not_supporting_space_test(Config) ->
    P2Id = api_test_env:get_provider_id(p2, Config),
    [P2Node] = api_test_env:get_provider_nodes(p2, Config),
    {FileType, _FilePath, FileGuid, _ShareId} = api_test_utils:create_shared_file_in_space1(Config),

    ValidateGsCallResultFun = fun(_, Result) ->
        ?assertEqual(?ERROR_SPACE_NOT_SUPPORTED_BY(P2Id), Result)
    end,

    ?assert(onenv_api_test_runner:run_tests(Config, [
        #scenario_spec{
            name = str_utils:format("Get instance for ~s on provider not supporting user using gs api", [
                FileType
            ]),
            type = gs,
            target_nodes = [P2Node],
            client_spec = ?CLIENT_SPEC_FOR_SPACE_1,
            prepare_args_fun = build_get_instance_prepare_gs_args_fun(FileGuid, private),
            validate_result_fun = ValidateGsCallResultFun,
            data_spec = undefined
        }
    ])).


%% @private
-spec file_details_to_gs_json(undefined | od_share:id(), #file_details{}) -> map().
file_details_to_gs_json(ShareId, #file_details{file_attr = #file_attr{
    guid = FileGuid
}} = FileDetails) ->

    JsonFileDetails = api_test_utils:file_details_to_gs_json(ShareId, FileDetails),
    JsonFileDetails#{
        <<"gri">> => gri:serialize(#gri{
            type = op_file,
            id = file_id:guid_to_share_guid(FileGuid, ShareId),
            aspect = instance,
            scope = case ShareId of
                undefined -> private;
                _ -> public
            end
        }),
        <<"revision">> => 1
    }.


%% @private
-spec build_get_instance_prepare_gs_args_fun(file_id:file_guid(), gri:scope()) ->
    onenv_api_test_runner:prepare_args_fun().
build_get_instance_prepare_gs_args_fun(FileGuid, Scope) ->
    fun(#api_test_ctx{data = Data}) ->
        {GriId, _} = api_test_utils:maybe_substitute_bad_id(FileGuid, Data),

        #gs_args{
            operation = get,
            gri = #gri{type = op_file, id = GriId, aspect = instance, scope = Scope}
        }
    end.


%% @private
-spec build_get_instance_validate_gs_call_fun(ExpJsonDetails :: map()) ->
    onenv_api_test_runner:validate_call_result_fun().
build_get_instance_validate_gs_call_fun(ExpJsonDetails) ->
    fun(_TestCtx, Result) ->
        ?assertEqual({ok, ExpJsonDetails}, Result)
    end.


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


%%%===================================================================
%%% Update file instance test functions
%%%===================================================================


update_file_instance_test(Config) ->
    Providers = ?config(op_worker_nodes, Config),

    {FileType, _FilePath, FileGuid, ShareId} = api_test_utils:create_and_sync_shared_file_in_space2(
        8#707, Config
    ),
    ShareGuid = file_id:guid_to_share_guid(FileGuid, ShareId),

    GetMode = fun(Node) ->
        {ok, #file_attr{mode = Mode}} = api_test_utils:get_file_attrs(Node, FileGuid),
        Mode
    end,

    ?assert(onenv_api_test_runner:run_tests(Config, [
        #scenario_spec{
            name = str_utils:format("Update ~s instance using gs private api", [FileType]),
            type = gs,
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SPACE_2,

            prepare_args_fun = build_update_file_instance_test_prepare_gs_args_fun(FileGuid, private),
            validate_result_fun = fun(_, Result) ->
                ?assertEqual({ok, undefined}, Result)
            end,
            verify_fun = fun
                (expected_failure, #api_test_ctx{node = TestNode}) ->
                    ?assertMatch(8#707, GetMode(TestNode), ?ATTEMPTS),
                    true;
                (expected_success, #api_test_ctx{data = Data}) ->
                    PosixPerms = maps:get(<<"posixPermissions">>, Data, <<"0707">>),
                    Mode = binary_to_integer(PosixPerms, 8),
                    lists:foreach(fun(Node) -> ?assertMatch(Mode, GetMode(Node), ?ATTEMPTS) end, Providers),
                    true
            end,

            data_spec = api_test_utils:add_file_id_errors_for_operations_not_available_in_share_mode(
                FileGuid, ShareId, update_file_instance_test_data_spec()
            )
        },
        #scenario_spec{
            name = str_utils:format("Update ~s instance using gs public api", [FileType]),
            type = gs_not_supported,
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SHARES,
            prepare_args_fun = build_update_file_instance_test_prepare_gs_args_fun(ShareGuid, public),
            validate_result_fun = fun(_TestCaseCtx, Result) ->
                ?assertEqual(?ERROR_NOT_SUPPORTED, Result)
            end,
            data_spec = update_file_instance_test_data_spec()
        }
    ])).


update_file_instance_on_provider_not_supporting_space_test(Config) ->
    P2Id = api_test_env:get_provider_id(p2, Config),
    [P1Node] = api_test_env:get_provider_nodes(p1, Config),
    [P2Node] = api_test_env:get_provider_nodes(p2, Config),
    {FileType, _FilePath, FileGuid, _ShareId} = api_test_utils:create_shared_file_in_space1(Config),

    ?assert(onenv_api_test_runner:run_tests(Config, [
        #scenario_spec{
            name = str_utils:format("Update ~s instance on provider not supporting user using gs api", [
                FileType
            ]),
            type = gs,
            target_nodes = [P2Node],
            client_spec = ?CLIENT_SPEC_FOR_SPACE_1,
            verify_fun = fun(_, _) ->
                ?assertMatch(
                    {ok, #file_attr{mode = 8#777}},
                    api_test_utils:get_file_attrs(P1Node, FileGuid),
                    ?ATTEMPTS
                ),
                true
            end,
            prepare_args_fun = build_update_file_instance_test_prepare_gs_args_fun(FileGuid, private),
            validate_result_fun = fun(_, Result) ->
                ?assertEqual(?ERROR_SPACE_NOT_SUPPORTED_BY(P2Id), Result)
            end,
            data_spec = update_file_instance_test_data_spec()
        }
    ])).


%% @private
-spec update_file_instance_test_data_spec() -> onenv_api_test_runner:data_spec().
update_file_instance_test_data_spec() ->
    #data_spec{
        required = [<<"posixPermissions">>],
        correct_values = #{<<"posixPermissions">> => [
            <<"0000">>, <<"0111">>, <<"0544">>, <<"0707">>
        ]},
        bad_values = [
            {<<"posixPermissions">>, true, ?ERROR_BAD_VALUE_INTEGER(<<"posixPermissions">>)},
            {<<"posixPermissions">>, <<"integer">>, ?ERROR_BAD_VALUE_INTEGER(<<"posixPermissions">>)},
            {<<"posixPermissions">>, <<"0888">>, ?ERROR_BAD_VALUE_INTEGER(<<"posixPermissions">>)},
            {<<"posixPermissions">>, <<"888">>, ?ERROR_BAD_VALUE_INTEGER(<<"posixPermissions">>)},
            {<<"posixPermissions">>, <<"77777">>,
                ?ERROR_BAD_VALUE_NOT_IN_RANGE(<<"posixPermissions">>, 0, 8#777)}
        ]
    }.


%% @private
-spec build_update_file_instance_test_prepare_gs_args_fun(file_id:file_guid(), gri:scope()) ->
    onenv_api_test_runner:prepare_args_fun().
build_update_file_instance_test_prepare_gs_args_fun(FileGuid, Scope) ->
    fun(#api_test_ctx{data = Data0}) ->
        {GriId, Data1} = api_test_utils:maybe_substitute_bad_id(FileGuid, Data0),

        #gs_args{
            operation = update,
            gri = #gri{type = op_file, id = GriId, aspect = instance, scope = Scope},
            data = Data1
        }
    end.


%%%===================================================================
%%% Delete file instance test functions
%%%===================================================================


delete_file_instance_test(Config) ->
    [P1] = api_test_env:get_provider_nodes(p1, Config),
    [P2] = api_test_env:get_provider_nodes(p2, Config),
    Providers = [P1, P2],

    UserSessIdP1 = api_test_env:get_user_session_id(user3, p1, Config),
    SpaceOwnerSessId = api_test_env:get_user_session_id(user2, p1, Config),

    TopDirPath = filename:join(["/", ?SPACE_2, ?RANDOM_FILE_NAME()]),
    % TODO VFS-6932 - change to 704 when space owner will start to work on posix storage
    {ok, TopDirGuid} = lfm_proxy:mkdir(P1, UserSessIdP1, TopDirPath, 8#777),
    TopDirShareId = api_test_utils:share_file_and_sync_file_attrs(P1, SpaceOwnerSessId, Providers, TopDirGuid),
    TopDirShareGuid = file_id:guid_to_share_guid(TopDirGuid, TopDirShareId),

    FileType = api_test_utils:randomly_choose_file_type_for_test(),

    MemRef = api_test_memory:init(),

    ?assert(onenv_api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = Providers,
            client_spec = #client_spec{
                correct = [
                    user2  % space owner - doesn't need any perms
%%                    user3  TODO VFS-6933 - fix rm shared file
                ],
                unauthorized = [nobody],
                forbidden_not_in_space = [user1]
                % TODO VFS-6932 - enable after changing file mode to 704
%%                forbidden_in_space = [{user4, ?ERROR_POSIX(?EACCES)}]  % forbidden by file perms
            },

            setup_fun = build_delete_instance_setup_fun(MemRef, TopDirPath, FileType, Config),
            verify_fun = build_delete_instance_verify_fun(MemRef, FileType, Config),

            scenario_templates = [
                #scenario_template{
                    name = str_utils:format("Delete ~s instance using rest api", [FileType]),
                    type = rest,
                    prepare_args_fun = build_delete_instance_test_prepare_rest_args_fun({mem_ref, MemRef}),
                    validate_result_fun = fun(_, {ok, RespCode, _RespHeaders, _RespBody}) ->
                        ?assertEqual(?HTTP_204_NO_CONTENT, RespCode)
                    end
                },
                #scenario_template{
                    name = str_utils:format("Delete ~s instance using gs private api", [FileType]),
                    type = gs,
                    prepare_args_fun = build_delete_instance_test_prepare_gs_args_fun({mem_ref, MemRef}, private),
                    validate_result_fun = fun(_, Result) ->
                        ?assertEqual({ok, undefined}, Result)
                    end
                }
            ],

            data_spec = api_test_utils:add_file_id_errors_for_operations_not_available_in_share_mode(
                TopDirGuid, TopDirShareId, undefined
            )
        },
        #suite_spec{
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SHARES,
            scenario_templates = [
                #scenario_template{
                    name = str_utils:format("Delete shared ~s instance using rest api", [FileType]),
                    type = rest_not_supported,
                    prepare_args_fun = build_delete_instance_test_prepare_rest_args_fun({guid, TopDirShareGuid}),
                    validate_result_fun = fun(_TestCaseCtx, {ok, RespCode, _RespHeaders, RespBody}) ->
                        ?assertEqual(errors:to_http_code(?ERROR_NOT_SUPPORTED), RespCode),
                        ?assertEqual(?REST_ERROR(?ERROR_NOT_SUPPORTED), RespBody)
                    end
                }
                #scenario_template{
                    name = str_utils:format("Delete shared ~s instance using gs public api", [FileType]),
                    type = gs_not_supported,
                    prepare_args_fun = build_delete_instance_test_prepare_gs_args_fun({guid, TopDirShareGuid}, public),
                    validate_result_fun = fun(_TestCaseCtx, Result) ->
                        ?assertEqual(?ERROR_NOT_SUPPORTED, Result)
                    end
                }
            ]
        }
    ])).


delete_file_instance_on_provider_not_supporting_space_test(Config) ->
    P2Id = api_test_env:get_provider_id(p2, Config),
    [P1Node] = api_test_env:get_provider_nodes(p1, Config),
    [P2Node] = api_test_env:get_provider_nodes(p2, Config),
    {FileType, _FilePath, FileGuid, _ShareId} = api_test_utils:create_shared_file_in_space1(Config),

    ?assert(onenv_api_test_runner:run_tests(Config, [
        #scenario_spec{
            name = str_utils:format("Delete ~s instance on provider not supporting user using gs api", [
                FileType
            ]),
            type = gs,
            target_nodes = [P2Node],
            client_spec = ?CLIENT_SPEC_FOR_SPACE_1,

            prepare_args_fun = build_delete_instance_test_prepare_gs_args_fun({guid, FileGuid}, private),
            validate_result_fun = fun(_, Result) ->
                ?assertEqual(?ERROR_SPACE_NOT_SUPPORTED_BY(P2Id), Result)
            end,
            verify_fun = fun(_, _) ->
                ?assertMatch({ok, _}, api_test_utils:get_file_attrs(P1Node, FileGuid), ?ATTEMPTS),
                true
            end
        }
    ])).


%% @private
-spec build_delete_instance_setup_fun(
    api_test_memory:mem_ref(),
    file_meta:path(),
    api_test_utils:file_type(),
    onenv_api_test_runner:ct_config()
) ->
    onenv_api_test_runner:setup_fun().
build_delete_instance_setup_fun(MemRef, TopDirPath, FileType, Config) ->
    [P1Node] = api_test_env:get_provider_nodes(p1, Config),
    [P2Node] = api_test_env:get_provider_nodes(p2, Config),
    UserSessIdP1 = api_test_env:get_user_session_id(user3, p1, Config),

    fun() ->
        Path = filename:join([TopDirPath, ?RANDOM_FILE_NAME()]),
        % TODO VFS-6932 - change to 704 when space owner will start to work on posix storage
        {ok, Guid} = api_test_utils:create_file(FileType, P1Node, UserSessIdP1, Path, 8#777),
        ?assertMatch({ok, _}, api_test_utils:get_file_attrs(P2Node, Guid), ?ATTEMPTS),

        api_test_memory:set(MemRef, file_guid, Guid),

        case FileType of
            <<"dir">> ->
                % TODO VFS-6932 - uncomment when space owner will start to work on posix storage
%%                Files = lists_utils:pmap(fun(Num) ->
%%                    {_, _, FileGuid, _} = api_test_utils:create_file_in_space2_with_additional_metadata(
%%                        Path, false, <<"file_or_dir_", Num>>, Config
%%                    ),
%%                    FileGuid
%%                end, [$0, $1, $2, $3, $4]),
%%
%%                api_test_memory:set(MemRef, files_in_dir, Files);
                api_test_memory:set(MemRef, files_in_dir, []);
            _ ->
                ok
        end
    end.


%% @private
-spec build_delete_instance_verify_fun(
    api_test_memory:mem_ref(),
    api_test_utils:file_type(),
    onenv_api_test_runner:ct_config()
) ->
    onenv_api_test_runner:setup_fun().
build_delete_instance_verify_fun(MemRef, FileType, Config) ->
    Nodes = ?config(op_worker_nodes, Config),

    fun(Expectation, _) ->
        ExpResult = case Expectation of
            expected_failure -> ok;
            expected_success -> {error, ?ENOENT}
        end,

        FileGuid = api_test_memory:get(MemRef, file_guid),
        lists:foreach(fun(Node) ->
            ?assertMatch(
                ExpResult,
                ?extract_ok(lfm_proxy:stat(Node, ?ROOT_SESS_ID, {guid, FileGuid})),
                ?ATTEMPTS
            )
        end, Nodes),

        case FileType of
            <<"dir">> ->
                lists:foreach(fun(Guid) ->
                    lists:foreach(fun(Node) ->
                        ?assertMatch(
                            ExpResult,
                            ?extract_ok(lfm_proxy:stat(Node, ?ROOT_SESS_ID, {guid, Guid})),
                            ?ATTEMPTS
                        )
                    end, Nodes)
                end, api_test_memory:get(MemRef, files_in_dir));
            _ ->
                ok
        end,

        true
    end.


%% @private
-spec build_delete_instance_test_prepare_rest_args_fun(
    {guid, file_id:file_guid()} | {mem_ref, api_test_memory:mem_ref()}
) ->
    onenv_api_test_runner:prepare_args_fun().
build_delete_instance_test_prepare_rest_args_fun(MemRefOrGuid) ->
    fun(#api_test_ctx{data = Data}) ->
        BareGuid = ensure_guid(MemRefOrGuid),
        {ok, ObjectId} = file_id:guid_to_objectid(BareGuid),
        {Id, _} = api_test_utils:maybe_substitute_bad_id(ObjectId, Data),

        #rest_args{
            method = delete,
            path = <<"data/", Id/binary>>
        }
    end.


%% @private
-spec build_delete_instance_test_prepare_gs_args_fun(
    {guid, file_id:file_guid()} | {mem_ref, api_test_memory:mem_ref()},
    gri:scope()
) ->
    onenv_api_test_runner:prepare_args_fun().
build_delete_instance_test_prepare_gs_args_fun(MemRefOrGuid, Scope) ->
    fun(#api_test_ctx{data = Data}) ->
        BareGuid = ensure_guid(MemRefOrGuid),
        {Id, _} = api_test_utils:maybe_substitute_bad_id(BareGuid, Data),

        #gs_args{
            operation = delete,
            gri = #gri{type = op_file, id = Id, aspect = instance, scope = Scope}
        }
    end.


%% @private
-spec ensure_guid({guid, file_id:file_guid()} | {mem_ref, api_test_memory:mem_ref()}) ->
    file_if:file_guid().
ensure_guid({guid, Guid}) -> Guid;
ensure_guid({mem_ref, MemRef}) -> api_test_memory:get(MemRef, file_guid).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    ssl:start(),
    hackney:start(),
    api_test_env:init_per_suite(Config, #onenv_test_config{envs = [
        {op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}
    ]}).


end_per_suite(_Config) ->
    hackney:stop(),
    ssl:stop().


init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 10}),
    lfm_proxy:init(Config).


end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).
