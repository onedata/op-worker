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
    get_file_attrs_test/1,
    get_shared_file_attrs_test/1,
    get_attrs_on_provider_not_supporting_space_test/1,

    set_file_mode_test/1,
    set_mode_on_provider_not_supporting_space_test/1,

    get_file_distribution_test/1,
    get_dir_distribution_test/1
]).

all() ->
    ?ALL([
        get_file_attrs_test,
        get_shared_file_attrs_test,
        get_attrs_on_provider_not_supporting_space_test,

        set_file_mode_test,
        set_mode_on_provider_not_supporting_space_test,

        get_file_distribution_test,
        get_dir_distribution_test
    ]).


-define(ATTEMPTS, 30).


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
                    prepare_args_fun = build_get_attrs_prepare_new_id_rest_args_fun(FileObjectId),
                    validate_result_fun = build_get_attrs_validate_rest_call_fun(JsonAttrs, undefined)
                },
                #scenario_template{
                    name = <<"Get attrs from ", FileType/binary, " using /files/ rest endpoint">>,
                    type = rest_with_file_path,
                    prepare_args_fun = build_get_attrs_prepare_deprecated_path_rest_args_fun(FilePath),
                    validate_result_fun = build_get_attrs_validate_rest_call_fun(JsonAttrs, undefined)
                },
                #scenario_template{
                    name = <<"Get attrs from ", FileType/binary, " using /files-id/ rest endpoint">>,
                    type = rest,
                    prepare_args_fun = build_get_attrs_prepare_deprecated_id_rest_args_fun(FileObjectId),
                    validate_result_fun = build_get_attrs_validate_rest_call_fun(JsonAttrs, undefined)
                },
                #scenario_template{
                    name = <<"Get attrs from ", FileType/binary, " using gs api">>,
                    type = gs,
                    prepare_args_fun = build_get_attrs_prepare_gs_args_fun(FileGuid, private),
                    validate_result_fun = build_get_attrs_validate_gs_call_fun(JsonAttrs, undefined)
                }
            ],
            randomly_select_scenarios = true,
            data_spec = api_test_utils:add_file_id_errors_for_operations_available_in_share_mode(
                FileGuid, undefined, get_attrs_data_spec(normal_mode)
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
                    prepare_args_fun = build_get_attrs_prepare_new_id_rest_args_fun(ShareObjectId),
                    validate_result_fun = build_get_attrs_validate_rest_call_fun(JsonAttrs, ShareId1)
                },
                #scenario_template{
                    name = <<"Get attrs from shared ", FileType/binary, " using /files-id/ rest endpoint">>,
                    type = rest,
                    prepare_args_fun = build_get_attrs_prepare_deprecated_id_rest_args_fun(ShareObjectId),
                    validate_result_fun = build_get_attrs_validate_rest_call_fun(JsonAttrs, ShareId1)
                },
                #scenario_template{
                    name = <<"Get attrs from shared ", FileType/binary, " using gs public api">>,
                    type = gs,
                    prepare_args_fun = build_get_attrs_prepare_gs_args_fun(ShareGuid, public),
                    validate_result_fun = build_get_attrs_validate_gs_call_fun(JsonAttrs, ShareId1)
                }
            ],
            randomly_select_scenarios = true,
            data_spec = api_test_utils:add_file_id_errors_for_operations_available_in_share_mode(
                FileGuid, ShareId1, get_attrs_data_spec(share_mode)
            )
        },
        #scenario_spec{
            name = <<"Get attrs from shared ", FileType/binary, " using private gs api">>,
            type = gs_with_shared_guid_and_aspect_private,
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SHARE_SCENARIOS(Config),
            prepare_args_fun = build_get_attrs_prepare_gs_args_fun(ShareGuid, private),
            validate_result_fun = fun(_, Result) ->
                ?assertEqual(?ERROR_UNAUTHORIZED, Result)
            end,
            data_spec = api_test_utils:add_file_id_errors_for_operations_available_in_share_mode(
                FileGuid, ShareId1, get_attrs_data_spec(normal_mode)
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
                    prepare_args_fun = build_get_attrs_prepare_new_id_rest_args_fun(Space1ObjectId),
                    validate_result_fun = build_get_attrs_validate_rest_call_fun(JsonAttrs, undefined, P2)
                },
                #scenario_template{
                    name = <<"Get attrs from ", ?SPACE_1/binary, " on provider not supporting user using /files/ rest endpoint">>,
                    type = rest_with_file_path,
                    prepare_args_fun = build_get_attrs_prepare_deprecated_path_rest_args_fun(<<"/", ?SPACE_1/binary>>),
                    validate_result_fun = build_get_attrs_validate_rest_call_fun(JsonAttrs, undefined, P2)
                },
                #scenario_template{
                    name = <<"Get attrs from ", ?SPACE_1/binary, " on provider not supporting user using /files-id/ rest endpoint">>,
                    type = rest,
                    prepare_args_fun = build_get_attrs_prepare_deprecated_id_rest_args_fun(Space1ObjectId),
                    validate_result_fun = build_get_attrs_validate_rest_call_fun(JsonAttrs, undefined, P2)
                },
                #scenario_template{
                    name = <<"Get attrs from ", ?SPACE_1/binary, " on provider not supporting user using gs api">>,
                    type = gs,
                    prepare_args_fun = build_get_attrs_prepare_gs_args_fun(Space1Guid, private),
                    validate_result_fun = build_get_attrs_validate_gs_call_fun(JsonAttrs, undefined, P2)
                }
            ],
            randomly_select_scenarios = true,
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
build_get_attrs_prepare_new_id_rest_args_fun(FileObjectId) ->
    build_get_attrs_prepare_rest_args_fun(new_id, FileObjectId).


%% @private
build_get_attrs_prepare_deprecated_path_rest_args_fun(FilePath) ->
    build_get_attrs_prepare_rest_args_fun(deprecated_path, FilePath).


%% @private
build_get_attrs_prepare_deprecated_id_rest_args_fun(FileObjectId) ->
    build_get_attrs_prepare_rest_args_fun(deprecated_id, FileObjectId).


%% @private
build_get_attrs_prepare_rest_args_fun(Endpoint, ValidId) ->
    fun(#api_test_ctx{data = Data0}) ->
        Data1 = utils:ensure_defined(Data0, #{}),
        {Id, Data2} = api_test_utils:maybe_substitute_bad_id(ValidId, Data1),

        RestPath = case Endpoint of
            new_id -> <<"data/", Id/binary>>;
            deprecated_path -> <<"metadata/attrs", Id/binary>>;
            deprecated_id -> <<"metadata-id/attrs/", Id/binary>>
        end,
        #rest_args{
            method = get,
            path = http_utils:append_url_parameters(
                RestPath,
                maps:with([<<"attribute">>], Data2)
            )
        }
    end.


%% @private
build_get_attrs_prepare_gs_args_fun(FileGuid, Scope) ->
    fun(#api_test_ctx{data = Data0}) ->
        {GriId, Data1} = api_test_utils:maybe_substitute_bad_id(FileGuid, Data0),

        #gs_args{
            operation = get,
            gri = #gri{type = op_file, id = GriId, aspect = attrs, scope = Scope},
            data = Data1
        }
    end.


%% @private
build_get_attrs_validate_rest_call_fun(JsonAttrs, ShareId) ->
    build_get_attrs_validate_rest_call_fun(JsonAttrs, ShareId, undefined).


%% @private
build_get_attrs_validate_rest_call_fun(JsonAttrs, ShareId, ProviderNotSupportingSpace) ->
    fun
        (#api_test_ctx{node = TestNode}, {ok, RespCode, _RespHeaders, RespBody}) when TestNode == ProviderNotSupportingSpace ->
            ProviderDomain = ?GET_DOMAIN_BIN(ProviderNotSupportingSpace),
            ExpError = ?REST_ERROR(?ERROR_SPACE_NOT_SUPPORTED_BY(ProviderDomain)),
            ?assertEqual({?HTTP_400_BAD_REQUEST, ExpError}, {RespCode, RespBody});
        (TestCtx, {ok, RespCode, _RespHeaders, RespBody}) ->
            case get_attrs_exp_result(TestCtx, JsonAttrs, ShareId) of
                {ok, ExpAttrs} ->
                    ?assertEqual({?HTTP_200_OK, ExpAttrs}, {RespCode, RespBody});
                {error, _} = Error ->
                    ExpRestError = {errors:to_http_code(Error), ?REST_ERROR(Error)},
                    ?assertEqual(ExpRestError, {RespCode, RespBody})
            end
    end.


%% @private
build_get_attrs_validate_gs_call_fun(JsonAttrs, ShareId) ->
    build_get_attrs_validate_gs_call_fun(JsonAttrs, ShareId, undefined).


%% @private
build_get_attrs_validate_gs_call_fun(JsonAttrs, ShareId, ProviderNotSupportingSpace) ->
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

    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    FilePath = filename:join(["/", ?SPACE_2, ?RANDOM_FILE_NAME()]),
    {ok, FileGuid} = api_test_utils:create_file(FileType, P1, SessIdP1, FilePath, 8#777),
    {ok, ShareId} = lfm_proxy:create_share(P1, SessIdP1, {guid, FileGuid}, <<"share">>),

    api_test_utils:wait_for_file_sync(P2, SessIdP2, FileGuid),

    ShareGuid = file_id:guid_to_share_guid(FileGuid, ShareId),
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),

    GetExpectedResultFun = fun
        (#api_test_ctx{client = ?USER_IN_BOTH_SPACES_AUTH}) -> ok;
        (_) -> ?ERROR_POSIX(?EACCES)
    end,
    ValidateRestSuccessfulCallFun =  fun(TestCtx, {ok, RespCode, _RespHeaders, RespBody}) ->
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
            lfm_proxy:stat(Node, ?SESS_ID(?USER_IN_BOTH_SPACES, Node, Config), {guid, FileGuid})
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
                    prepare_args_fun = build_set_mode_prepare_new_id_rest_args_fun(FileObjectId),
                    validate_result_fun = ValidateRestSuccessfulCallFun
                },
                #scenario_template{
                    name = <<"Set mode for ", FileType/binary, " using /files/ rest endpoint">>,
                    type = rest_with_file_path,
                    prepare_args_fun = build_set_mode_prepare_deprecated_path_rest_args_fun(FilePath),
                    validate_result_fun = ValidateRestSuccessfulCallFun
                },
                #scenario_template{
                    name = <<"Set mode for ", FileType/binary, " using /files-id/ rest endpoint">>,
                    type = rest,
                    prepare_args_fun = build_set_mode_prepare_deprecated_id_rest_args_fun(FileObjectId),
                    validate_result_fun = ValidateRestSuccessfulCallFun
                },
                #scenario_template{
                    name = <<"Set mode for ", FileType/binary, " using gs api">>,
                    type = gs,
                    prepare_args_fun = build_set_mode_prepare_gs_args_fun(FileGuid, private),
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
            randomly_select_scenarios = true,
            data_spec = api_test_utils:add_file_id_errors_for_operations_not_available_in_share_mode(
                FileGuid, ShareId, set_mode_data_spec()
            )
        },

        #scenario_spec{
            name = <<"Set mode for shared ", FileType/binary, " using gs public api">>,
            type = gs_not_supported,
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SHARE_SCENARIOS(Config),
            prepare_args_fun = build_set_mode_prepare_gs_args_fun(ShareGuid, public),
            validate_result_fun = fun(_TestCaseCtx, Result) ->
                ?assertEqual(?ERROR_NOT_SUPPORTED, Result)
            end,
            data_spec = set_mode_data_spec()
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

    ValidateRestSetMetadataOnProvidersNotSupportingUserFun = fun
        (_TestCtx, {ok, RespCode, _RespHeaders, RespBody}) ->
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
                    prepare_args_fun = build_set_mode_prepare_new_id_rest_args_fun(Space1RootObjectId),
                    validate_result_fun = ValidateRestSetMetadataOnProvidersNotSupportingUserFun
                },
                #scenario_template{
                    name = <<"Set mode for root dir in ", ?SPACE_1/binary, " on provider not supporting user using /files/ rest endpoint">>,
                    type = rest_with_file_path,
                    prepare_args_fun = build_set_mode_prepare_deprecated_path_rest_args_fun(Space1RootDirPath),
                    validate_result_fun = ValidateRestSetMetadataOnProvidersNotSupportingUserFun
                },
                #scenario_template{
                    name = <<"Set mode for root dir in ", ?SPACE_1/binary, " on provider not supporting user using /files-id/ rest endpoint">>,
                    type = rest,
                    prepare_args_fun = build_set_mode_prepare_deprecated_id_rest_args_fun(Space1RootObjectId),
                    validate_result_fun = ValidateRestSetMetadataOnProvidersNotSupportingUserFun
                },
                #scenario_template{
                    name = <<"Set mode for root dir in ", ?SPACE_1/binary, " on provider not supporting user using gs api">>,
                    type = gs,
                    prepare_args_fun = build_set_mode_prepare_gs_args_fun(Space1RootDirGuid, private),
                    validate_result_fun = fun(_TestCtx, Result) ->
                        ?assertEqual(?ERROR_SPACE_NOT_SUPPORTED_BY(Provider2DomainBin), Result)
                    end
                }
            ],
            randomly_select_scenarios = true,
            data_spec = set_mode_data_spec()
        }
    ])).


%% @private
build_set_mode_prepare_new_id_rest_args_fun(FileObjectId) ->
    build_set_mode_prepare_rest_args_fun(new_id, FileObjectId).


%% @private
build_set_mode_prepare_deprecated_path_rest_args_fun(FilePath) ->
    build_set_mode_prepare_rest_args_fun(deprecated_path, FilePath).


%% @private
build_set_mode_prepare_deprecated_id_rest_args_fun(FileObjectId) ->
    build_set_mode_prepare_rest_args_fun(deprecated_id, FileObjectId).


%% @private
build_set_mode_prepare_rest_args_fun(Endpoint, ValidId) ->
    fun(#api_test_ctx{data = Data0}) ->
        {Id, Data1} = api_test_utils:maybe_substitute_bad_id(ValidId, Data0),

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
build_set_mode_prepare_gs_args_fun(FileGuid, Scope) ->
    fun(#api_test_ctx{data = Data0}) ->
        {GriId, Data1} = api_test_utils:maybe_substitute_bad_id(FileGuid, Data0),

        #gs_args{
            operation = create,
            gri = #gri{type = op_file, id = GriId, aspect = attrs, scope = Scope},
            data = Data1
        }
    end.


set_mode_data_spec() ->
    #data_spec{
        required = [<<"mode">>],
        correct_values = #{<<"mode">> => [<<"0000">>, <<"0111">>, <<"0544">>, <<"0777">>]},
        bad_values = [
            {<<"mode">>, true, ?ERROR_BAD_VALUE_INTEGER(<<"mode">>)},
            {<<"mode">>, <<"integer">>, ?ERROR_BAD_VALUE_INTEGER(<<"mode">>)}
        ]
    }.


%%%===================================================================
%%% Get file distribution test functions
%%%===================================================================


get_file_distribution_test(Config) ->
    [P2, P1] = ?config(op_worker_nodes, Config),
    SessIdP1 = ?USER_IN_BOTH_SPACES_SESS_ID(P1, Config),
    SessIdP2 = ?USER_IN_BOTH_SPACES_SESS_ID(P2, Config),

    FileType = <<"file">>,
    FilePath = filename:join(["/", ?SPACE_2, ?RANDOM_FILE_NAME()]),
    {ok, FileGuid} = api_test_utils:create_file(FileType, P1, SessIdP1, FilePath, 8#777),
    {ok, ShareId} = lfm_proxy:create_share(P1, SessIdP1, {guid, FileGuid}, <<"share">>),
    api_test_utils:wait_for_file_sync(P2, SessIdP2, FileGuid),

    api_test_utils:fill_file_with_dummy_data(P1, SessIdP1, FileGuid, 0, 20),
    ExpDist1 = [#{
        <<"providerId">> => ?GET_DOMAIN_BIN(P1),
        <<"blocks">> => [[0, 20]],
        <<"totalBlocksSize">> => 20
    }],
    wait_for_file_location_sync(P2, SessIdP2, FileGuid, ExpDist1),
    get_distribution_test_base(FileType, FilePath, FileGuid, ShareId, ExpDist1, Config),

    % Write another block to file on P2 and check returned distribution

    api_test_utils:fill_file_with_dummy_data(P2, SessIdP2, FileGuid, 30, 20),
    ExpDist2 = [
        #{
            <<"providerId">> => ?GET_DOMAIN_BIN(P1),
            <<"blocks">> => [[0, 20]],
            <<"totalBlocksSize">> => 20
        },
        #{
            <<"providerId">> => ?GET_DOMAIN_BIN(P2),
            <<"blocks">> => [[30, 20]],
            <<"totalBlocksSize">> => 20
        }
    ],
    wait_for_file_location_sync(P1, SessIdP1, FileGuid, ExpDist2),
    get_distribution_test_base(FileType, FilePath, FileGuid, ShareId, ExpDist2, Config).


get_dir_distribution_test(Config) ->
    [P2, P1] = ?config(op_worker_nodes, Config),
    SessIdP1 = ?USER_IN_BOTH_SPACES_SESS_ID(P1, Config),
    SessIdP2 = ?USER_IN_BOTH_SPACES_SESS_ID(P2, Config),

    FileType = <<"dir">>,
    DirPath = filename:join(["/", ?SPACE_2, ?RANDOM_FILE_NAME()]),
    {ok, DirGuid} = api_test_utils:create_file(FileType, P1, SessIdP1, DirPath, 8#777),
    {ok, ShareId} = lfm_proxy:create_share(P1, SessIdP1, {guid, DirGuid}, <<"share">>),
    api_test_utils:wait_for_file_sync(P2, SessIdP2, DirGuid),

    ExpDist1 = [],
    wait_for_file_location_sync(P2, SessIdP2, DirGuid, ExpDist1),
    get_distribution_test_base(FileType, DirPath, DirGuid, ShareId, ExpDist1, Config),

    % Create file in dir and assert that dir distribution hasn't changed

    {ok, FileGuid} = api_test_utils:create_file(
        <<"file">>, P2, SessIdP2,
        filename:join([DirPath, ?RANDOM_FILE_NAME()]),
        8#777
    ),
    api_test_utils:fill_file_with_dummy_data(P2, SessIdP2, FileGuid, 30, 20),

    ExpDist2 = [],
    wait_for_file_location_sync(P1, SessIdP1, DirGuid, ExpDist2),
    get_distribution_test_base(FileType, DirPath, DirGuid, ShareId, ExpDist2, Config).


%% @private
wait_for_file_location_sync(Node, SessId, FileGuid, ExpDistribution) ->
    ?assertMatch(
        {ok, ExpDistribution},
        lfm_proxy:get_file_distribution(Node, SessId, {guid, FileGuid}),
        ?ATTEMPTS
    ).


%% @private
get_distribution_test_base(FileType, FilePath, FileGuid, ShareId, ExpDistribution, Config) ->
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),

    ValidateRestSuccessfulCallFun =  fun(_TestCtx, {ok, RespCode, _RespHeaders, RespBody}) ->
        ?assertEqual({?HTTP_200_OK, ExpDistribution}, {RespCode, RespBody})
    end,

    ExpGsDistribution = file_gui_gs_translator:translate_distribution(ExpDistribution),

    CreateValidateGsSuccessfulCallFun = fun(Type) ->
        ExpGsResponse = ExpGsDistribution#{
            <<"gri">> => gri:serialize(#gri{
                type = Type,
                id = FileGuid,
                aspect = distribution,
                scope = private
            }),
            <<"revision">> => 1
        },
        fun(_TestCtx, Result) ->
            ?assertEqual({ok, ExpGsResponse}, Result)
        end
    end,

    ?assert(api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = ?config(op_worker_nodes, Config),
            client_spec = ?CLIENT_SPEC_FOR_SPACE_2_SCENARIOS(Config),
            scenario_templates = [
                #scenario_template{
                    name = <<"Get distribution for ", FileType/binary, " using /data/FileId/distribution rest endpoint">>,
                    type = rest,
                    prepare_args_fun = build_get_distribution_prepare_rest_args_fun(FileObjectId),
                    validate_result_fun = ValidateRestSuccessfulCallFun
                },
                #scenario_template{
                    name = <<"Get distribution for ", FileType/binary, " using op_file gs api">>,
                    type = gs,
                    prepare_args_fun = build_get_distribution_prepare_gs_args_fun(FileGuid, private),
                    validate_result_fun = CreateValidateGsSuccessfulCallFun(op_file)
                },

                %% TEST DEPRECATED REPLICA ENDPOINTS

                #scenario_template{
                    name = <<"Get distribution for ", FileType/binary, " using /replicas/ rest endpoint">>,
                    type = rest_with_file_path,
                    prepare_args_fun = build_get_replicas_prepare_rest_args_fun(path, FilePath),
                    validate_result_fun = ValidateRestSuccessfulCallFun
                },
                #scenario_template{
                    name = <<"Get distribution for ", FileType/binary, " using /replicas-id/ rest endpoint">>,
                    type = rest,
                    prepare_args_fun = build_get_replicas_prepare_rest_args_fun(id, FileObjectId),
                    validate_result_fun = ValidateRestSuccessfulCallFun
                },
                #scenario_template{
                    name = <<"Get distribution for ", FileType/binary, " using op_replica gs api">>,
                    type = gs,
                    prepare_args_fun = build_get_replicas_prepare_gs_args_fun(FileGuid, private),
                    validate_result_fun = CreateValidateGsSuccessfulCallFun(op_replica)
                }
            ],
            data_spec = api_test_utils:add_file_id_errors_for_operations_not_available_in_share_mode(
                FileGuid, ShareId, undefined
            )
        }
    ])).


%% @private
build_get_distribution_prepare_rest_args_fun(ValidId) ->
    fun(#api_test_ctx{data = Data}) ->
        {Id, _} = api_test_utils:maybe_substitute_bad_id(ValidId, Data),

        #rest_args{
            method = get,
            path = <<"data/", Id/binary, "/distribution">>
        }
    end.


%% @private
build_get_distribution_prepare_gs_args_fun(FileGuid, Scope) ->
    fun(#api_test_ctx{data = Data}) ->
        {GriId, _} = api_test_utils:maybe_substitute_bad_id(FileGuid, Data),

        #gs_args{
            operation = get,
            gri = #gri{type = op_file, id = GriId, aspect = distribution, scope = Scope}
        }
    end.


%% @private
build_get_replicas_prepare_rest_args_fun(Endpoint, ValidId) ->
    fun(#api_test_ctx{data = Data}) ->
        {Id, _} = api_test_utils:maybe_substitute_bad_id(ValidId, Data),

        #rest_args{
            method = get,
            path = case Endpoint of
                id -> <<"replicas-id/", Id/binary>>;
                path -> <<"replicas", Id/binary>>
            end
        }
    end.


%% @private
build_get_replicas_prepare_gs_args_fun(FileGuid, Scope) ->
    fun(#api_test_ctx{data = Data}) ->
        {GriId, _} = api_test_utils:maybe_substitute_bad_id(FileGuid, Data),

        #gs_args{
            operation = get,
            gri = #gri{type = op_replica, id = GriId, aspect = distribution, scope = Scope}
        }
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
