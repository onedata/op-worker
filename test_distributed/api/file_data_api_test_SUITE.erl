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

-include("file_api_test_utils.hrl").
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
    get_file_attrs_test/1,
    get_shared_file_attrs_test/1,
    get_attrs_on_provider_not_supporting_space_test/1,

    get_file_shares_test/1,

    set_file_mode_test/1,
    set_mode_on_provider_not_supporting_space_test/1,

    get_file_distribution_test/1,
    get_dir_distribution_test/1
]).

all() -> [
    get_file_attrs_test,
    get_shared_file_attrs_test,
    get_attrs_on_provider_not_supporting_space_test,

    get_file_shares_test,

    set_file_mode_test,
    set_mode_on_provider_not_supporting_space_test,

    get_file_distribution_test,
    get_dir_distribution_test
].


-define(ATTEMPTS, 30).


%%%===================================================================
%%% Get attrs test functions
%%%===================================================================


get_file_attrs_test(Config) ->
    [P1Node] = api_test_env:get_provider_nodes(p1, Config),
    [P2Node] = api_test_env:get_provider_nodes(p2, Config),

    {FileType, FilePath, FileGuid, _ShareId} = api_test_utils:create_and_sync_shared_file_in_space2(
        8#707, Config
    ),
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),

    {ok, FileAttrs} = api_test_utils:get_file_attrs(P2Node, FileGuid),
    JsonAttrs = attrs_to_json(undefined, FileAttrs),

    ?assert(onenv_api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = [P1Node, P2Node],
            client_spec = #client_spec{
                correct = [
                    user2,  % space owner - doesn't need any perms
                    user3,  % files owner
                    user4   % space member - should succeed as getting attrs doesn't require any perms
                            % TODO VFS-6766 revoke ?SPACE_VIEW priv and see that list of shares is empty
                ],
                unauthorized = [nobody],
                forbidden_not_in_space = [user1]
            },
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
    [P1Node] = api_test_env:get_provider_nodes(p1, Config),
    [P2Node] = api_test_env:get_provider_nodes(p2, Config),
    Providers = [P1Node, P2Node],

    SpaceOwnerSessId = api_test_env:get_user_session_id(user2, p1, Config),
    UserSessIdP1 = api_test_env:get_user_session_id(user3, p1, Config),

    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    FilePath = filename:join(["/", ?SPACE_2, ?RANDOM_FILE_NAME()]),
    {ok, FileGuid} = api_test_utils:create_file(FileType, P1Node, UserSessIdP1, FilePath, 8#707),

    {ok, ShareId1} = lfm_proxy:create_share(P1Node, SpaceOwnerSessId, {guid, FileGuid}, <<"share1">>),
    {ok, ShareId2} = lfm_proxy:create_share(P1Node, SpaceOwnerSessId, {guid, FileGuid}, <<"share2">>),

    ShareGuid = file_id:guid_to_share_guid(FileGuid, ShareId1),
    {ok, ShareObjectId} = file_id:guid_to_objectid(ShareGuid),

    {ok, FileAttrs} = ?assertMatch(
        {ok, #file_attr{shares = [ShareId2, ShareId1]}},
        api_test_utils:get_file_attrs(P2Node, FileGuid),
        ?ATTEMPTS
    ),
    JsonAttrs = attrs_to_json(ShareId1, FileAttrs),

    ?assert(onenv_api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SHARES,
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
            client_spec = ?CLIENT_SPEC_FOR_SHARES,
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
    P2Id = api_test_env:get_provider_id(p2, Config),
    [P2Node] = api_test_env:get_provider_nodes(p2, Config),
    {FileType, FilePath, FileGuid, _ShareId} = api_test_utils:create_shared_file_in_space1(Config),
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),

    ValidateRestCallResultFun = fun(_, {ok, RespCode, _RespHeaders, RespBody}) ->
        ExpError = ?REST_ERROR(?ERROR_SPACE_NOT_SUPPORTED_BY(P2Id)),
        ?assertEqual({?HTTP_400_BAD_REQUEST, ExpError}, {RespCode, RespBody})
    end,
    ValidateGsCallResultFun = fun(_, Result) ->
        ?assertEqual(?ERROR_SPACE_NOT_SUPPORTED_BY(P2Id), Result)
    end,

    ?assert(onenv_api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = [P2Node],
            client_spec = ?CLIENT_SPEC_FOR_SPACE_1,
            scenario_templates = [
                #scenario_template{
                    name = <<"Get attrs from ", FileType/binary, " on provider not supporting user using /data/ rest endpoint">>,
                    type = rest,
                    prepare_args_fun = build_get_attrs_prepare_new_id_rest_args_fun(FileObjectId),
                    validate_result_fun = ValidateRestCallResultFun
                },
                #scenario_template{
                    name = <<"Get attrs from ", FileType/binary, " on provider not supporting user using /files/ rest endpoint">>,
                    type = rest_with_file_path,
                    prepare_args_fun = build_get_attrs_prepare_deprecated_path_rest_args_fun(FilePath),
                    validate_result_fun = ValidateRestCallResultFun
                },
                #scenario_template{
                    name = <<"Get attrs from ", FileType/binary, " on provider not supporting user using /files-id/ rest endpoint">>,
                    type = rest,
                    prepare_args_fun = build_get_attrs_prepare_deprecated_id_rest_args_fun(FileObjectId),
                    validate_result_fun = ValidateRestCallResultFun
                },
                #scenario_template{
                    name = <<"Get attrs from ", FileType/binary, " on provider not supporting user using gs api">>,
                    type = gs,
                    prepare_args_fun = build_get_attrs_prepare_gs_args_fun(FileGuid, private),
                    validate_result_fun = ValidateGsCallResultFun
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
-spec get_attrs_data_spec(TestMode :: normal_mode | share_mode) -> onenv_api_test_runner:data_spec().
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


%% @private
-spec build_get_attrs_prepare_new_id_rest_args_fun(file_id:objectid()) ->
    onenv_api_test_runner:prepare_args_fun().
build_get_attrs_prepare_new_id_rest_args_fun(FileObjectId) ->
    build_get_attrs_prepare_rest_args_fun(new_id, FileObjectId).


%% @private
-spec build_get_attrs_prepare_deprecated_path_rest_args_fun(file_meta:path()) ->
    onenv_api_test_runner:prepare_args_fun().
build_get_attrs_prepare_deprecated_path_rest_args_fun(FilePath) ->
    build_get_attrs_prepare_rest_args_fun(deprecated_path, FilePath).


%% @private
-spec build_get_attrs_prepare_deprecated_id_rest_args_fun(file_id:objectid()) ->
    onenv_api_test_runner:prepare_args_fun().
build_get_attrs_prepare_deprecated_id_rest_args_fun(FileObjectId) ->
    build_get_attrs_prepare_rest_args_fun(deprecated_id, FileObjectId).


%% @private
-spec build_get_attrs_prepare_rest_args_fun(
    Endpoint :: new_id | deprecated_path | deprecated_id,
    ValidId :: file_meta:path() | file_id:objectid()
) ->
    onenv_api_test_runner:prepare_args_fun().
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
-spec build_get_attrs_prepare_gs_args_fun(file_id:file_guid(), gri:scope()) ->
    onenv_api_test_runner:prepare_args_fun().
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
-spec build_get_attrs_validate_rest_call_fun(AllFileAttrs :: map(), undefined | od_share:id()) ->
    onenv_api_test_runner:validate_call_result_fun().
build_get_attrs_validate_rest_call_fun(JsonAttrs, ShareId) ->
    fun(TestCtx, {ok, RespCode, _RespHeaders, RespBody}) ->
        case get_attrs_exp_result(TestCtx, JsonAttrs, ShareId) of
            {ok, ExpAttrs} ->
                ?assertEqual({?HTTP_200_OK, ExpAttrs}, {RespCode, RespBody});
            {error, _} = Error ->
                ExpRestError = {errors:to_http_code(Error), ?REST_ERROR(Error)},
                ?assertEqual(ExpRestError, {RespCode, RespBody})
        end
    end.


%% @private
-spec build_get_attrs_validate_gs_call_fun(AllFileAttrs :: map(), undefined | od_share:id()) ->
    onenv_api_test_runner:validate_call_result_fun().
build_get_attrs_validate_gs_call_fun(JsonAttrs, ShareId) ->
    fun(TestCtx, Result) ->
        case get_attrs_exp_result(TestCtx, JsonAttrs, ShareId) of
            {ok, ExpAttrs} ->
                ?assertEqual({ok, #{<<"attributes">> => ExpAttrs}}, Result);
            {error, _} = ExpError ->
                ?assertEqual(ExpError, Result)
        end
    end.


%% @private
-spec get_attrs_exp_result(
    onenv_api_test_runner:api_test_ctx(),
    AllFileAttrs :: map(),
    undefined | od_share:id()
) ->
    {ok, ExpectedFileAttrs :: map()}.
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


%%%===================================================================
%%% Get shares test functions
%%%===================================================================


get_file_shares_test(Config) ->
    [P1Node] = api_test_env:get_provider_nodes(p1, Config),
    [P2Node] = api_test_env:get_provider_nodes(p2, Config),
    Providers = [P1Node, P2Node],

    SpaceOwnerSessIdP1 = api_test_env:get_user_session_id(user2, p1, Config),

    {FileType, _FilePath, FileGuid, ShareId1} = api_test_utils:create_and_sync_shared_file_in_space2(
        8#707, Config
    ),
    ShareGuid1 = file_id:guid_to_share_guid(FileGuid, ShareId1),

    ShareId2 = api_test_utils:share_file_and_sync_file_attrs(P1Node, SpaceOwnerSessIdP1, [P2Node], FileGuid),

    ExpGsResponse = #{
        <<"revision">> => 1,
        <<"gri">> => gri:serialize(#gri{
            type = op_file, id = FileGuid, aspect = shares, scope = private
        }),
        <<"list">> => lists:map(fun(ShareId) -> gri:serialize(#gri{
            type = op_share, id = ShareId, aspect = instance, scope = private
        }) end, [ShareId2, ShareId1])
    },

    ?assert(onenv_api_test_runner:run_tests(Config, [
        #scenario_spec{
            name = <<"Get ", FileType/binary, " shares using gs private api">>,
            type = gs,
            target_nodes = Providers,
            client_spec = #client_spec{
                correct = [
                    user2,  % space owner - doesn't need any perms
                    user3,  % files owner
                    user4   % space member - should succeed as getting attrs doesn't require any perms
                            % TODO VFS-6766 revoke ?SPACE_VIEW priv and see that list of shares is empty
                ],
                unauthorized = [nobody],
                forbidden_not_in_space = [user1]
            },
            prepare_args_fun = build_get_shares_prepare_gs_args_fun(FileGuid, private),
            validate_result_fun = fun(_, Result) ->
                ?assertEqual({ok, ExpGsResponse}, Result)
            end,
            data_spec = api_test_utils:add_file_id_errors_for_operations_not_available_in_share_mode(
                FileGuid, ShareId1, undefined
            )
        },
        #scenario_spec{
            name = <<"Get ", FileType/binary, " shares using gs public api">>,
            type = gs_not_supported,
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SHARES,
            prepare_args_fun = build_get_shares_prepare_gs_args_fun(ShareGuid1, public),
            validate_result_fun = fun(_TestCaseCtx, Result) ->
                ?assertEqual(?ERROR_NOT_SUPPORTED, Result)
            end,
            data_spec = undefined
        }
    ])).


%% @private
-spec build_get_shares_prepare_gs_args_fun(file_id:file_guid(), gri:scope()) ->
    onenv_api_test_runner:prepare_args_fun().
build_get_shares_prepare_gs_args_fun(FileGuid, Scope) ->
    fun(#api_test_ctx{data = Data0}) ->
        {GriId, Data1} = api_test_utils:maybe_substitute_bad_id(FileGuid, Data0),

        #gs_args{
            operation = get,
            gri = #gri{type = op_file, id = GriId, aspect = shares, scope = Scope},
            data = Data1
        }
    end.


%%%===================================================================
%%% Set mode test functions
%%%===================================================================


set_file_mode_test(Config) ->
    Providers = ?config(op_worker_nodes, Config),
    User2Id = api_test_env:get_user_id(user2, Config),
    User3Id = api_test_env:get_user_id(user3, Config),

    {FileType, FilePath, FileGuid, ShareId} = api_test_utils:create_and_sync_shared_file_in_space2(
        8#707, Config
    ),
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),
    ShareGuid = file_id:guid_to_share_guid(FileGuid, ShareId),

    GetExpectedResultFun = fun
        (#api_test_ctx{client = ?USER(UserId)}) when UserId == User2Id orelse UserId == User3Id ->
            ok;
        (_) ->
            ?ERROR_POSIX(?EACCES)
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
        {ok, #file_attr{mode = Mode}} = api_test_utils:get_file_attrs(Node, FileGuid),
        Mode
    end,

    ?assert(onenv_api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SPACE_2,
            verify_fun = fun
                (expected_failure, #api_test_ctx{node = TestNode}) ->
                    ?assertMatch(8#707, GetMode(TestNode), ?ATTEMPTS),
                    true;
                (expected_success, #api_test_ctx{
                    client = ?USER(UserId),
                    data = #{<<"mode">> := ModeBin}
                }) when UserId == User2Id orelse UserId == User3Id ->
                    Mode = binary_to_integer(ModeBin, 8),
                    lists:foreach(fun(Node) -> ?assertMatch(Mode, GetMode(Node), ?ATTEMPTS) end, Providers),
                    true;
                (expected_success, #api_test_ctx{node = TestNode}) ->
                    ?assertMatch(8#707, GetMode(TestNode), ?ATTEMPTS),
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
            client_spec = ?CLIENT_SPEC_FOR_SHARES,
            prepare_args_fun = build_set_mode_prepare_gs_args_fun(ShareGuid, public),
            validate_result_fun = fun(_TestCaseCtx, Result) ->
                ?assertEqual(?ERROR_NOT_SUPPORTED, Result)
            end,
            data_spec = set_mode_data_spec()
        }
    ])).


set_mode_on_provider_not_supporting_space_test(Config) ->
    P2Id = api_test_env:get_provider_id(p2, Config),
    [P1Node] = api_test_env:get_provider_nodes(p1, Config),
    [P2Node] = api_test_env:get_provider_nodes(p2, Config),
    {FileType, FilePath, FileGuid, _ShareId} = api_test_utils:create_shared_file_in_space1(Config),
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),

    ValidateRestCallResultFun = fun(_, {ok, RespCode, _RespHeaders, RespBody}) ->
        ExpError = ?REST_ERROR(?ERROR_SPACE_NOT_SUPPORTED_BY(P2Id)),
        ?assertEqual({?HTTP_400_BAD_REQUEST, ExpError}, {RespCode, RespBody})
    end,
    ValidateGsCallResultFun = fun(_, Result) ->
        ?assertEqual(?ERROR_SPACE_NOT_SUPPORTED_BY(P2Id), Result)
    end,

    ?assert(onenv_api_test_runner:run_tests(Config, [
        #suite_spec{
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
            scenario_templates = [
                #scenario_template{
                    name = <<"Set mode for ", FileType/binary, " on provider not supporting user using /data/ rest endpoint">>,
                    type = rest,
                    prepare_args_fun = build_set_mode_prepare_new_id_rest_args_fun(FileObjectId),
                    validate_result_fun = ValidateRestCallResultFun
                },
                #scenario_template{
                    name = <<"Set mode for ", FileType/binary, " on provider not supporting user using /files/ rest endpoint">>,
                    type = rest_with_file_path,
                    prepare_args_fun = build_set_mode_prepare_deprecated_path_rest_args_fun(FilePath),
                    validate_result_fun = ValidateRestCallResultFun
                },
                #scenario_template{
                    name = <<"Set mode for ", FileType/binary, " on provider not supporting user using /files-id/ rest endpoint">>,
                    type = rest,
                    prepare_args_fun = build_set_mode_prepare_deprecated_id_rest_args_fun(FileObjectId),
                    validate_result_fun = ValidateRestCallResultFun
                },
                #scenario_template{
                    name = <<"Set mode for ", FileType/binary, " on provider not supporting user using gs api">>,
                    type = gs,
                    prepare_args_fun = build_set_mode_prepare_gs_args_fun(FileGuid, private),
                    validate_result_fun = ValidateGsCallResultFun
                }
            ],
            randomly_select_scenarios = true,
            data_spec = set_mode_data_spec()
        }
    ])).


%% @private
-spec set_mode_data_spec() -> onenv_api_test_runner:data_spec().
set_mode_data_spec() ->
    #data_spec{
        required = [<<"mode">>],
        correct_values = #{<<"mode">> => [<<"0000">>, <<"0111">>, <<"0544">>, <<"0707">>]},
        bad_values = [
            {<<"mode">>, true, ?ERROR_BAD_VALUE_INTEGER(<<"mode">>)},
            {<<"mode">>, <<"integer">>, ?ERROR_BAD_VALUE_INTEGER(<<"mode">>)},
            {<<"mode">>, <<"0888">>, ?ERROR_BAD_VALUE_INTEGER(<<"mode">>)},
            {<<"mode">>, <<"888">>, ?ERROR_BAD_VALUE_INTEGER(<<"mode">>)},
            {<<"mode">>, <<"77777">>, ?ERROR_BAD_VALUE_NOT_IN_RANGE(<<"mode">>, 0, 8#1777)}
        ]
    }.


%% @private
-spec build_set_mode_prepare_new_id_rest_args_fun(file_id:objectid()) ->
    onenv_api_test_runner:prepare_args_fun().
build_set_mode_prepare_new_id_rest_args_fun(FileObjectId) ->
    build_set_mode_prepare_rest_args_fun(new_id, FileObjectId).


%% @private
-spec build_set_mode_prepare_deprecated_path_rest_args_fun(file_meta:path()) ->
    onenv_api_test_runner:prepare_args_fun().
build_set_mode_prepare_deprecated_path_rest_args_fun(FilePath) ->
    build_set_mode_prepare_rest_args_fun(deprecated_path, FilePath).


%% @private
-spec build_set_mode_prepare_deprecated_id_rest_args_fun(file_id:objectid()) ->
    onenv_api_test_runner:prepare_args_fun().
build_set_mode_prepare_deprecated_id_rest_args_fun(FileObjectId) ->
    build_set_mode_prepare_rest_args_fun(deprecated_id, FileObjectId).


%% @private
-spec build_set_mode_prepare_rest_args_fun(
    Endpoint :: new_id | deprecated_path | deprecated_id,
    ValidId :: file_meta:path() | file_id:objectid()
) ->
    onenv_api_test_runner:prepare_args_fun().
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
-spec build_set_mode_prepare_gs_args_fun(file_id:file_guid(), gri:scope()) ->
    onenv_api_test_runner:prepare_args_fun().
build_set_mode_prepare_gs_args_fun(FileGuid, Scope) ->
    fun(#api_test_ctx{data = Data0}) ->
        {GriId, Data1} = api_test_utils:maybe_substitute_bad_id(FileGuid, Data0),

        #gs_args{
            operation = create,
            gri = #gri{type = op_file, id = GriId, aspect = attrs, scope = Scope},
            data = Data1
        }
    end.


%%%===================================================================
%%% Get file distribution test functions
%%%===================================================================


get_file_distribution_test(Config) ->
    P1Id = api_test_env:get_provider_id(p1, Config),
    [P1Node] = api_test_env:get_provider_nodes(p1, Config),

    P2Id = api_test_env:get_provider_id(p2, Config),
    [P2Node] = api_test_env:get_provider_nodes(p2, Config),

    SpaceOwnerSessIdP1 = api_test_env:get_user_session_id(user2, p1, Config),
    UserSessIdP1 = api_test_env:get_user_session_id(user3, p1, Config),
    UserSessIdP2 = api_test_env:get_user_session_id(user3, p2, Config),

    FileType = <<"file">>,
    FilePath = filename:join(["/", ?SPACE_2, ?RANDOM_FILE_NAME()]),
    {ok, FileGuid} = api_test_utils:create_file(FileType, P1Node, UserSessIdP1, FilePath, 8#707),
    {ok, ShareId} = lfm_proxy:create_share(P1Node, SpaceOwnerSessIdP1, {guid, FileGuid}, <<"share">>),

    api_test_utils:wait_for_file_sync(P2Node, UserSessIdP2, FileGuid),

    api_test_utils:fill_file_with_dummy_data(P1Node, UserSessIdP1, FileGuid, 0, 20),
    ExpDist1 = [#{
        <<"providerId">> => P1Id,
        <<"blocks">> => [[0, 20]],
        <<"totalBlocksSize">> => 20
    }],
    wait_for_file_location_sync(P2Node, UserSessIdP2, FileGuid, ExpDist1),
    get_distribution_test_base(FileType, FilePath, FileGuid, ShareId, ExpDist1, Config),

    % Write another block to file on P2 and check returned distribution

    api_test_utils:fill_file_with_dummy_data(P2Node, UserSessIdP2, FileGuid, 30, 20),
    ExpDist2 = [
        #{
            <<"providerId">> => P1Id,
            <<"blocks">> => [[0, 20]],
            <<"totalBlocksSize">> => 20
        },
        #{
            <<"providerId">> => P2Id,
            <<"blocks">> => [[30, 20]],
            <<"totalBlocksSize">> => 20
        }
    ],
    wait_for_file_location_sync(P1Node, UserSessIdP1, FileGuid, ExpDist2),
    get_distribution_test_base(FileType, FilePath, FileGuid, ShareId, ExpDist2, Config).


get_dir_distribution_test(Config) ->
    [P1Node] = api_test_env:get_provider_nodes(p1, Config),
    [P2Node] = api_test_env:get_provider_nodes(p2, Config),

    SpaceOwnerSessIdP1 = api_test_env:get_user_session_id(user2, p1, Config),
    UserSessIdP1 = api_test_env:get_user_session_id(user3, p1, Config),
    UserSessIdP2 = api_test_env:get_user_session_id(user3, p2, Config),

    FileType = <<"dir">>,
    DirPath = filename:join(["/", ?SPACE_2, ?RANDOM_FILE_NAME()]),
    {ok, DirGuid} = api_test_utils:create_file(FileType, P1Node, UserSessIdP1, DirPath, 8#707),
    {ok, ShareId} = lfm_proxy:create_share(P1Node, SpaceOwnerSessIdP1, {guid, DirGuid}, <<"share">>),
    api_test_utils:wait_for_file_sync(P2Node, UserSessIdP2, DirGuid),

    ExpDist = [],
    wait_for_file_location_sync(P2Node, UserSessIdP2, DirGuid, ExpDist),
    get_distribution_test_base(FileType, DirPath, DirGuid, ShareId, ExpDist, Config),

    % Create file in dir and assert that dir distribution hasn't changed

    {ok, FileGuid} = api_test_utils:create_file(
        <<"file">>, P2Node, UserSessIdP2,
        filename:join([DirPath, ?RANDOM_FILE_NAME()]),
        8#707
    ),
    api_test_utils:fill_file_with_dummy_data(P2Node, UserSessIdP2, FileGuid, 30, 20),

    wait_for_file_location_sync(P1Node, UserSessIdP1, DirGuid, ExpDist),
    get_distribution_test_base(FileType, DirPath, DirGuid, ShareId, ExpDist, Config).


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
                type = Type, id = FileGuid, aspect = distribution, scope = private
            }),
            <<"revision">> => 1
        },
        fun(_TestCtx, Result) ->
            ?assertEqual({ok, ExpGsResponse}, Result)
        end
    end,

    ?assert(onenv_api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = ?config(op_worker_nodes, Config),
            client_spec = ?CLIENT_SPEC_FOR_SPACE_2,
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
-spec build_get_distribution_prepare_rest_args_fun(file_id:objectid()) ->
    onenv_api_test_runner:prepare_args_fun().
build_get_distribution_prepare_rest_args_fun(FileObjectId) ->
    fun(#api_test_ctx{data = Data}) ->
        {Id, _} = api_test_utils:maybe_substitute_bad_id(FileObjectId, Data),

        #rest_args{
            method = get,
            path = <<"data/", Id/binary, "/distribution">>
        }
    end.


%% @private
-spec build_get_distribution_prepare_gs_args_fun(file_id:file_guid(), gri:scope()) ->
    onenv_api_test_runner:prepare_args_fun().
build_get_distribution_prepare_gs_args_fun(FileGuid, Scope) ->
    fun(#api_test_ctx{data = Data}) ->
        {GriId, _} = api_test_utils:maybe_substitute_bad_id(FileGuid, Data),

        #gs_args{
            operation = get,
            gri = #gri{type = op_file, id = GriId, aspect = distribution, scope = Scope}
        }
    end.


%% @private
-spec build_get_replicas_prepare_rest_args_fun(
    Endpoint :: id | path,
    ValidId :: file_meta:path() | file_id:objectid()
) ->
    onenv_api_test_runner:prepare_args_fun().
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
-spec build_get_replicas_prepare_gs_args_fun(file_id:file_guid(), gri:scope()) ->
    onenv_api_test_runner:prepare_args_fun().
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
