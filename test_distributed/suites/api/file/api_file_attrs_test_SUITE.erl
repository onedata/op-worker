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
-module(api_file_attrs_test_SUITE).
-author("Bartosz Walkowicz").

-include("api_file_test_utils.hrl").
-include("modules/fslogic/data_distribution.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("modules/dir_stats_collector/dir_size_stats.hrl").
-include("onenv_test_utils.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/graph_sync/gri.hrl").
-include_lib("ctool/include/http/codes.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("cluster_worker/include/time_series/browsing.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_time_series.hrl").

-export([
    groups/0, all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_group/2, end_per_group/2,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    get_file_attrs_test/1,
    get_file_attrs_with_xattrs_test/1,
    get_shared_file_attrs_test/1,
    get_attrs_on_provider_not_supporting_space_test/1,

    get_file_shares_test/1,

    set_file_mode_test/1,
    set_mode_on_provider_not_supporting_space_test/1,

    get_reg_file_distribution_test/1,
    get_dir_distribution_1_test/1,
    get_dir_distribution_2_test/1,
    get_dir_distribution_3_test/1,
    get_symlink_distribution_test/1,
    
    get_reg_file_storage_locations_test_posix/1,
    get_reg_file_storage_locations_test_s3/1,

    test_for_hardlink_between_files_test/1,
    
    get_historical_dir_size_stats_schema_test/1,
    get_historical_dir_size_stats_layout_test/1,
    get_historical_dir_size_stats_slice_test/1,
    get_historical_dir_size_stats_disabled_test/1
]).

groups() -> [
    {parallel_tests, [parallel], [
        get_file_attrs_test,
        get_file_attrs_with_xattrs_test,
        get_shared_file_attrs_test,
        get_attrs_on_provider_not_supporting_space_test,

        get_file_shares_test,

        set_file_mode_test,
        set_mode_on_provider_not_supporting_space_test,

        get_symlink_distribution_test,
        get_reg_file_distribution_test,

        get_reg_file_storage_locations_test_posix,
        get_reg_file_storage_locations_test_s3,

        test_for_hardlink_between_files_test
    ]},
    {sequential_tests, [sequential], [
        get_dir_distribution_1_test,
        get_dir_distribution_2_test,
        get_dir_distribution_3_test,
        get_historical_dir_size_stats_schema_test,
        get_historical_dir_size_stats_layout_test,
        get_historical_dir_size_stats_slice_test,
        get_historical_dir_size_stats_disabled_test
    ]}
].

all() -> [
    {group, parallel_tests},
    {group, sequential_tests}
].


-define(BLOCK(__OFFSET, __SIZE), #file_block{offset = __OFFSET, size = __SIZE}).
-define(ATTEMPTS, 30).
-define(MAX_WINDOW_LIMIT, 1000).


%%%===================================================================
%%% Get attrs test functions
%%%===================================================================

get_file_attrs_test(Config) ->
    [P2Node] = oct_background:get_provider_nodes(paris),
    
    {FileType, _FilePath, FileGuid, _ShareId} = api_test_utils:create_and_sync_shared_file_in_space_krk_par(8#707),
    
    {ok, FileAttrs} = file_test_utils:get_attrs(P2Node, FileGuid),
    DataSpec = api_test_utils:add_file_id_errors_for_operations_available_in_share_mode(
        FileGuid, undefined, get_attrs_data_spec(normal_mode)
    ),
    get_file_attrs_test_base(Config, DataSpec, FileType, FileGuid, FileAttrs).


get_file_attrs_with_xattrs_test(Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    
    {FileType, _FilePath, FileGuid, _ShareId} = api_test_utils:create_and_sync_shared_file_in_space_krk_par(?DEFAULT_FILE_MODE),
    ok = file_test_utils:set_xattr(P1Node, FileGuid, <<"xattr_name">>, <<"xattr_value">>),
    ok = file_test_utils:set_xattr(P1Node, FileGuid, <<"xattr_name2">>, <<"xattr_value2">>),
    file_test_utils:await_xattr(P2Node, FileGuid, [<<"xattr_name">>, <<"xattr_name2">>], ?ATTEMPTS),
    {ok, FileAttrs} = file_test_utils:get_attrs(P2Node, FileGuid),
    
    DataSpec = #data_spec{
        optional = [<<"attributes">>],
        correct_values = #{<<"attributes">> => [<<"xattr.xattr_name">>]}
    },
    get_file_attrs_test_base(Config, DataSpec, FileType, FileGuid, FileAttrs#file_attr{xattrs = #{<<"xattr_name">> => <<"xattr_value">>}}).


get_file_attrs_test_base(Config, DataSpec, FileType, FileGuid, FileAttrs) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),

    ?assert(onenv_api_test_runner:run_tests([
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
                    prepare_args_fun = build_get_attrs_prepare_rest_args_fun(FileObjectId),
                    validate_result_fun = build_get_attrs_validate_rest_call_fun(FileAttrs, undefined)
                },
                #scenario_template{
                    name = <<"Get attrs from ", FileType/binary, " using gs private api">>,
                    type = gs,
                    prepare_args_fun = build_get_attrs_prepare_gs_args_fun(FileGuid, private),
                    validate_result_fun = build_get_attrs_validate_gs_call_fun(FileAttrs, undefined)
                }
            ],
            randomly_select_scenarios = true,
            data_spec = DataSpec
        },
        #suite_spec{
            target_nodes = ?config(op_worker_nodes, Config),
            client_spec = ?CLIENT_SPEC_FOR_SHARES,
            scenario_templates = [
                #scenario_template{
                    name = <<"Get attrs from ", FileType/binary, " using gs public api">>,
                    type = gs,
                    prepare_args_fun = build_get_attrs_prepare_gs_args_fun(FileGuid, public),
                    validate_result_fun = fun(#api_test_ctx{client = Client}, Result) ->
                        case Client of
                            ?NOBODY -> ?assertEqual(?ERROR_UNAUTHORIZED, Result);
                            _ -> ?assertEqual(?ERROR_FORBIDDEN, Result)
                        end
                    end
                }
            ]
        }
    ])).


get_shared_file_attrs_test(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    Providers = [P1Node, P2Node],

    SpaceOwnerSessId = oct_background:get_user_session_id(user2, krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user3, krakow),

    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    FilePath = filename:join(["/", ?SPACE_KRK_PAR, ?RANDOM_FILE_NAME()]),
    {ok, FileGuid} = lfm_test_utils:create_file(FileType, P1Node, UserSessIdP1, FilePath, 8#707),

    ShareId1 = api_test_utils:share_file_and_sync_file_attrs(P1Node, SpaceOwnerSessId, Providers, FileGuid),
    ShareId2 = api_test_utils:share_file_and_sync_file_attrs(P1Node, SpaceOwnerSessId, Providers, FileGuid),

    ShareGuid = file_id:guid_to_share_guid(FileGuid, ShareId1),
    {ok, ShareObjectId} = file_id:guid_to_objectid(ShareGuid),

    {ok, FileAttrs} = ?assertMatch(
        {ok, #file_attr{shares = [ShareId2, ShareId1]}},
        file_test_utils:get_attrs(P2Node, FileGuid),
        ?ATTEMPTS
    ),

    ?assert(onenv_api_test_runner:run_tests([
        #suite_spec{
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SHARES,
            scenario_templates = [
                #scenario_template{
                    name = <<"Get attrs from shared ", FileType/binary, " using /data/ rest endpoint">>,
                    type = {rest_with_shared_guid, file_id:guid_to_space_id(FileGuid)},
                    prepare_args_fun = build_get_attrs_prepare_rest_args_fun(ShareObjectId),
                    validate_result_fun = build_get_attrs_validate_rest_call_fun(FileAttrs, ShareId1)
                },
                #scenario_template{
                    name = <<"Get attrs from shared ", FileType/binary, " using gs public api">>,
                    type = gs,
                    prepare_args_fun = build_get_attrs_prepare_gs_args_fun(ShareGuid, public),
                    validate_result_fun = build_get_attrs_validate_gs_call_fun(FileAttrs, ShareId1)
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


get_attrs_on_provider_not_supporting_space_test(_Config) ->
    P2Id = oct_background:get_provider_id(paris),
    [P2Node] = oct_background:get_provider_nodes(paris),
    SpaceId = oct_background:get_space_id(space_krk),
    {FileType, _FilePath, FileGuid, _ShareId} = api_test_utils:create_shared_file_in_space_krk(),
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),

    ValidateRestCallResultFun = fun(_, {ok, RespCode, _RespHeaders, RespBody}) ->
        ExpError = ?REST_ERROR(?ERROR_SPACE_NOT_SUPPORTED_BY(SpaceId, P2Id)),
        ?assertEqual({?HTTP_400_BAD_REQUEST, ExpError}, {RespCode, RespBody})
    end,
    ValidateGsCallResultFun = fun(_, Result) ->
        ?assertEqual(?ERROR_SPACE_NOT_SUPPORTED_BY(SpaceId, P2Id), Result)
    end,

    ?assert(onenv_api_test_runner:run_tests([
        #suite_spec{
            target_nodes = [P2Node],
            client_spec = ?CLIENT_SPEC_FOR_SPACE_KRK,
            scenario_templates = [
                #scenario_template{
                    name = <<"Get attrs from ", FileType/binary, " on provider not supporting user using /data/ rest endpoint">>,
                    type = rest,
                    prepare_args_fun = build_get_attrs_prepare_rest_args_fun(FileObjectId),
                    validate_result_fun = ValidateRestCallResultFun
                },
                #scenario_template{
                    name = <<"Get attrs from ", FileType/binary, " on provider not supporting user using gs api">>,
                    type = gs,
                    prepare_args_fun = build_get_attrs_prepare_gs_args_fun(FileGuid, private),
                    validate_result_fun = ValidateGsCallResultFun
                }
            ]
        }
    ])).


test_for_hardlink_between_files_test(_Config) ->
    [ProviderNode] = oct_background:get_provider_nodes(krakow),
    GenPathFun = fun() ->
        filename:join(["/", ?SPACE_KRK_PAR, ?RANDOM_FILE_NAME()])
    end,
    UserSessIdP1 = oct_background:get_user_session_id(user3, krakow),
    {ok, TargetGuid} = lfm_test_utils:create_file(<<"file">>, ProviderNode, UserSessIdP1, GenPathFun()),
    {ok, NotAffiliatedGuid} = lfm_test_utils:create_file(<<"file">>, ProviderNode, UserSessIdP1, GenPathFun()),
    {ok, #file_attr{guid = LinkGuid1}} = lfm_proxy:make_link(ProviderNode, UserSessIdP1, GenPathFun(), TargetGuid),
    {ok, #file_attr{guid = LinkGuid2}} = lfm_proxy:make_link(ProviderNode, UserSessIdP1, GenPathFun(), TargetGuid),
    
    MemRef = api_test_memory:init(),
    
    ValidateResultFun = fun
        (<<"invalid_guid">>, Result, _ExpectedOk, _ExpectedError, ExpectedBadValue) ->
            ?assertEqual(ExpectedBadValue, Result);
        (Guid, Result, ExpectedOk, ExpectedError, _ExpectedBadValue) ->
            case lists:member(Guid, [TargetGuid, LinkGuid1, LinkGuid2]) of
                true -> ?assertEqual(ExpectedOk, Result);
                false -> ?assertEqual(ExpectedError, Result)
            end
    end,
    
    ValidateGsCallResultFun = fun
        (_, Result) ->
            MappedResult = case Result of
                {ok, ResultMap} -> {ok, maps:without([<<"gri">>, <<"revision">>], ResultMap)};
                _ -> Result
            end, 
            Guid = api_test_memory:get(MemRef, <<"guid">>),
            ValidateResultFun(Guid, MappedResult, {ok, #{}}, ?ERROR_NOT_FOUND, ?ERROR_BAD_VALUE_IDENTIFIER(<<"guid">>))
    end,
    
    ValidateRestCallResultFun = fun
        (_, {ok, RespCode, _RespHeaders, _RespBody}) ->
            Guid = api_test_memory:get(MemRef, <<"guid">>),
            ValidateResultFun(Guid, RespCode, 204, 404, 400)
    end,
    
    ?assert(onenv_api_test_runner:run_tests([
        #suite_spec{
            target_nodes = [ProviderNode],
            client_spec = #client_spec{
                correct = [
                    user2,  % space owner - doesn't need any perms
                    user3,  % files owner
                    user4   % space member - should succeed as no perms are required
                ]
            },
            scenario_templates = [
                #scenario_template{
                    name = <<"Test for hardlink between files using gs api">>,
                    type = gs,
                    prepare_args_fun = build_get_hardlink_relation_prepare_gs_args_fun(MemRef, TargetGuid),
                    validate_result_fun = ValidateGsCallResultFun
                },
                #scenario_template{
                    name = <<"Test for hardlink between files using rest api">>,
                    type = rest,
                    prepare_args_fun = build_get_hardlink_relation_prepare_rest_args_fun(MemRef, TargetGuid),
                    validate_result_fun = ValidateRestCallResultFun
                }
            ],
            data_spec = #data_spec{
                optional = [<<"guid">>],
                correct_values = #{
                    <<"guid">> => [LinkGuid1, LinkGuid2, NotAffiliatedGuid, TargetGuid]
                }
            }
        }
    ])).


%% @private
-spec get_attrs_data_spec(TestMode :: normal_mode | share_mode) -> onenv_api_test_runner:data_spec().
get_attrs_data_spec(normal_mode) ->
    AllowedAttrsJson = [onedata_file:attr_name_to_json(A) || A <- ?API_FILE_ATTRS],
    #data_spec{
        optional = [<<"attributes">>],
        correct_values = #{<<"attributes">> => AllowedAttrsJson},
        bad_values = [
            {<<"attributes">>, true, ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"attributes">>, AllowedAttrsJson ++ [<<"xattr.*">>])},
            {<<"attributes">>, 10, {gs, ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"attributes">>, AllowedAttrsJson ++ [<<"xattr.*">>])}},
            {<<"attributes">>, <<"NaN">>, ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"attributes">>, AllowedAttrsJson ++ [<<"xattr.*">>])}
        ]
    };
get_attrs_data_spec(share_mode) ->
    AllowedAttrsJson = [onedata_file:attr_name_to_json(A) || A <- ?PUBLIC_API_FILE_ATTRS],
    #data_spec{
        optional = [<<"attributes">>],
        correct_values = #{<<"attributes">> => AllowedAttrsJson},
        bad_values = [
            {<<"attributes">>, true, ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"attributes">>, AllowedAttrsJson ++ [<<"xattr.*">>])},
            {<<"attributes">>, 10, {gs, ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"attributes">>, AllowedAttrsJson ++ [<<"xattr.*">>])}},
            {<<"attributes">>, <<"NaN">>, ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"attributes">>, AllowedAttrsJson ++ [<<"xattr.*">>])},
            {<<"attributes">>, <<"owner_id">>, ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"attributes">>, AllowedAttrsJson ++ [<<"xattr.*">>])}
        ]
    }.


%% @private
-spec build_get_attrs_prepare_rest_args_fun(file_meta:path() | file_id:objectid()) ->
    onenv_api_test_runner:prepare_args_fun().
build_get_attrs_prepare_rest_args_fun(ValidId) ->
    fun(#api_test_ctx{data = Data0}) ->
        Data1 = utils:ensure_defined(Data0, #{}),
        {Id, Data2} = api_test_utils:maybe_substitute_bad_id(ValidId, Data1),

        RestPath = <<"data/", Id/binary>>,
    
        % randomly pass options in body or in query string
        {FinalPath, FinalBody} = case rand:uniform(2) of
            1 -> {
                RestPath,
                json_utils:encode(maps:with([<<"attributes">>], Data2))
            };
            2 -> {
                http_utils:append_url_parameters(RestPath, maps:with([<<"attributes">>], Data2)),
                <<>>
            }
        end,

        #rest_args{
            method = get,
            path = FinalPath,
            body = FinalBody
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
            gri = #gri{type = op_file, id = GriId, aspect = instance, scope = Scope},
            data = Data1
        }
    end.


%% @private
-spec build_get_hardlink_relation_prepare_gs_args_fun(api_test_memory:mem_ref(), file_id:file_guid()) ->
    onenv_api_test_runner:prepare_args_fun().
build_get_hardlink_relation_prepare_gs_args_fun(MemRef, FileGuid) ->
    fun(#api_test_ctx{data = Data0}) ->
        {GriId, Data1} = api_test_utils:maybe_substitute_bad_id(FileGuid, Data0),
        Guid2 = maps:get(<<"guid">>, Data1, <<"invalid_guid">>),
        api_test_memory:set(MemRef, <<"guid">>, Guid2),
        
        #gs_args{
            operation = get,
            gri = #gri{type = op_file, id = GriId, aspect = {hardlinks, Guid2}, scope = private},
            data = undefined
        }
    end.


%% @private
-spec build_get_hardlink_relation_prepare_rest_args_fun(api_test_memory:mem_ref(), file_id:file_guid()) ->
    onenv_api_test_runner:prepare_args_fun().
build_get_hardlink_relation_prepare_rest_args_fun(MemRef, FileGuid) ->
    fun(#api_test_ctx{data = Data0}) ->
        Data1 = utils:ensure_defined(Data0, #{}),
        {ok, ValidObjectId} = file_id:guid_to_objectid(FileGuid),
        {Id, _Data2} = api_test_utils:maybe_substitute_bad_id(ValidObjectId, Data1),
        Guid2 = maps:get(<<"guid">>, Data1, <<"invalid_guid">>),
        {ok, ObjectId2} = file_id:guid_to_objectid(Guid2),
        api_test_memory:set(MemRef, <<"guid">>, Guid2),
        
        RestPath = <<"data/", Id/binary, "/hardlinks/", ObjectId2/binary>>,
        
        #rest_args{
            method = get,
            path = RestPath
        }
    end.


%% @private
-spec build_get_attrs_validate_rest_call_fun(#file_attr{}, undefined | od_share:id()) ->
    onenv_api_test_runner:validate_call_result_fun().
build_get_attrs_validate_rest_call_fun(FileAttr, ShareId) ->
    fun(TestCtx, {ok, RespCode, _RespHeaders, RespBody}) ->
        case get_attrs_exp_result(TestCtx, FileAttr, ShareId, rest) of
            {ok, ExpAttrs} ->
                ?assertEqual({?HTTP_200_OK, ExpAttrs}, {RespCode, RespBody});
            {error, _} = Error ->
                ?assertEqual({?HTTP_400_BAD_REQUEST, #{<<"error">> => errors:to_json(Error)}}, {RespCode, RespBody})
        end
    end.


%% @private
-spec build_get_attrs_validate_gs_call_fun(#file_attr{}, undefined | od_share:id()) ->
    onenv_api_test_runner:validate_call_result_fun().
build_get_attrs_validate_gs_call_fun(FileAttr, ShareId) ->
    fun(TestCtx, Result) ->
        case get_attrs_exp_result(TestCtx, FileAttr, ShareId, gs) of
            {ok, ExpAttrs} ->
                {ok, ResultMap} = ?assertMatch({ok, _}, Result),
                ?assertEqual(ExpAttrs, maps:without([<<"gri">>, <<"revision">>], ResultMap));
            {error, _} = Error ->
                ?assertEqual(Error, Result)
        end
    end.


%% @private
-spec get_attrs_exp_result(
    onenv_api_test_runner:api_test_ctx(),
    file_attr:record(),
    undefined | od_share:id(),
    Format :: gs | rest
) ->
    {ok, ExpectedFileAttrs :: map()}.
get_attrs_exp_result(#api_test_ctx{data = Data, client = Client, node = Node}, #file_attr{acl = Acl} = FileAttr, ShareId, Format) ->
    User4Id = oct_background:get_user_id(user4),
    case {Client, is_acl_requested(Data), Acl} of
        {?USER(UserId), true, [_|_]} when UserId == User4Id ->
            ?ERROR_POSIX(?EACCES);
        _ ->
            ProviderId = case ShareId of
                undefined -> opw_test_rpc:get_provider_id(Node);
                _ -> undefined
            end,
            JsonAttrs = api_test_utils:file_attr_to_json(ShareId, Format, ProviderId, FileAttr),
            {AttrType, RequestedAttributesJson} = case maps:get(<<"attributes">>, Data, undefined) of
                undefined ->
                    case ShareId of
                        %% @TODO VFS-11377 change defaults after deprecated attrs are removed
                        undefined ->
                            {deprecated, lists:flatmap(fun(A) ->
                                [onedata_file:attr_name_to_json(deprecated, A), onedata_file:attr_name_to_json(A)]
                            end, ?DEPRECATED_ALL_FILE_ATTRS)};
                        _ ->
                            {deprecated, lists:flatmap(fun(A) ->
                                [onedata_file:attr_name_to_json(deprecated, A), onedata_file:attr_name_to_json(A)]
                            end, ?DEPRECATED_PUBLIC_FILE_ATTRS)}
                    end;
                Attr ->
                    {current, utils:ensure_list(Attr)}
            end,
            JsonAttrs2 = case AttrType of
                deprecated -> maps:merge(JsonAttrs, api_test_utils:replace_attrs_with_deprecated(JsonAttrs));
                current -> JsonAttrs
            end,
            {ok, maps:with(RequestedAttributesJson, JsonAttrs2)}
    end.


%% @private
is_acl_requested(Data) ->
    lists:member(<<"acl">>, utils:ensure_list(maps:get(<<"attributes">>, Data, maps:get(<<"attribute">>, Data, [])))).


%%%===================================================================
%%% Get shares test functions
%%%===================================================================

get_file_shares_test(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    Providers = [P1Node, P2Node],

    SpaceOwnerSessIdP1 = oct_background:get_user_session_id(user2, krakow),

    {FileType, _FilePath, FileGuid, ShareId1} = api_test_utils:create_and_sync_shared_file_in_space_krk_par(8#707),
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

    ?assert(onenv_api_test_runner:run_tests([
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
    User2Id = oct_background:get_user_id(user2),
    User3Id = oct_background:get_user_id(user3),

    {FileType, _FilePath, FileGuid, ShareId} = api_test_utils:create_and_sync_shared_file_in_space_krk_par(8#707),
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),
    ShareGuid = file_id:guid_to_share_guid(FileGuid, ShareId),

    GetExpectedResultFun = fun
        (#api_test_ctx{client = ?USER(UserId)}) when UserId == User2Id orelse UserId == User3Id ->
            ok;
        (_) ->
            ?ERROR_POSIX(?EACCES)
    end,
    ValidateRestSuccessfulCallFun = fun(TestCtx, {ok, RespCode, _RespHeaders, RespBody}) ->
        {ExpCode, ExpBody} = case GetExpectedResultFun(TestCtx) of
            ok ->
                {?HTTP_204_NO_CONTENT, #{}};
            {error, _} = ExpError ->
                {errors:to_http_code(ExpError), ?REST_ERROR(ExpError)}
        end,
        ?assertEqual({ExpCode, ExpBody}, {RespCode, RespBody})
    end,

    GetMode = fun(Node) ->
        {ok, #file_attr{mode = Mode}} = file_test_utils:get_attrs(Node, FileGuid),
        Mode
    end,

    ?assert(onenv_api_test_runner:run_tests([
        #suite_spec{
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SPACE_KRK_PAR,
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
                    prepare_args_fun = build_set_mode_prepare_rest_args_fun(FileObjectId),
                    validate_result_fun = ValidateRestSuccessfulCallFun
                },
                #scenario_template{
                    name = <<"Set mode for ", FileType/binary, " using gs api">>,
                    type = gs,
                    prepare_args_fun = build_set_mode_prepare_gs_args_fun(FileGuid, private),
                    validate_result_fun = fun(TestCtx, Result) ->
                        case GetExpectedResultFun(TestCtx) of
                            ok ->
                                ?assertEqual(ok, Result);
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


set_mode_on_provider_not_supporting_space_test(_Config) ->
    P2Id = oct_background:get_provider_id(paris),
    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),

    SpaceId = oct_background:get_space_id(space_krk),
    {FileType, _FilePath, FileGuid, _ShareId} = api_test_utils:create_shared_file_in_space_krk(),
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),

    DataSpec = set_mode_data_spec(),
    DataSpecWithoutBadValues = DataSpec#data_spec{bad_values = []},

    ValidateRestCallResultFun = fun(_, {ok, RespCode, _RespHeaders, RespBody}) ->
        ExpError = ?REST_ERROR(?ERROR_SPACE_NOT_SUPPORTED_BY(SpaceId, P2Id)),
        ?assertEqual({?HTTP_400_BAD_REQUEST, ExpError}, {RespCode, RespBody})
    end,
    ValidateGsCallResultFun = fun(_, Result) ->
        ?assertEqual(?ERROR_SPACE_NOT_SUPPORTED_BY(SpaceId, P2Id), Result)
    end,

    VerifyFun = fun(_, _) ->
        ?assertMatch(
            {ok, #file_attr{mode = 8#777}},
            file_test_utils:get_attrs(P1Node, FileGuid),
            ?ATTEMPTS
        ),
        true
    end,

    ?assert(onenv_api_test_runner:run_tests([
        #suite_spec{
            target_nodes = [P2Node],
            client_spec = ?CLIENT_SPEC_FOR_SPACE_KRK,
            verify_fun = VerifyFun,
            scenario_templates = [
                #scenario_template{
                    name = <<"Set mode for ", FileType/binary, " on provider not supporting user using /data/ rest endpoint">>,
                    type = rest,
                    prepare_args_fun = build_set_mode_prepare_rest_args_fun(FileObjectId),
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
            data_spec = DataSpecWithoutBadValues
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
-spec build_set_mode_prepare_rest_args_fun(file_meta:path() | file_id:objectid()) ->
    onenv_api_test_runner:prepare_args_fun().
build_set_mode_prepare_rest_args_fun(ValidId) ->
    fun(#api_test_ctx{data = Data0}) ->
        {Id, Data1} = api_test_utils:maybe_substitute_bad_id(
            ValidId, utils:ensure_defined(Data0, #{})
        ),
        RestPath = <<"data/", Id/binary>>,

        #rest_args{
            method = put,
            path = http_utils:append_url_parameters(
                RestPath,
                maps:with([<<"attributes">>], Data1)
            ),
            headers = #{?HDR_CONTENT_TYPE => <<"application/json">>},
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

get_reg_file_distribution_test(Config) ->
    P1Id = oct_background:get_provider_id(krakow),
    [P1Node] = oct_background:get_provider_nodes(krakow),

    P2Id = oct_background:get_provider_id(paris),
    [P2Node] = oct_background:get_provider_nodes(paris),

    SpaceId = oct_background:get_space_id(space_krk_par),
    P1StorageId = get_storage_id(SpaceId, P1Id),
    P2StorageId = get_storage_id(SpaceId, P2Id),

    SpaceOwnerSessIdP1 = oct_background:get_user_session_id(user2, krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user3, krakow),
    UserSessIdP2 = oct_background:get_user_session_id(user3, paris),

    FileType = <<"file">>,
    Filename = ?RANDOM_FILE_NAME(),
    FilePath = filename:join(["/", ?SPACE_KRK_PAR, Filename]),
    FileStorageLocation = filename:join(["/", SpaceId, Filename]),
    {ok, FileGuid} = lfm_test_utils:create_file(FileType, P1Node, UserSessIdP1, FilePath, 8#707),
    {ok, ShareId} = opt_shares:create(P1Node, SpaceOwnerSessIdP1, ?FILE_REF(FileGuid), <<"share">>),

    file_test_utils:await_sync(P2Node, FileGuid),

    lfm_test_utils:write_file(P1Node, UserSessIdP1, FileGuid, 0, {rand_content, 20}),
    ExpDist1 = #data_distribution_gather_result{distribution = #reg_distribution_gather_result{
        distribution_per_provider = #{
            P1Id => #provider_reg_distribution_get_result{
                virtual_size = 20,
                blocks_per_storage = #{P1StorageId => [?BLOCK(0, 20)]},
                locations_per_storage = #{P1StorageId => FileStorageLocation}
            },
            P2Id => #provider_reg_distribution_get_result{
                virtual_size = 20,
                blocks_per_storage = #{P2StorageId => []},
                locations_per_storage = #{P2StorageId => undefined}
            }
        }
    }},
    wait_for_file_location_sync(P2Node, UserSessIdP2, FileGuid, ExpDist1),
    get_distribution_test_base(FileType, FileGuid, ShareId, ExpDist1, Config),

    % Write another block to file on P2 and check returned distribution

    lfm_test_utils:write_file(P2Node, UserSessIdP2, FileGuid, 30, {rand_content, 20}),
    ExpDist2 = #data_distribution_gather_result{distribution = #reg_distribution_gather_result{
        distribution_per_provider = #{
            P1Id => #provider_reg_distribution_get_result{
                virtual_size = 50,
                blocks_per_storage = #{P1StorageId => [?BLOCK(0, 20)]},
                locations_per_storage = #{P1StorageId => FileStorageLocation}
            },
            P2Id => #provider_reg_distribution_get_result{
                virtual_size = 50,
                blocks_per_storage = #{P2StorageId => [?BLOCK(30, 20)]},
                locations_per_storage = #{P2StorageId => FileStorageLocation}
            }
        }
    }},
    wait_for_file_location_sync(P1Node, UserSessIdP1, FileGuid, ExpDist2),
    get_distribution_test_base(FileType, FileGuid, ShareId, ExpDist2, Config).


get_dir_distribution_1_test(Config) ->
    FileType = <<"dir">>,

    disable_dir_stats_collecting_for_space(krakow, space_krk_par),
    disable_dir_stats_collecting_for_space(paris, space_krk_par),

    [P2Node] = oct_background:get_provider_nodes(paris),

    UserSessIdP1 = oct_background:get_user_session_id(user3, krakow),
    UserSessIdP2 = oct_background:get_user_session_id(user3, paris),

    #object{
        guid = DirGuid,
        shares = [ShareId],
        children = [#object{guid = FileGuid}]
    } = onenv_file_test_utils:create_and_sync_file_tree(
        user3, space_krk_par, #dir_spec{
            mode = 8#707,
            shares = [#share_spec{}],
            children = [#file_spec{}]
        }
    ),

    ExpDist = #data_distribution_gather_result{distribution = #dir_distribution_gather_result{
        distribution_per_provider = #{
            oct_background:get_provider_id(krakow) => ?ERROR_DIR_STATS_DISABLED_FOR_SPACE,
            oct_background:get_provider_id(paris) => ?ERROR_DIR_STATS_DISABLED_FOR_SPACE
        }
    }},
    wait_for_file_location_sync(paris, UserSessIdP2, DirGuid, ExpDist),
    get_distribution_test_base(FileType, DirGuid, ShareId, ExpDist, Config),

    % Write to file in dir and assert that dir distribution hasn't changed
    lfm_test_utils:write_file(P2Node, UserSessIdP2, FileGuid, 30, {rand_content, 20}),

    wait_for_file_location_sync(krakow, UserSessIdP1, DirGuid, ExpDist),
    get_distribution_test_base(FileType, DirGuid, ShareId, ExpDist, Config).


get_dir_distribution_2_test(Config) ->
    FileType = <<"dir">>,

    enable_dir_stats_collecting_for_space(krakow, space_krk_par),
    enable_dir_stats_collecting_for_space(paris, space_krk_par),

    SpaceId = oct_background:get_space_id(space_krk_par),
    P1Id = oct_background:get_provider_id(krakow),
    P1StorageId = get_storage_id(SpaceId, P1Id),
    P2Id = oct_background:get_provider_id(paris),
    P2StorageId = get_storage_id(SpaceId, P2Id),

    [P2Node] = oct_background:get_provider_nodes(paris),
    UserSessIdP1 = oct_background:get_user_session_id(user3, krakow),
    UserSessIdP2 = oct_background:get_user_session_id(user3, paris),

    #object{guid = DirGuid, shares = [ShareId]} = onenv_file_test_utils:create_and_sync_file_tree(
        user3, space_krk_par, #dir_spec{mode = 8#707, shares = [#share_spec{}]}
    ),

    ExpDist1 = #data_distribution_gather_result{distribution = #dir_distribution_gather_result{
        distribution_per_provider = #{
            P1Id => #provider_dir_distribution_get_result{
                virtual_size = 0,
                logical_size = 0,
                physical_size_per_storage = #{P1StorageId => 0}
            },
            P2Id => #provider_dir_distribution_get_result{
                virtual_size = 0,
                logical_size = 0,
                physical_size_per_storage = #{P2StorageId => 0}
            }
        }
    }},
    wait_for_file_location_sync(paris, UserSessIdP2, DirGuid, ExpDist1),
    get_distribution_test_base(FileType, DirGuid, ShareId, ExpDist1, Config),

    {ok, FileGuid} = lfm_proxy:create(P2Node, UserSessIdP2, DirGuid, ?RAND_STR(), 8#707),
    lfm_test_utils:write_file(P2Node, UserSessIdP2, FileGuid, 30, {rand_content, 20}),

    ExpDist2 = #data_distribution_gather_result{distribution = #dir_distribution_gather_result{
        distribution_per_provider = #{
            P1Id => #provider_dir_distribution_get_result{
                virtual_size = 50,
                logical_size = 50,
                physical_size_per_storage = #{P1StorageId => 0}
            },
            P2Id => #provider_dir_distribution_get_result{
                virtual_size = 50,
                logical_size = 50,
                physical_size_per_storage = #{P2StorageId => 20}
            }
        }
    }},
    wait_for_file_location_sync(krakow, UserSessIdP1, DirGuid, ExpDist2),
    get_distribution_test_base(FileType, DirGuid, ShareId, ExpDist2, Config).


get_dir_distribution_3_test(Config) ->
    FileType = <<"dir">>,

    enable_dir_stats_collecting_for_space(krakow, space_krk_par),
    disable_dir_stats_collecting_for_space(paris, space_krk_par),

    SpaceId = oct_background:get_space_id(space_krk_par),
    P1Id = oct_background:get_provider_id(krakow),
    P1StorageId = get_storage_id(SpaceId, P1Id),
    P2Id = oct_background:get_provider_id(paris),

    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),

    UserSessIdP1 = oct_background:get_user_session_id(user3, krakow),
    UserSessIdP2 = oct_background:get_user_session_id(user3, paris),

    #object{guid = DirGuid, shares = [ShareId]} = onenv_file_test_utils:create_and_sync_file_tree(
        user3, space_krk_par, #dir_spec{mode = 8#707, shares = [#share_spec{}]}
    ),

    ExpDist1 = #data_distribution_gather_result{distribution = #dir_distribution_gather_result{
        distribution_per_provider = #{
            P1Id => #provider_dir_distribution_get_result{
                virtual_size = 0,
                logical_size = 0,
                physical_size_per_storage = #{P1StorageId => 0}
            },
            P2Id => ?ERROR_DIR_STATS_DISABLED_FOR_SPACE
        }
    }},
    wait_for_file_location_sync(paris, UserSessIdP2, DirGuid, ExpDist1),
    get_distribution_test_base(FileType, DirGuid, ShareId, ExpDist1, Config),

    #object{guid = FileGuid} = onenv_file_test_utils:create_and_sync_file_tree(
        user3, DirGuid, #file_spec{}
    ),
    lfm_test_utils:write_file(P1Node, UserSessIdP1, FileGuid, 5, {rand_content, 10}),
    lfm_test_utils:write_file(P2Node, UserSessIdP2, FileGuid, 30, {rand_content, 20}),

    ExpDist2 = #data_distribution_gather_result{distribution = #dir_distribution_gather_result{
        distribution_per_provider = #{
            P1Id => #provider_dir_distribution_get_result{
                virtual_size = 50,
                logical_size = 50,
                physical_size_per_storage = #{P1StorageId => 10}
            },
            P2Id => ?ERROR_DIR_STATS_DISABLED_FOR_SPACE
        }
    }},
    wait_for_file_location_sync(krakow, UserSessIdP1, DirGuid, ExpDist2),
    get_distribution_test_base(FileType, DirGuid, ShareId, ExpDist2, Config).


get_symlink_distribution_test(Config) ->
    FileType = <<"sym">>,
    
    SpaceId = oct_background:get_space_id(space_krk_par),
    P1Id = oct_background:get_provider_id(krakow),
    P1StorageId = get_storage_id(SpaceId, P1Id),
    P2Id = oct_background:get_provider_id(paris),
    P2StorageId = get_storage_id(SpaceId, P2Id),
    
    #object{guid = SymGuid, shares = [ShareId]} = onenv_file_test_utils:create_and_sync_file_tree(
        user3, space_krk_par, #symlink_spec{symlink_value = <<"abcd">>, shares = [#share_spec{}]}
    ),
    
    ExpDist = #data_distribution_gather_result{distribution = #symlink_distribution_get_result{
        virtual_size = 0,
        storages_per_provider = #{
            P1Id => [P1StorageId],
            P2Id => [P2StorageId]
        }
    }},
    ClientSpec = #client_spec{
        correct = [
            user2, % space owner - doesn't need any perms
            user3, % files owner (see fun create_shared_file/1)
            user4  % any user is allowed to see symlinks distribution (as symlink always has 777 posix perms)
        ],
        unauthorized = [nobody],
        forbidden_not_in_space = [user1]
    },
    get_distribution_test_base(FileType, SymGuid, ShareId, ExpDist, Config, ClientSpec).

%% @private
-spec enable_dir_stats_collecting_for_space(
    oct_background:entity_selector(),
    oct_background:entity_selector()
) ->
    ok.
enable_dir_stats_collecting_for_space(ProviderPlaceholder, SpacePlaceholder) ->
    SpaceId = oct_background:get_space_id(SpacePlaceholder),
    ?rpc(ProviderPlaceholder, space_logic:update_support_parameters(SpaceId, #support_parameters{
        dir_stats_service_enabled = true
    })),
    await_dir_stats_collecting_status(ProviderPlaceholder, SpaceId, enabled).


%% @private
-spec disable_dir_stats_collecting_for_space(
    oct_background:entity_selector(),
    oct_background:entity_selector()
) ->
    ok.
disable_dir_stats_collecting_for_space(ProviderPlaceholder, SpacePlaceholder) ->
    SpaceId = oct_background:get_space_id(SpacePlaceholder),
    ?rpc(ProviderPlaceholder, space_logic:update_support_parameters(SpaceId, #support_parameters{
        dir_stats_service_enabled = false
    })),
    await_dir_stats_collecting_status(ProviderPlaceholder, SpaceId, disabled).


%% @private
-spec await_dir_stats_collecting_status(
    oct_background:entity_selector(),
    od_space:id(),
    dir_stats_collector_config:extended_collecting_status()
) ->
    ok.
await_dir_stats_collecting_status(ProviderPlaceholder, SpaceId, Status) ->
    ?assertEqual(
        Status,
        ?rpc(ProviderPlaceholder, dir_stats_service_state:get_extended_status(SpaceId)),
        ?ATTEMPTS
    ).


-spec await_file_size_sync(oct_background:entity_selector(), file_meta:size(), file_id:file_guid()) -> ok.
await_file_size_sync(ProviderPlaceholder, ExpectedSize, Guid) ->
    Node = oct_background:get_random_provider_node(ProviderPlaceholder),
    SessId = oct_background:get_user_session_id(user3, ProviderPlaceholder),
    ?assertMatch({ok, #file_attr{size = ExpectedSize}}, lfm_proxy:stat(Node, SessId, ?FILE_REF(Guid)), ?ATTEMPTS),
    ok.


%% @private
-spec await_dir_size_sync(
    oct_background:entity_selector() | [oct_background:entity_selector()],
    file_meta:size(),
    file_id:file_guid()
) ->
    ok.
await_dir_size_sync(ProviderPlaceholders, ExpectedSize, DirGuid) ->
    lists:foreach(fun(ProviderPlaceholder) ->
        ?assertEqual(
            ExpectedSize,
            gather_historical_dir_size_stats(DirGuid, ProviderPlaceholder),
            ?ATTEMPTS
        )
    end, utils:ensure_list(ProviderPlaceholders)).


%% @private
-spec get_storage_id(od_space:id(), oneprovider:id()) -> od_storage:id().
get_storage_id(SpaceId, ProviderId) ->
    {ok, Storages} = ?rpc(krakow, space_logic:get_provider_storages(SpaceId, ProviderId)),
    hd(maps:keys(Storages)).


%% @private
wait_for_file_location_sync(ProviderSelector, SessId, FileGuid, ExpDistribution) ->
    ?assertEqual(
        ExpDistribution,
        ?rpc(ProviderSelector, mi_file_metadata:gather_distribution(SessId, ?FILE_REF(FileGuid))),
        ?ATTEMPTS
    ).



%% @private
get_distribution_test_base(FileType, FileGuid, ShareId, ExpDistribution, Config) ->
    get_distribution_test_base(FileType, FileGuid, ShareId, ExpDistribution, Config, ?CLIENT_SPEC_FOR_SPACE_KRK_PAR).


%% @private
get_distribution_test_base(FileType, FileGuid, ShareId, ExpDistribution, Config, ClientSpec) ->
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),

    ExpRestDistribution = opw_test_rpc:call(krakow, data_distribution_translator, gather_result_to_json, [rest, ExpDistribution, FileGuid]),
    ValidateRestSuccessfulCallFun = fun(_TestCtx, {ok, RespCode, _RespHeaders, RespBody}) ->
        ?assertEqual({?HTTP_200_OK, ExpRestDistribution}, {RespCode, RespBody})
    end,
    
    ExpGsDistribution = opw_test_rpc:call(krakow, data_distribution_translator, gather_result_to_json, [gs, ExpDistribution, FileGuid]),
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

    ?assert(onenv_api_test_runner:run_tests([
        #suite_spec{
            target_nodes = ?config(op_worker_nodes, Config),
            client_spec = ClientSpec,
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
                }
            ],
            data_spec = api_test_utils:replace_enoent_with_error_not_found_in_error_expectations(
                api_test_utils:add_file_id_errors_for_operations_not_available_in_share_mode(
                    FileGuid, ShareId, undefined
                )
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

%%%===================================================================
%%% Gather historical dir size stats test functions
%%%===================================================================

get_historical_dir_size_stats_schema_test(Config) ->
    ?assert(onenv_api_test_runner:run_tests([
        #suite_spec{
            target_nodes = ?config(op_worker_nodes, Config),
            client_spec = ?CLIENT_SPEC_FOR_PUBLIC_ACCESS_SCENARIOS,
            scenario_templates = [
                #scenario_template{
                    name = <<"Get dir size stats collection schema using op_file gs api">>,
                    type = gs,
                    prepare_args_fun = fun(_) ->
                        #gs_args{
                            operation = get,
                            gri = #gri{type = op_file, id = undefined, aspect = dir_size_stats_collection_schema, scope = public}
                        }
                    end,
                    validate_result_fun = fun(_TestCtx, {ok, Result}) ->
                        ?assertEqual(
                            jsonable_record:from_json(Result, time_series_collection_schema),
                            ?DIR_SIZE_STATS_COLLECTION_SCHEMA
                        )
                    end
                }
            ]
        }
    ])).


get_historical_dir_size_stats_layout_test(Config) ->
    enable_dir_stats_collecting_for_space(krakow, space_krk_par),
    enable_dir_stats_collecting_for_space(paris, space_krk_par),

    SpaceId = oct_background:get_space_id(space_krk_par),
    P1Id = oct_background:get_provider_id(krakow),
    P1StorageId = get_storage_id(SpaceId, P1Id),
    P2Id = oct_background:get_provider_id(paris),
    P2StorageId = get_storage_id(SpaceId, P2Id),

    [#object{guid = DirGuid, shares = [ShareId]}, #object{guid = FileGuid}] =
        onenv_file_test_utils:create_and_sync_file_tree(
            user3, space_krk_par, [
                #dir_spec{
                    mode = 8#707,
                    shares = [#share_spec{}],
                    children = [#file_spec{content = crypto:strong_rand_bytes(8)}]
                },
                #file_spec{content = crypto:strong_rand_bytes(8)}
            ]
    ),
    Metrics = [?DAY_METRIC, ?HOUR_METRIC, ?MINUTE_METRIC, ?MONTH_METRIC],
    ExpLayoutBase = #{
        ?DIR_COUNT => Metrics,
        ?REG_FILE_AND_LINK_COUNT => Metrics,
        ?VIRTUAL_SIZE => Metrics,
        ?LOGICAL_SIZE => Metrics,
        ?FILE_ERROR_COUNT => Metrics,
        ?DIR_ERROR_COUNT => Metrics
    },
    
    ExpLayoutFun = fun
        (krakow) -> ExpLayoutBase#{?PHYSICAL_SIZE(P1StorageId) => Metrics};
        (paris) -> ExpLayoutBase#{?PHYSICAL_SIZE(P2StorageId) => Metrics}
    end,

    await_file_size_sync(krakow, 8, DirGuid),
    await_file_size_sync(paris, 8, DirGuid),
    
    MemRef = api_test_memory:init(),

    ValidateGsSuccessfulCallFun = fun(_TestCtx, Result) ->
        Provider = api_test_memory:get(MemRef, current_provider),
        ?assertEqual({ok, #{<<"layout">> => ExpLayoutFun(Provider)}}, Result)
    end,
    ValidateRestSuccessfulCallFun = fun(TestCtx, {ok, RespCode, _RespHeaders, RespBody}) ->
        ProviderId = opw_test_rpc:get_provider_id(TestCtx#api_test_ctx.node),
        Provider = oct_background:to_entity_placeholder(ProviderId),
        ?assertEqual(?HTTP_200_OK, RespCode),
        ?assertEqual(#{<<"layout">> => ExpLayoutFun(Provider)}, RespBody)
    end,
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),
    DataSpecFun = fun(_) -> #data_spec{
        optional = [<<"mode">>],
        correct_values = #{<<"mode">> => [<<"layout">>]},
        bad_values = [
            {<<"mode">>, mode, ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"mode">>, [<<"layout">>, <<"slice">>])},
            {bad_id, FileObjectId, {rest, ?ERROR_POSIX(?ENOTDIR)}},
            {bad_id, FileGuid, {gs, ?ERROR_POSIX(?ENOTDIR)}}
        ]}
    end,
    gather_historical_dir_size_stats_test_base(
        MemRef, DirGuid, ShareId, ValidateGsSuccessfulCallFun, ValidateRestSuccessfulCallFun, Config, DataSpecFun, #{}).


get_historical_dir_size_stats_slice_test(Config) ->
    CurrentTimestamp = 1689168600,
    FutureTimestamp = 1689169900,
    PastTimestamp = 1687380000,

    enable_dir_stats_collecting_for_space(krakow, space_krk_par),
    enable_dir_stats_collecting_for_space(paris, space_krk_par),
    
    SpaceId = oct_background:get_space_id(space_krk_par),
    P1Id = oct_background:get_provider_id(krakow),
    P1StorageId = get_storage_id(SpaceId, P1Id),
    P2Id = oct_background:get_provider_id(paris),
    P2StorageId = get_storage_id(SpaceId, P2Id),

    time_test_utils:set_current_time_seconds(CurrentTimestamp),
    [#object{guid = DirGuid, shares = [ShareId]}, #object{guid = FileGuid}] =
        onenv_file_test_utils:create_and_sync_file_tree(
            user3, space_krk_par, [
                #dir_spec{
                    mode = 8#707,
                    shares = [#share_spec{}],
                    children = [
                        #file_spec{content = crypto:strong_rand_bytes(8)},
                        #dir_spec{}
                    ]
                },
                #file_spec{content = crypto:strong_rand_bytes(8)}
            ],
            krakow
    ),
    await_dir_size_sync([krakow, paris], 8, DirGuid),

    time_test_utils:simulate_seconds_passing(100),
    onenv_file_test_utils:create_and_sync_file_tree(
        user3, DirGuid, #file_spec{content = crypto:strong_rand_bytes(16)}, krakow
    ),
    await_dir_size_sync([krakow, paris], 24, DirGuid),

    time_test_utils:simulate_seconds_passing(180),
    onenv_file_test_utils:create_and_sync_file_tree(
        user3, DirGuid, #file_spec{content = crypto:strong_rand_bytes(8)}, krakow
    ),
    await_dir_size_sync([krakow, paris], 32, DirGuid),

    Metrics = lists_utils:random_sublist([?DAY_METRIC, ?HOUR_METRIC, ?MINUTE_METRIC, ?MONTH_METRIC]),
    BaseLayout = maps_utils:random_submap(#{
        ?DIR_COUNT => Metrics,
        ?REG_FILE_AND_LINK_COUNT => Metrics,
        ?VIRTUAL_SIZE => Metrics,
        ?LOGICAL_SIZE => Metrics
    }),
    LayoutFun = fun
        (krakow) -> BaseLayout#{?PHYSICAL_SIZE(P1StorageId) => Metrics};
        (paris) -> BaseLayout#{?PHYSICAL_SIZE(P2StorageId) => Metrics}
    end,
    ExpTimestampFilter = fun(Data, Windows) ->
        StartTimestamp = maps:get(<<"startTimestamp">>, Data, FutureTimestamp),
        StopTimestamp = maps:get(<<"stopTimestamp">>, Data, PastTimestamp),
        lists:sublist(lists:filter(fun(Window) ->
            Timestamp = maps:get(<<"timestamp">>, Window),
            Timestamp =< StartTimestamp andalso Timestamp >= StopTimestamp
        end, Windows), maps:get(<<"windowLimit">>, Data, ?MAX_WINDOW_LIMIT))
    end,
    BuildExpMetricsFun = fun(Data, ExpValues) ->
        [Last, Middle, First] = ExpValues,
        lists:foldl(fun(Metric, Acc) ->
            Acc#{Metric =>
            case Metric of
                ?MINUTE_METRIC ->
                    ExpTimestampFilter(Data, [
                        #{ <<"timestamp">> => 1689168840, <<"value">> => Last},
                        #{ <<"timestamp">> => 1689168660, <<"value">> => Middle},
                        #{ <<"timestamp">> => 1689168600, <<"value">> => First}
                    ]);
                ?HOUR_METRIC -> ExpTimestampFilter(Data, [#{ <<"timestamp">> => 1689166800, <<"value">> => Last}]);
                ?DAY_METRIC -> ExpTimestampFilter(Data, [#{ <<"timestamp">> => 1689120000, <<"value">> => Last}]);
                ?MONTH_METRIC -> ExpTimestampFilter(Data, [#{ <<"timestamp">> => 1687392000, <<"value">> => Last}])
            end
            }
        end, #{}, Metrics)
    end,
    BuildExpSliceFun = fun(Data) -> maps:with(maps:keys(maps:get(<<"layout">>, Data, #{})), #{
            ?DIR_COUNT => BuildExpMetricsFun(Data, [1, 1, 1]),
            ?REG_FILE_AND_LINK_COUNT =>  BuildExpMetricsFun(Data, [3, 2, 1]),
            ?PHYSICAL_SIZE(P1StorageId) =>  BuildExpMetricsFun(Data, [32, 24, 8]),
            ?PHYSICAL_SIZE(P2StorageId) =>  BuildExpMetricsFun(Data, [0, 0, 0]),
            ?VIRTUAL_SIZE => BuildExpMetricsFun(Data, [32, 24, 8]),
            ?LOGICAL_SIZE => BuildExpMetricsFun(Data, [32, 24, 8])
        })
    end,
    await_file_size_sync(krakow, 32, DirGuid),
    await_file_size_sync(paris, 32, DirGuid),
    
    MemRef = api_test_memory:init(),

    ValidateSliceFun = fun(Slice, Data) ->
        ResultWithoutMeasurementTimestamps = tsc_structure:map(fun(_TimeSeriesName, _MetricsName, Windows) ->
            lists:map(fun(Map) ->
                maps:without([<<"firstMeasurementTimestamp">>, <<"lastMeasurementTimestamp">>], Map)
            end, Windows)
        end, Slice),
        ?assertEqual(BuildExpSliceFun(Data), ResultWithoutMeasurementTimestamps)
    end,

    ValidateGsSuccessfulCallFun = fun(TestCtx, Result) ->
        {ok, ResultData} = ?assertMatch({ok, #{<<"slice">> := _}}, Result),
        ValidateSliceFun(maps:get(<<"slice">> , ResultData), TestCtx#api_test_ctx.data)
    end,
    ValidateRestSuccessfulCallFun =  fun(TestCtx, {ok, RespCode, _RespHeaders, RespBody}) ->
        ?assertEqual(?HTTP_200_OK, RespCode),
        ResultData = ?assertMatch(#{<<"slice">> := _}, RespBody),
        ValidateSliceFun(maps:get(<<"slice">> , ResultData), TestCtx#api_test_ctx.data)
    end,
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),
    DataSpecFun = fun(ProviderId) -> #data_spec{
        required = [<<"layout">>],
        optional = [
            <<"mode">>,
            <<"windowLimit">>,
            <<"startTimestamp">>,
            <<"stopTimestamp">>
        ],
        correct_values = #{
            <<"mode">> => [<<"slice">>],
            <<"layout">> => [LayoutFun(ProviderId)],
            <<"windowLimit">> => [2, 5],
            <<"startTimestamp">> => [1689166750, 1689166800, 1689168840, 1689166900],
            <<"stopTimestamp">> => [1687392000, 1687380000, 1689168660, 1689166650]
        },
        bad_values = [
            {<<"mode">>, mode, ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"mode">>, [<<"layout">>, <<"slice">>])},
            {<<"layout">>, 8, {?ERROR_BAD_VALUE_JSON(<<"layout">>)}},
            {<<"windowLimit">>, 0, {?ERROR_BAD_VALUE_NOT_IN_RANGE(<<"windowLimit">>, 1, 1000)}},
            {<<"windowLimit">>, 8888, {?ERROR_BAD_VALUE_NOT_IN_RANGE(<<"windowLimit">>, 1, 1000)}},
            {<<"windowLimit">>, atom, {?ERROR_BAD_VALUE_INTEGER(<<"windowLimit">>)}},
            {<<"startTimestamp">>, -1, {?ERROR_BAD_VALUE_TOO_LOW(<<"startTimestamp">>, 0)}},
            {<<"startTimestamp">>, atom, {?ERROR_BAD_VALUE_INTEGER(<<"startTimestamp">>)}},
            {bad_id, FileObjectId, {rest, ?ERROR_POSIX(?ENOTDIR)}},
            {bad_id, FileGuid, {gs, ?ERROR_POSIX(?ENOTDIR)}}
        ]
    }
    end,
    gather_historical_dir_size_stats_test_base(MemRef, DirGuid, ShareId, ValidateGsSuccessfulCallFun,
        ValidateRestSuccessfulCallFun, Config, DataSpecFun,
        #{<<"mode">> => <<"slice">>} % add mode to data, when it is left out, as when it is omitted `layout` mode is assumed
    ).


get_historical_dir_size_stats_disabled_test(Config) ->
    disable_dir_stats_collecting_for_space(krakow, space_krk_par),
    disable_dir_stats_collecting_for_space(paris, space_krk_par),

    [#object{guid = DirGuid, shares = [ShareId]}] =
        onenv_file_test_utils:create_and_sync_file_tree(
            user3, space_krk_par, [
                #dir_spec{
                    mode = 8#707,
                    shares = [#share_spec{}],
                    children = [#file_spec{content = crypto:strong_rand_bytes(8)}]
                }
            ]
        ),
    MemRef = api_test_memory:init(),

    Metrics = lists_utils:random_sublist([?DAY_METRIC, ?HOUR_METRIC, ?MINUTE_METRIC, ?MONTH_METRIC]),
    BaseLayout = maps_utils:random_submap(#{
        ?DIR_COUNT => Metrics,
        ?REG_FILE_AND_LINK_COUNT => Metrics,
        ?VIRTUAL_SIZE => Metrics
    }),

    ValidateGsErrorCallFun = fun(_TestCtx, Result) ->
        ?assertEqual(?ERROR_DIR_STATS_DISABLED_FOR_SPACE, Result)
    end,
    ValidateRestErrorCallFun = fun(_TestCtx, {ok, RespCode, _RespHeaders, RespBody}) ->
        ?assertEqual(?HTTP_400_BAD_REQUEST, RespCode),
        ?assertEqual(#{<<"error">> => errors:to_json(?ERROR_DIR_STATS_DISABLED_FOR_SPACE)}, RespBody)
    end,

    DataSpecFunLayout = fun(_) -> #data_spec{
        optional = [<<"mode">>],
        correct_values = #{<<"mode">> => [<<"layout">>]}
    } end,

    DataSpecFunSlice = fun(_) -> #data_spec{
        required = [<<"layout">>],
        correct_values = #{
            <<"layout">> => [BaseLayout]
        }
    } end,

    gather_historical_dir_size_stats_test_base(
        MemRef, DirGuid, ShareId, ValidateGsErrorCallFun, ValidateRestErrorCallFun, Config, DataSpecFunLayout, #{}
    ),

    gather_historical_dir_size_stats_test_base(MemRef, DirGuid, ShareId, ValidateGsErrorCallFun,
        ValidateRestErrorCallFun,  Config, DataSpecFunSlice,
        #{<<"mode">> => <<"slice">>} % add mode to data, when it is left out, as when it is omitted `layout` mode is assumed
    ).


%% @private
gather_historical_dir_size_stats_test_base(
    MemRef, FileGuid, ShareId, ValidateGsSuccessfulCallFun,
    ValidateRestSuccessfulCallFun, Config, DataSpecFun, DefaultData
) ->
    lists:foreach(fun(Provider) ->
        api_test_memory:set(MemRef, current_provider, Provider),
        DataSpec = api_test_utils:replace_enoent_with_error_not_found_in_error_expectations(
            api_test_utils:add_file_id_errors_for_operations_not_available_in_share_mode(
                FileGuid, ShareId, DataSpecFun(Provider)
            )
        ),

        ?assert(onenv_api_test_runner:run_tests([
            #suite_spec{
                target_nodes = ?config(op_worker_nodes, Config),
                client_spec = ?CLIENT_SPEC_FOR_SPACE_KRK_PAR,
                scenario_templates = [
                    #scenario_template{
                        name = <<"Get dir size stats using op_file gs api">>,
                        type = gs,
                        prepare_args_fun = build_gather_historical_dir_size_stats_prepare_gs_args_fun(FileGuid, Provider, DefaultData),
                        validate_result_fun = ValidateGsSuccessfulCallFun
                    }
                ],
                data_spec = DataSpec
            }
        ])),

        ?assert(onenv_api_test_runner:run_tests([
            #suite_spec{
                target_nodes = [Provider],
                client_spec = ?CLIENT_SPEC_FOR_SPACE_KRK_PAR,
                scenario_templates = [
                    #scenario_template{
                        name = <<"Get dir size stats using rest endpoint">>,
                        type = rest,
                        prepare_args_fun = build_gather_historical_dir_size_stats_prepare_rest_args_fun(FileGuid, DefaultData),
                        validate_result_fun = ValidateRestSuccessfulCallFun
                    }
                ],
                data_spec = DataSpec
            }
        ]))
    end, [krakow, paris]).


%% @private
-spec build_gather_historical_dir_size_stats_prepare_gs_args_fun(file_id:file_guid(), oct_background:entity_selector() ,map()) ->
    onenv_api_test_runner:prepare_args_fun().
build_gather_historical_dir_size_stats_prepare_gs_args_fun(FileGuid, Provider, DefaultData) ->
    fun(#api_test_ctx{data = Data}) ->
        {GriId, Data2} = api_test_utils:maybe_substitute_bad_id(FileGuid, Data),

        #gs_args{
            operation = get,
            gri = #gri{type = op_file, id = GriId, aspect = {dir_size_stats_collection, oct_background:get_provider_id(Provider)}},
            data = maps:merge(DefaultData, Data2)
        }
    end.


%% @private
-spec build_gather_historical_dir_size_stats_prepare_rest_args_fun(file_id:file_guid(), map()) ->
    onenv_api_test_runner:prepare_args_fun().
build_gather_historical_dir_size_stats_prepare_rest_args_fun(Guid, DefaultData) ->
    fun(#api_test_ctx{data = Data}) ->
        {ok, ObjectId} = file_id:guid_to_objectid(Guid),
        {FinalObjectId, Data2} = api_test_utils:maybe_substitute_bad_id(ObjectId, Data),
        RestPath = <<"data/", FinalObjectId/binary, "/dir_size_stats">>,

        #rest_args{
            method = get,
            path = RestPath,
            headers = #{?HDR_CONTENT_TYPE => <<"application/json">>},
            body = json_utils:encode(maps:merge(DefaultData, Data2))
        }
    end.

%%%===================================================================
%%% Get file storage locations test functions
%%%===================================================================

get_reg_file_storage_locations_test_posix(Config) ->
    get_reg_file_storage_locations_test(Config, posix).


get_reg_file_storage_locations_test_s3(Config) ->
    get_reg_file_storage_locations_test(Config, s3).


get_reg_file_storage_locations_test(Config, StorageType) ->
    P1Id = oct_background:get_provider_id(krakow),
    [P1Node] = oct_background:get_provider_nodes(krakow),
    
    P2Id = oct_background:get_provider_id(paris),
    [P2Node] = oct_background:get_provider_nodes(paris),
    
    SpaceId = case StorageType of
        posix -> oct_background:get_space_id(space_krk_par);
        s3 -> oct_background:get_space_id(space_s3)
    end,
    P1StorageId = get_storage_id(SpaceId, P1Id),
    P2StorageId = get_storage_id(SpaceId, P2Id),
    
    UserSessIdP1 = oct_background:get_user_session_id(user3, krakow),
    UserSessIdP2 = oct_background:get_user_session_id(user3, paris),
    
    #object{guid = FileGuid, shares = [ShareId]} = onenv_file_test_utils:create_and_sync_file_tree(
        user3, fslogic_file_id:spaceid_to_space_dir_guid(SpaceId), #file_spec{
            shares = [#share_spec{}],
            mode = 8#707,
            content = crypto:strong_rand_bytes(20)
        }, krakow
    ),
    FileUuid = file_id:guid_to_uuid(FileGuid),
    
    {ok, FilePath} = lfm_proxy:get_file_path(P1Node, UserSessIdP1, FileGuid),
    [_Sep, _SpaceName | PathTokens] = filename:split(FilePath),
    FileStoragePath = case StorageType of
        posix -> 
            filename:join([<<"/">>, SpaceId | PathTokens]);
        s3 -> 
            <<A:1/binary, B:1/binary, C:1/binary, _/binary>> = FileUuid,
            filename:join([<<"/">>, SpaceId, A, B, C, FileUuid])
    end,
    ExpResult1 = #{
        <<"locationsPerProvider">> => #{
            P1Id => #{
                <<"locationsPerStorage">> => #{
                    P1StorageId => FileStoragePath
                },
                <<"success">> => true
            },
            P2Id => #{
                <<"locationsPerStorage">> => #{
                    P2StorageId => null
                },
                <<"success">> => true
            }
        }
    },
    assert_file_location_created(P2Node, FileUuid, P1Id),
    get_storage_locations_test_base(FileGuid, ShareId, ExpResult1, Config),
    
    % Read file on the other provider and check file storage locations again.
    
    lfm_test_utils:read_file(P2Node, UserSessIdP2, FileGuid, 20),
    assert_file_location_created(P1Node, FileUuid, P2Id),
    ExpResult2 = #{
        <<"locationsPerProvider">> => #{
            P1Id => #{
                <<"locationsPerStorage">> => #{
                    P1StorageId => FileStoragePath
                },
                <<"success">> => true
            },
            P2Id => #{
                <<"locationsPerStorage">> => #{
                    P2StorageId => FileStoragePath
                },
                <<"success">> => true
            }
        }
    },
    get_storage_locations_test_base(FileGuid, ShareId, ExpResult2, Config).


get_storage_locations_test_base(FileGuid, ShareId, ExpResult, Config) ->
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),
    
    ValidateRestSuccessfulCallFun = fun(_TestCtx, {ok, RespCode, _RespHeaders, RespBody}) ->
        ?assertEqual({?HTTP_200_OK, ExpResult}, {RespCode, RespBody})
    end,
    
    ValidateGsSuccessfulCallFun = fun(_TestCtx, Result) ->
        ExpGsResponse = ExpResult#{
            <<"gri">> => gri:serialize(#gri{
                type = op_file, id = FileGuid, aspect = storage_locations, scope = private
            }),
            <<"revision">> => 1
        },
        ?assertEqual({ok, ExpGsResponse}, Result)
    end,
    
    ?assert(onenv_api_test_runner:run_tests([
        #suite_spec{
            target_nodes = ?config(op_worker_nodes, Config),
            client_spec = ?CLIENT_SPEC_FOR_SPACE_KRK_PAR,
            scenario_templates = [
                #scenario_template{
                    name = <<"Get file storage locations using /data/FileId/storage_locations rest endpoint">>,
                    type = rest,
                    prepare_args_fun = build_get_storage_locations_prepare_rest_args_fun(FileObjectId),
                    validate_result_fun = ValidateRestSuccessfulCallFun
                },
                #scenario_template{
                    name = <<"Get file storage locations using op_file gs api">>,
                    type = gs,
                    prepare_args_fun = build_get_storage_locations_prepare_gs_args_fun(FileGuid, private),
                    validate_result_fun = ValidateGsSuccessfulCallFun
                }
            ],
            data_spec = api_test_utils:replace_enoent_with_error_not_found_in_error_expectations(
                api_test_utils:add_file_id_errors_for_operations_not_available_in_share_mode(
                    FileGuid, ShareId, undefined
                )
            )
        }
    ])).


%% @private
-spec build_get_storage_locations_prepare_rest_args_fun(file_id:objectid()) ->
    onenv_api_test_runner:prepare_args_fun().
build_get_storage_locations_prepare_rest_args_fun(FileObjectId) ->
    fun(#api_test_ctx{data = Data}) ->
        {Id, _} = api_test_utils:maybe_substitute_bad_id(FileObjectId, Data),
        
        #rest_args{
            method = get,
            path = <<"data/", Id/binary, "/storage_locations">>
        }
    end.


%% @private
-spec build_get_storage_locations_prepare_gs_args_fun(file_id:file_guid(), gri:scope()) ->
    onenv_api_test_runner:prepare_args_fun().
build_get_storage_locations_prepare_gs_args_fun(FileGuid, Scope) ->
    fun(#api_test_ctx{data = Data}) ->
        {GriId, _} = api_test_utils:maybe_substitute_bad_id(FileGuid, Data),
        
        #gs_args{
            operation = get,
            gri = #gri{type = op_file, id = GriId, aspect = storage_locations, scope = Scope}
        }
    end.


%% @private
-spec assert_file_location_created(node(), file_id:file_meta_uuid(), oneprovider:id()) -> 
    {ok, file_location:doc()} | no_return().
assert_file_location_created(Node, FileUuid, LocationProviderId) ->
    ?assertMatch({ok, _}, opw_test_rpc:call(Node, fslogic_location_cache, get_location, [
        file_location:id(FileUuid, LocationProviderId), FileUuid, false
    ]), ?ATTEMPTS).


%% @private
-spec gather_historical_dir_size_stats(dir_id:dir_guid(), oneprovider:id()) ->
    window_info:value().
gather_historical_dir_size_stats(DirGuid, ProviderPlaceholder) ->
    SessionId = oct_background:get_user_session_id(user3, ProviderPlaceholder),
    ProviderId = oct_background:get_provider_id(ProviderPlaceholder),
    Request = #time_series_slice_get_request{
        layout = #{?VIRTUAL_SIZE => [?HOUR_METRIC]}
    },
    #time_series_slice_get_result{
        slice = #{?VIRTUAL_SIZE := #{
            ?HOUR_METRIC := [#window_info{value = Value}]
        }}
    } = ?rpc(ProviderPlaceholder, mi_file_metadata:get_historical_dir_size_stats(
        SessionId, ?FILE_REF(DirGuid), ProviderId, Request)
    ),
    Value.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    oct_background:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "api_tests",
        envs = [{op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}],
        posthook = fun(NewConfig) ->
            User3Id = oct_background:get_user_id(user3),
            lists:foreach(fun(SpacePlaceholder) ->
                SpaceId = oct_background:get_space_id(SpacePlaceholder),
                ozt_spaces:set_privileges(SpaceId, User3Id, [
                    ?SPACE_MANAGE_SHARES | privileges:space_member()
                ])
            end, [space_krk_par, space_s3]),
            NewConfig
        end
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_group(_Group, Config) ->
    lfm_proxy:init(Config, false).


end_per_group(_Group, Config) ->
    lfm_proxy:teardown(Config).

init_per_testcase(get_historical_dir_size_stats_slice_test=Case, Config) ->
    time_test_utils:freeze_time(Config),
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 10}),
    Config.

end_per_testcase(get_historical_dir_size_stats_slice_test=Case, Config) ->
    ok = time_test_utils:unfreeze_time(Config),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(_Case, _Config) ->
    ok.
