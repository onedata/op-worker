%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020-2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests concerning share basic API (REST + gs).
%%% @end
%%%-------------------------------------------------------------------
-module(api_share_test_SUITE).
-author("Bartosz Walkowicz").

-include("api_file_test_utils.hrl").
-include("api_test_runner.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("onenv_test_utils.hrl").
-include_lib("ctool/include/graph_sync/gri.hrl").
-include_lib("ctool/include/http/codes.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/privileges.hrl").

-export([
    groups/0, all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_group/2, end_per_group/2,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    create_share_test/1,
    get_share_test/1,
    update_share_test/1,
    delete_share_test/1
]).

groups() -> [
    {all_tests, [parallel], [
        create_share_test,
        get_share_test,
        update_share_test,
        delete_share_test
    ]}
].

all() -> [
    {group, all_tests}
].


-define(ATTEMPTS, 30).


%%%===================================================================
%%% Get file distribution test functions
%%%===================================================================


create_share_test(_Config) ->
    Providers = [krakow, paris],
    SpaceId = oct_background:get_space_id(space_krk_par),

    {FileType, FileSpec} = generate_random_file_spec(),
    FileInfo = onenv_file_test_utils:create_and_sync_file_tree(user3, SpaceId, FileSpec),
    FileGuid = FileInfo#object.guid,
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),

    MemRef = api_test_memory:init(),

    ?assert(onenv_api_test_runner:run_tests([
        #suite_spec{
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SPACE_KRK_PAR(?EPERM),
            verify_fun = build_verify_file_shares_fun(MemRef, Providers, user3, FileGuid),
            scenario_templates = [
                #scenario_template{
                    name = <<"Create share for ", FileType/binary, " using /shares rest endpoint">>,
                    type = rest,
                    prepare_args_fun = fun create_share_prepare_rest_args_fun/1,
                    validate_result_fun = build_create_share_validate_rest_call_result_fun(
                        MemRef, Providers, FileType, SpaceId
                    )
                },
                #scenario_template{
                    name = <<"Create share for ", FileType/binary, " using gs api">>,
                    type = gs,
                    prepare_args_fun = fun create_share_prepare_gs_args_fun/1,
                    validate_result_fun = build_create_share_validate_gs_call_result_fun(
                        MemRef, Providers, FileType, SpaceId
                    )
                }
            ],
            randomly_select_scenarios = true,
            data_spec = api_test_utils:add_cdmi_id_errors_for_operations_not_available_in_share_mode(
                % Operations should be rejected even before checking if share exists
                % (in case of using share file id) so it is not necessary to use
                % valid share id
                <<"rootFileId">>, FileGuid, SpaceId, <<"NonExistentShare">>, #data_spec{
                    required = [<<"name">>],
                    at_least_one = [<<"fileId">>, <<"rootFileId">>],
                    optional = [<<"description">>],
                    correct_values = #{
                        <<"name">> => [<<"share1">>, <<"share2">>],
                        <<"description">> => [<<"">>, <<"# Some description">>],
                        <<"fileId">> => [FileObjectId],
                        <<"rootFileId">> => [FileObjectId]
                    },
                    bad_values = [
                        {<<"name">>, 100, ?ERROR_BAD_VALUE_BINARY(<<"name">>)},
                        {<<"description">>, 14, ?ERROR_BAD_VALUE_BINARY(<<"description">>)}
                    ]
                }
            )
        }
    ])).


%% @private
-spec create_share_prepare_rest_args_fun(onenv_api_test_runner:api_test_ctx()) ->
    onenv_api_test_runner:rest_args().
create_share_prepare_rest_args_fun(#api_test_ctx{data = Data}) ->
    #rest_args{
        method = post,
        path = <<"shares">>,
        headers = #{?HDR_CONTENT_TYPE => <<"application/json">>},
        body = json_utils:encode(Data)
    }.


%% @private
-spec create_share_prepare_gs_args_fun(onenv_api_test_runner:api_test_ctx()) ->
    onenv_api_test_runner:gs_args().
create_share_prepare_gs_args_fun(#api_test_ctx{data = Data}) ->
    #gs_args{
        operation = create,
        gri = #gri{type = op_share, aspect = instance, scope = private},
        data = Data
    }.


%% @private
-spec build_create_share_validate_rest_call_result_fun(
    api_test_memory:mem_ref(),
    [oct_background:entity_selector()],
    api_test_utils:file_type(),
    od_space:id()
) ->
    onenv_api_test_runner:validate_call_result_fun().
build_create_share_validate_rest_call_result_fun(MemRef, Providers, FileType, SpaceId) ->
    fun(#api_test_ctx{
        node = TestNode,
        client = ?USER(UserId),
        data = Data = #{<<"name">> := ShareName}
    }, Result) ->
        {ok, FileGuid} = file_id:objectid_to_guid(get_root_file_id(Data)),
        Description = maps:get(<<"description">>, Data, <<"">>),

        {ok, _, Headers, Body} = ?assertMatch(
            {ok, ?HTTP_201_CREATED, #{?HDR_LOCATION := _}, #{<<"shareId">> := _}},
            Result
        ),
        ShareId = maps:get(<<"shareId">>, Body),

        api_test_memory:set(MemRef, shares, [ShareId | api_test_memory:get(MemRef, shares, [])]),

        ExpLocation = api_test_utils:build_rest_url(TestNode, [<<"shares">>, ShareId]),
        ?assertEqual(ExpLocation, maps:get(?HDR_LOCATION, Headers)),

        verify_share_doc(
            Providers, ShareId, ShareName, Description,
            SpaceId, FileGuid, FileType, UserId
        )
    end.


%% @private
-spec build_create_share_validate_gs_call_result_fun(
    api_test_memory:mem_ref(),
    [oct_background:entity_selector()],
    api_test_utils:file_type(),
    od_space:id()
) ->
    onenv_api_test_runner:validate_call_result_fun().
build_create_share_validate_gs_call_result_fun(MemRef, Providers, FileType, SpaceId) ->
    fun(#api_test_ctx{
        client = ?USER(UserId),
        data = Data = #{<<"name">> := ShareName}
    }, Result) ->
        {ok, FileGuid} = file_id:objectid_to_guid(get_root_file_id(Data)),
        Description = maps:get(<<"description">>, Data, <<"">>),

        {ok, #{<<"gri">> := ShareGri} = ShareData} = ?assertMatch({ok, _}, Result),

        #gri{id = ShareId} = ?assertMatch(
            #gri{type = op_share, aspect = instance, scope = private},
            gri:deserialize(ShareGri)
        ),
        api_test_memory:set(MemRef, shares, [ShareId | api_test_memory:get(MemRef, shares, [])]),

        assert_proper_gs_share_translation(SpaceId, ShareId, ShareName, Description, private, FileGuid, FileType, ShareData),

        verify_share_doc(
            Providers, ShareId, ShareName, Description,
            SpaceId, FileGuid, FileType, UserId
        )
    end.


%% @private
-spec get_root_file_id(middleware:data()) -> file_id:file_guid().
get_root_file_id(Data) ->
    case maps:get(<<"rootFileId">>, Data, undefined) of
        undefined -> maps:get(<<"fileId">>, Data);
        RootFileGuid -> RootFileGuid
    end.


get_share_test(_Config) ->
    Providers = [krakow, paris],
    SpaceId = oct_background:get_space_id(space_krk_par),

    ShareName = <<"share">>,
    Description = <<"# Collection ABC\nThis collection contains elements.">>,

    {FileType, FileSpec} = generate_random_file_spec([
        #share_spec{name = ShareName, description = Description}
    ]),
    #object{guid = FileGuid, shares = [ShareId]} = onenv_file_test_utils:create_and_sync_file_tree(
        user3, SpaceId, FileSpec
    ),
    ShareGuid = file_id:guid_to_share_guid(FileGuid, ShareId),
    {ok, ShareObjectId} = file_id:guid_to_objectid(ShareGuid),

    DataSpec = #data_spec{
        bad_values = [{bad_id, <<"NonExistentShare">>, ?ERROR_NOT_FOUND}]
    },

    ?assert(onenv_api_test_runner:run_tests([
        #suite_spec{
            target_nodes = Providers,
            client_spec = #client_spec{
                correct = [
                    user2, % space owner - doesn't need any perms
                    user3, user4 % space members - any space member can fetch info about share
                ],
                unauthorized = [nobody],
                forbidden_not_in_space = [user1]
            },
            scenario_templates = [
                #scenario_template{
                    name = <<"Get share using /shares rest endpoint">>,
                    type = rest,
                    prepare_args_fun = build_get_share_prepare_rest_args_fun(ShareId),
                    validate_result_fun = fun(_, {ok, RespCode, _, RespBody}) ->
                        ExpShareData = #{
                            <<"shareId">> => ShareId,
                            <<"name">> => ShareName,
                            <<"description">> => Description,
                            <<"rootFileType">> => FileType,
                            <<"publicUrl">> => build_share_public_url(ShareId),
                            <<"publicRestUrl">> => build_share_public_rest_url(ShareId),
                            <<"rootFileId">> => ShareObjectId,
                            <<"spaceId">> => SpaceId,
                            <<"handleId">> => null
                        },
                        ?assertEqual({?HTTP_200_OK, ExpShareData}, {RespCode, RespBody})
                    end
                },
                #scenario_template{
                    name = <<"Get share using gs private api">>,
                    type = gs,
                    prepare_args_fun = build_get_share_prepare_gs_args_fun(ShareId, private),
                    validate_result_fun = fun(_, {ok, Result}) ->
                        assert_proper_gs_share_translation(SpaceId, ShareId, ShareName, Description, private, FileGuid, FileType, Result)
                    end
                }
            ],
            data_spec = DataSpec
        },
        #scenario_spec{
            name = <<"Get share using gs public api">>,
            type = gs,
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SHARES,
            prepare_args_fun = build_get_share_prepare_gs_args_fun(ShareId, public),
            validate_result_fun = fun(_, {ok, Result}) ->
                assert_proper_gs_share_translation(SpaceId, ShareId, ShareName, Description, public, FileGuid, FileType, Result)
            end,
            data_spec = DataSpec
        }
    ])).


%% @private
-spec build_get_share_prepare_rest_args_fun(od_share:id()) ->
    onenv_api_test_runner:prepare_args_fun().
build_get_share_prepare_rest_args_fun(ShareId) ->
    fun(#api_test_ctx{data = Data}) ->
        {Id, _} = api_test_utils:maybe_substitute_bad_id(ShareId, Data),

        #rest_args{
            method = get,
            path = <<"shares/", Id/binary>>
        }
    end.


%% @private
-spec build_get_share_prepare_gs_args_fun(od_share:id(), gri:scope()) ->
    onenv_api_test_runner:prepare_args_fun().
build_get_share_prepare_gs_args_fun(ShareId, Scope) ->
    fun(#api_test_ctx{data = Data0}) ->
        {Id, Data1} = api_test_utils:maybe_substitute_bad_id(ShareId, Data0),

        #gs_args{
            operation = get,
            gri = #gri{type = op_share, id = Id, aspect = instance, scope = Scope},
            data = Data1
        }
    end.


update_share_test(_Config) ->
    Providers = [krakow, paris],
    SpaceKrkParId = oct_background:get_space_id(space_krk_par),
    User3Id = oct_background:get_user_id(user3),

    OriginalShareName = <<"share">>,
    OriginalDescription = <<"### Nested heading at the beginning - total markdown anarchy.">>,

    {FileType, FileSpec} = generate_random_file_spec([
        #share_spec{name = OriginalShareName, description = OriginalDescription}
    ]),
    #object{guid = FileGuid, shares = [ShareId]} = onenv_file_test_utils:create_and_sync_file_tree(
        user3, SpaceKrkParId, FileSpec
    ),

    MemRef = api_test_memory:init(),
    api_test_memory:set(MemRef, previous_name, OriginalShareName),
    api_test_memory:set(MemRef, previous_description, OriginalDescription),

    ?assert(onenv_api_test_runner:run_tests([
        #suite_spec{
            target_nodes = Providers,
            client_spec = #client_spec{
                correct = [
                    user2, % space owner - doesn't need any perms
                    user3  % files owner (see fun create_shared_file/1)
                ],
                unauthorized = [nobody],
                forbidden_not_in_space = [user1],
                forbidden_in_space = [user4]  % forbidden by lack of privileges
            },
            verify_fun = fun
                (expected_failure, _) ->
                    PreviousName = api_test_memory:get(MemRef, previous_name),
                    PreviousDescription = api_test_memory:get(MemRef, previous_description),

                    verify_share_doc(
                        Providers, ShareId, PreviousName, PreviousDescription,
                        SpaceKrkParId, FileGuid, FileType, User3Id
                    );
                (expected_success, #api_test_ctx{client = ?USER(UserId), data = Data}) ->
                    PreviousName = api_test_memory:get(MemRef, previous_name),
                    PreviousDescription = api_test_memory:get(MemRef, previous_description),
                    ExpectedName = maps:get(<<"name">>, Data, PreviousName),
                    ExpectedDescription = maps:get(<<"description">>, Data, PreviousDescription),

                    verify_share_doc(
                        Providers, ShareId, ExpectedName, ExpectedDescription,
                        SpaceKrkParId, FileGuid, FileType, UserId
                    ),
                    api_test_memory:set(MemRef, previous_name, ExpectedName),
                    api_test_memory:set(MemRef, previous_description, ExpectedDescription)
            end,
            scenario_templates = [
                #scenario_template{
                    name = <<"Update share for ", FileType/binary, " using /shares rest endpoint">>,
                    type = rest,
                    prepare_args_fun = build_update_share_prepare_rest_args_fun(ShareId),
                    validate_result_fun = fun(_, {ok, RespCode, _, RespBody}) ->
                        ?assertEqual({?HTTP_204_NO_CONTENT, #{}}, {RespCode, RespBody})
                    end
                },
                #scenario_template{
                    name = <<"Update share for ", FileType/binary, " using gs api">>,
                    type = gs,
                    prepare_args_fun = build_update_share_prepare_gs_args_fun(ShareId),
                    validate_result_fun = fun(_, Result) ->
                        ?assertEqual(ok, Result)
                    end
                }
            ],
            data_spec = #data_spec{
                at_least_one = [<<"description">>, <<"name">>],
                correct_values = #{
                    <<"name">> => [<<"szer">>, OriginalShareName],
                    <<"description">> => [<<"">>, OriginalDescription]
                },
                bad_values = [
                    {<<"name">>, 100, ?ERROR_BAD_VALUE_BINARY(<<"name">>)},
                    {<<"name">>, <<>>, ?ERROR_BAD_VALUE_EMPTY(<<"name">>)},
                    {<<"description">>, 90, ?ERROR_BAD_VALUE_BINARY(<<"description">>)},
                    {bad_id, <<"NonExistentShare">>, ?ERROR_NOT_FOUND}
                ]
            }
        }
    ])).


%% @private
-spec build_update_share_prepare_rest_args_fun(od_share:id()) ->
    onenv_api_test_runner:prepare_args_fun().
build_update_share_prepare_rest_args_fun(ShareId) ->
    fun(#api_test_ctx{data = Data0}) ->
        {Id, Data1} = api_test_utils:maybe_substitute_bad_id(ShareId, Data0),

        #rest_args{
            method = patch,
            path = <<"shares/", Id/binary>>,
            headers = #{?HDR_CONTENT_TYPE => <<"application/json">>},
            body = json_utils:encode(Data1)
        }
    end.


%% @private
-spec build_update_share_prepare_gs_args_fun(od_share:id()) ->
    onenv_api_test_runner:prepare_args_fun().
build_update_share_prepare_gs_args_fun(ShareId) ->
    fun(#api_test_ctx{data = Data0}) ->
        {Id, Data1} = api_test_utils:maybe_substitute_bad_id(ShareId, Data0),

        #gs_args{
            operation = update,
            gri = #gri{type = op_share, id = Id, aspect = instance, scope = private},
            data = Data1
        }
    end.


delete_share_test(_Config) ->
    Providers = [krakow, paris],
    SpaceId = oct_background:get_space_id(space_krk_par),

    {FileType, FileSpec} = generate_random_file_spec([#share_spec{} || _ <- lists:seq(1, 4)]),
    #object{guid = FileGuid, shares = ShareIds} = onenv_file_test_utils:create_and_sync_file_tree(
        user3, SpaceId, FileSpec
    ),

    MemRef = api_test_memory:init(),
    api_test_memory:set(MemRef, shares, ShareIds),

    ?assert(onenv_api_test_runner:run_tests([
        #suite_spec{
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SPACE_KRK_PAR(?EPERM),
            verify_fun = build_verify_file_shares_fun(MemRef, Providers, user3, FileGuid),
            scenario_templates = [
                #scenario_template{
                    name = <<"Delete share for ", FileType/binary, " using /shares rest endpoint">>,
                    type = rest,
                    prepare_args_fun = build_delete_share_prepare_rest_args_fun(MemRef),
                    validate_result_fun = build_delete_share_validate_rest_call_result_fun(MemRef, Providers)
                },
                #scenario_template{
                    name = <<"Delete share for ", FileType/binary, " using gs api">>,
                    type = gs,
                    prepare_args_fun = build_delete_share_prepare_gs_args_fun(MemRef),
                    validate_result_fun = build_delete_share_validate_gs_call_result_fun(MemRef, Providers)
                }
            ],
            data_spec = #data_spec{
                bad_values = [
                    {bad_id, <<"NonExistentShare">>, ?ERROR_NOT_FOUND}
                ]
            }
        }
    ])).


%% @private
-spec build_delete_share_prepare_rest_args_fun(api_test_memory:mem_ref()) ->
    onenv_api_test_runner:prepare_args_fun().
build_delete_share_prepare_rest_args_fun(MemRef) ->
    fun(#api_test_ctx{data = Data}) ->
        ShareId = choose_share_to_remove(MemRef),
        {Id, _} = api_test_utils:maybe_substitute_bad_id(ShareId, Data),

        #rest_args{
            method = delete,
            path = <<"shares/", Id/binary>>
        }
    end.


%% @private
-spec build_delete_share_prepare_gs_args_fun(api_test_memory:mem_ref()) ->
    onenv_api_test_runner:prepare_args_fun().
build_delete_share_prepare_gs_args_fun(MemRef) ->
    fun(#api_test_ctx{data = Data0}) ->
        ShareId = choose_share_to_remove(MemRef),
        {Id, Data1} = api_test_utils:maybe_substitute_bad_id(ShareId, Data0),

        #gs_args{
            operation = delete,
            gri = #gri{type = op_share, id = Id, aspect = instance, scope = private},
            data = Data1
        }
    end.


%% @private
-spec choose_share_to_remove(api_test_memory:mem_ref()) -> od_share:id().
choose_share_to_remove(MemRef) ->
    Shares = api_test_memory:get(MemRef, shares),
    ShareId = lists_utils:random_element(Shares),
    api_test_memory:set(MemRef, share_to_remove, ShareId),

    ShareId.


%% @private
-spec build_delete_share_validate_rest_call_result_fun(
    api_test_memory:mem_ref(),
    [oct_background:entity_selector()]
) ->
    onenv_api_test_runner:validate_call_result_fun().
build_delete_share_validate_rest_call_result_fun(MemRef, Providers) ->
    fun(#api_test_ctx{client = ?USER(UserId)}, {ok, RespCode, _, RespBody}) ->
        ?assertEqual({?HTTP_204_NO_CONTENT, #{}}, {RespCode, RespBody}),
        validate_delete_share_result(MemRef, UserId, Providers)
    end.


%% @private
-spec build_delete_share_validate_gs_call_result_fun(
    api_test_memory:mem_ref(),
    [oct_background:entity_selector()]
) ->
    onenv_api_test_runner:validate_call_result_fun().
build_delete_share_validate_gs_call_result_fun(MemRef, Providers) ->
    fun(#api_test_ctx{client = ?USER(UserId)}, Result) ->
        ?assertEqual(ok, Result),
        validate_delete_share_result(MemRef, UserId, Providers)
    end.


%% @private
-spec validate_delete_share_result(
    api_test_memory:mem_ref(),
    od_user:id(),
    [oct_background:entity_selector()]
) ->
    ok.
validate_delete_share_result(MemRef, UserId, Providers) ->
    ShareId = api_test_memory:get(MemRef, share_to_remove),

    lists:foreach(fun(Provider) ->
        ?assertEqual(?ERROR_NOT_FOUND, get_share_doc(Provider, UserId, ShareId), ?ATTEMPTS)
    end, Providers),

    api_test_memory:set(MemRef, shares, lists:delete(ShareId, api_test_memory:get(MemRef, shares))).


%%%===================================================================
%%% Common share test utils
%%%===================================================================


%% @private
-spec generate_random_file_spec() ->
    {api_test_utils:file_type(), onenv_file_test_utils:file_spec()}.
generate_random_file_spec() ->
    generate_random_file_spec([]).


%% @private
-spec generate_random_file_spec([onenv_file_test_utils:shares_spec()]) ->
    {binary(), onenv_file_test_utils:file_spec()}.
generate_random_file_spec(ShareSpecs) ->
    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    case FileType of
        <<"file">> -> {<<"REG">>, #file_spec{shares = ShareSpecs}};
        <<"dir">> -> {<<"DIR">>, #dir_spec{shares = ShareSpecs}}
    end.


%% @private
-spec verify_share_doc(
    [oct_background:entity_selector()], od_share:id(), od_share:name(), od_share:description(),
    od_space:id(), file_id:file_guid(), api_test_utils:file_type(), od_user:id()
) ->
    ok.
verify_share_doc(Providers, ShareId, ShareName, Description, SpaceId, FileGuid, FileType, UserId) ->
    ExpPublicUrl = build_share_public_url(ShareId),
    ExpPublicRestUrl = build_share_public_rest_url(ShareId),
    ExpFileType = case FileType of
        <<"REG">> -> file;
        <<"DIR">> -> dir
    end,
    ShareFileGuid = file_id:guid_to_share_guid(FileGuid, ShareId),

    lists:foreach(fun(Provider) ->
        ?assertMatch(
            {ok, #document{key = ShareId, value = #od_share{
                name = ShareName,
                description = Description,
                space = SpaceId,
                root_file = ShareFileGuid,
                public_url = ExpPublicUrl,
                public_rest_url = ExpPublicRestUrl,
                file_type = ExpFileType,
                handle = undefined
            }}},
            get_share_doc(Provider, UserId, ShareId),
            ?ATTEMPTS
        )
    end, Providers).


%% @private
-spec get_share_doc(oct_background:entity_selector(), od_user:id(), od_share:id()) ->
    od_share:doc().
get_share_doc(ProviderSelector, UserId, ShareId) ->
    Node = get_random_op_node(ProviderSelector),
    ProviderId = oct_background:get_provider_id(ProviderSelector),
    UserSessId = oct_background:get_user_session_id(UserId, ProviderId),

    rpc:call(Node, share_logic, get, [UserSessId, ShareId]).


%% @private
-spec assert_proper_gs_share_translation(
    od_space:id(), od_share:id(), od_share:name(), od_share:description(), gri:scope(),
    file_id:file_guid(), api_test_utils:file_type(), map()
) ->
    ok.
assert_proper_gs_share_translation(SpaceId, ShareId, ShareName, Description, Scope, FileGuid, FileType, GsShareData) ->
    ShareFileGuid = file_id:guid_to_share_guid(FileGuid, ShareId),

    ExpBasicShareData = #{
        <<"revision">> => 1,
        <<"gri">> => gri:serialize(#gri{
            type = op_share,
            id = ShareId,
            aspect = instance,
            scope = Scope
        }),
        <<"spaceId">> => SpaceId,
        <<"name">> => ShareName,
        <<"description">> => Description,
        <<"publicUrl">> => build_share_public_url(ShareId),
        <<"publicRestUrl">> => build_share_public_rest_url(ShareId),
        <<"rootFileType">> => FileType,
        <<"rootFile">> => gri:serialize(#gri{
            type = op_file,
            id = ShareFileGuid,
            aspect = instance,
            scope = public
        }),
        <<"handle">> => null
    },
    ExpShareData = case Scope of
        public ->
            ExpBasicShareData;
        private ->
            ExpBasicShareData#{<<"privateRootFile">> => gri:serialize(#gri{
                type = op_file,
                id = FileGuid,
                aspect = instance,
                scope = private
            })}
    end,
    ?assertEqual(ExpShareData, GsShareData).


%% @private
-spec build_verify_file_shares_fun(
    api_test_memory:mem_ref(),
    [oct_background:entity_selector()],
    oct_background:entity_selector(),
    file_id:file_guid()
) ->
    onenv_api_test_runner:verify_fun().
build_verify_file_shares_fun(MemRef, Providers, UserSelector, FileGuid) ->
    fun(_, _) ->
        ExpShares = lists:sort(api_test_memory:get(MemRef, shares, [])),

        lists:foreach(fun(Provider) ->
            ?assertEqual(ExpShares, get_file_shares(Provider, UserSelector, FileGuid), ?ATTEMPTS)
        end, Providers)
    end.


%% @private
-spec get_file_shares(
    oct_background:entity_selector(),
    oct_background:entity_selector(),
    file_id:file_guid()
) ->
    [od_share:id()].
get_file_shares(ProviderSelector, UserSelector, FileGuid) ->
    Node = get_random_op_node(ProviderSelector),
    ProviderId = oct_background:get_provider_id(ProviderSelector),
    UserSessId = oct_background:get_user_session_id(UserSelector, ProviderId),

    {ok, #file_attr{shares = FileShares}} = ?assertMatch(
        {ok, _},
        lfm_proxy:stat(Node, UserSessId, ?FILE_REF(FileGuid))
    ),
    lists:sort(FileShares).


%% @private
-spec get_random_op_node(oct_background:entity_selector()) -> node().
get_random_op_node(ProviderSelector) ->
    lists_utils:random_element(oct_background:get_provider_nodes(ProviderSelector)).


%% @private
-spec build_share_public_url(od_share:id()) -> binary().
build_share_public_url(ShareId) ->
    OzDomain = ozw_test_rpc:get_domain(),
    str_utils:format_bin("https://~s/share/~s", [OzDomain, ShareId]).


%% @private
-spec build_share_public_rest_url(od_share:id()) -> binary().
build_share_public_rest_url(ShareId) ->
    OzDomain = ozw_test_rpc:get_domain(),
    str_utils:format_bin("https://~s/api/v3/onezone/shares/~s/public", [OzDomain, ShareId]).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    oct_background:init_per_suite([{?LOAD_MODULES, [dir_stats_test_utils]} | Config], #onenv_test_config{
        onenv_scenario = "api_tests",
        envs = [{op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}],
        posthook = fun(NewConfig) ->
            dir_stats_test_utils:disable_stats_counting(NewConfig),
            User3Id = oct_background:get_user_id(user3),
            SpaceId = oct_background:get_space_id(space_krk_par),
            ozw_test_rpc:space_set_user_privileges(SpaceId, User3Id, [
                ?SPACE_MANAGE_SHARES | privileges:space_member()
            ]),
            NewConfig
        end
    }).


end_per_suite(Config) ->
    oct_background:end_per_suite(),
    dir_stats_test_utils:enable_stats_counting(Config).


init_per_group(_Group, Config) ->
    lfm_proxy:init(Config, false).


end_per_group(_Group, Config) ->
    lfm_proxy:teardown(Config).


init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 10}),
    Config.


end_per_testcase(_Case, _Config) ->
    ok.
