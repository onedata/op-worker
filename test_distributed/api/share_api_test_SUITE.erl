%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests concerning share basic API (REST + gs).
%%% @end
%%%-------------------------------------------------------------------
-module(share_api_test_SUITE).
-author("Bartosz Walkowicz").

-include("api_test_runner.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/graph_sync/gri.hrl").
-include_lib("ctool/include/http/codes.hrl").
-include_lib("ctool/include/test/performance.hrl").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

% exported for rpc test calls
-export([create_share/2, update_share/2, delete_share/2]).

-export([
    create_share_test/1,
    get_share_test/1,
    update_share_test/1,
    delete_share_test/1
]).

all() ->
    ?ALL([
        create_share_test,
        get_share_test,
        update_share_test,
        delete_share_test
    ]).

-define(SHARE_PUBLIC_URL(__SHARE_ID), <<__SHARE_ID/binary, "_public_url">>).
-define(SHARE_HANDLE(__SHARE_ID), <<__SHARE_ID/binary, "_handle_id">>).

-define(ATTEMPTS, 30).


%%%===================================================================
%%% Get file distribution test functions
%%%===================================================================


create_share_test(Config) ->
    [P2, P1] = Providers = ?config(op_worker_nodes, Config),
    SessIdP1 = ?USER_IN_BOTH_SPACES_SESS_ID(P1, Config),

    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    FilePath = filename:join(["/", ?SPACE_2, ?RANDOM_FILE_NAME()]),
    {ok, FileGuid} = api_test_utils:create_file(FileType, P1, SessIdP1, FilePath, 8#777),
    file_test_utils:await_sync(P2, FileGuid),

    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),

    MemRef = api_test_memory:init(),

    ?assert(api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SPACE_2_SCENARIOS(Config),
            verify_fun = build_verify_file_shares_fun(MemRef, Providers, ?USER_IN_BOTH_SPACES, FileGuid, Config),
            scenario_templates = [
                #scenario_template{
                    name = <<"Create share for ", FileType/binary, " using /shares rest endpoint">>,
                    type = rest,
                    prepare_args_fun = fun create_share_prepare_rest_args_fun/1,
                    validate_result_fun = build_create_share_validate_rest_call_result_fun(
                        MemRef, Providers, FileType, ?SPACE_2, Config
                    )
                },
                #scenario_template{
                    name = <<"Create share for ", FileType/binary, " using gs api">>,
                    type = gs,
                    prepare_args_fun = fun create_share_prepare_gs_args_fun/1,
                    validate_result_fun = build_create_share_validate_gs_call_result_fun(
                        MemRef, Providers, FileType, Config
                    )
                }
            ],
            randomly_select_scenarios = true,
            data_spec = api_test_utils:add_cdmi_id_errors_for_operations_not_available_in_share_mode(
                % Operations should be rejected even before checking if share exists
                % (in case of using share file id) so it is not necessary to use
                % valid share id
                FileGuid, ?SPACE_2, <<"NonExistentShare">>, #data_spec{
                    required = [<<"name">>, <<"fileId">>],
                    optional = [<<"description">>],
                    correct_values = #{
                        <<"name">> => [<<"share1">>, <<"share2">>],
                        <<"description">> => [<<"">>, <<"# Some description">>],
                        <<"fileId">> => [FileObjectId]
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
create_share_prepare_rest_args_fun(#api_test_ctx{data = Data}) ->
    #rest_args{
        method = post,
        path = <<"shares">>,
        headers = #{<<"content-type">> => <<"application/json">>},
        body = json_utils:encode(Data)
    }.


%% @private
create_share_prepare_gs_args_fun(#api_test_ctx{data = Data}) ->
    #gs_args{
        operation = create,
        gri = #gri{type = op_share, aspect = instance, scope = private},
        data = Data
    }.


%% @private
build_create_share_validate_rest_call_result_fun(MemRef, Providers, FileType, SpaceId, Config) ->
    fun(#api_test_ctx{
        node = Node,
        client = ?USER(UserId),
        data = Data = #{<<"name">> := ShareName, <<"fileId">> := FileObjectId}
    }, Result) ->
        Description = maps:get(<<"description">>, Data, <<"">>),
        {ok, _, Headers, Body} = ?assertMatch(
            {ok, ?HTTP_201_CREATED, #{<<"Location">> := _}, #{<<"shareId">> := _}},
            Result
        ),
        {ok, FileGuid} = file_id:objectid_to_guid(FileObjectId),
        ShareId = maps:get(<<"shareId">>, Body),

        api_test_memory:set(MemRef, shares, [ShareId | api_test_memory:get(MemRef, shares, [])]),

        ExpLocation = rpc:call(Node, oneprovider, get_rest_endpoint, [
            string:trim(filename:join([<<"/">>, <<"shares">>, ShareId]), leading, [$/])
        ]),
        ?assertEqual(ExpLocation, maps:get(<<"Location">>, Headers)),

        verify_share_doc(
            Providers, ShareId, ShareName, Description, SpaceId,
            FileGuid, FileType, UserId, Config
        )
    end.


%% @private
build_create_share_validate_gs_call_result_fun(MemRef, Providers, FileType, Config) ->
    fun(#api_test_ctx{
        client = ?USER(UserId),
        data = Data = #{<<"name">> := ShareName, <<"fileId">> := FileObjectId}
    }, Result) ->
        Description = maps:get(<<"description">>, Data, <<"">>),
        {ok, FileGuid} = file_id:objectid_to_guid(FileObjectId),
        {ok, #{<<"gri">> := ShareGri} = ShareData} = ?assertMatch({ok, _}, Result),

        #gri{id = ShareId} = ?assertMatch(
            #gri{type = op_share, aspect = instance, scope = private},
            gri:deserialize(ShareGri)
        ),
        api_test_memory:set(MemRef, shares, [ShareId | api_test_memory:get(MemRef, shares, [])]),

        assert_proper_gs_share_translation(ShareId, ShareName, Description, private, FileGuid, FileType, ShareData),

        verify_share_doc(
            Providers, ShareId, ShareName, Description, ?SPACE_2,
            FileGuid, FileType, UserId, Config
        )
    end.


get_share_test(Config) ->
    [P2, P1] = Providers = ?config(op_worker_nodes, Config),
    SessIdP1 = ?USER_IN_BOTH_SPACES_SESS_ID(P1, Config),

    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    FilePath = filename:join(["/", ?SPACE_2, ?RANDOM_FILE_NAME()]),
    {ok, FileGuid} = api_test_utils:create_file(FileType, P1, SessIdP1, FilePath, 8#777),

    ShareName = <<"share">>,
    Description = <<"# Collection ABC\nThis collection contains elements.">>,
    {ok, ShareId} = lfm_proxy:create_share(P1, SessIdP1, {guid, FileGuid}, ShareName, Description),
    file_test_utils:await_sync(P2, FileGuid),

    ShareGuid = file_id:guid_to_share_guid(FileGuid, ShareId),
    {ok, ShareObjectId} = file_id:guid_to_objectid(ShareGuid),

    DataSpec = #data_spec{
        bad_values = [{bad_id, <<"NonExistentShare">>, ?ERROR_NOT_FOUND}]
    },

    ?assert(api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SPACE_2_SCENARIOS(Config),
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
                            <<"fileType">> => FileType,
                            <<"publicUrl">> => ?SHARE_PUBLIC_URL(ShareId),
                            <<"rootFileId">> => ShareObjectId,
                            <<"spaceId">> => ?SPACE_2,
                            <<"handleId">> => ?SHARE_HANDLE(ShareId)
                        },
                        ?assertEqual({?HTTP_200_OK, ExpShareData}, {RespCode, RespBody})
                    end
                },
                #scenario_template{
                    name = <<"Get share using gs private api">>,
                    type = gs,
                    prepare_args_fun = build_get_share_prepare_gs_args_fun(ShareId, private),
                    validate_result_fun = fun(_, {ok, Result}) ->
                        assert_proper_gs_share_translation(ShareId, ShareName, Description, private, FileGuid, FileType, Result)
                    end
                }
            ],
            data_spec = DataSpec
        },
        #scenario_spec{
            name = <<"Get share using gs public api">>,
            type = gs,
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SHARE_SCENARIOS(Config),
            prepare_args_fun = build_get_share_prepare_gs_args_fun(ShareId, public),
            validate_result_fun = fun(_, {ok, Result}) ->
                assert_proper_gs_share_translation(ShareId, ShareName, Description, public, FileGuid, FileType, Result)
            end,
            data_spec = DataSpec
        }
    ])).


%% @private
build_get_share_prepare_rest_args_fun(ShareId) ->
    fun(#api_test_ctx{data = Data}) ->
        {Id, _} = api_test_utils:maybe_substitute_bad_id(ShareId, Data),

        #rest_args{
            method = get,
            path = <<"shares/", Id/binary>>
        }
    end.


%% @private
build_get_share_prepare_gs_args_fun(ShareId, Scope) ->
    fun(#api_test_ctx{data = Data0}) ->
        {Id, Data1} = api_test_utils:maybe_substitute_bad_id(ShareId, Data0),

        #gs_args{
            operation = get,
            gri = #gri{type = op_share, id = Id, aspect = instance, scope = Scope},
            data = Data1
        }
    end.


update_share_test(Config) ->
    [P2, P1] = Providers = ?config(op_worker_nodes, Config),
    SessIdP1 = ?USER_IN_BOTH_SPACES_SESS_ID(P1, Config),

    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    FilePath = filename:join(["/", ?SPACE_2, ?RANDOM_FILE_NAME()]),
    {ok, FileGuid} = api_test_utils:create_file(FileType, P1, SessIdP1, FilePath, 8#777),

    OriginalDescription = <<"### Nested heading at the beginning - total markdown anarchy.">>,
    OriginalShareName = <<"share">>,
    {ok, ShareId} = lfm_proxy:create_share(P1, SessIdP1, {guid, FileGuid}, OriginalShareName, OriginalDescription),
    file_test_utils:await_sync(P2, FileGuid),

    MemRef = api_test_memory:init(),
    api_test_memory:set(MemRef, previous_name, OriginalShareName),
    api_test_memory:set(MemRef, previous_description, OriginalDescription),

    ?assert(api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SPACE_2_SCENARIOS(Config),
            verify_fun = fun
                (expected_failure, _) ->
                    PreviousName = api_test_memory:get(MemRef, previous_name),
                    PreviousDescription = api_test_memory:get(MemRef, previous_description),
                    verify_share_doc(
                        Providers, ShareId, PreviousName, PreviousDescription, ?SPACE_2,
                        FileGuid, FileType, ?USER_IN_BOTH_SPACES, Config
                    ),
                    true;
                (expected_success, #api_test_ctx{client = ?USER(UserId), data = Data}) ->
                    PreviousName = api_test_memory:get(MemRef, previous_name),
                    PreviousDescription = api_test_memory:get(MemRef, previous_description),
                    ExpectedName = maps:get(<<"name">>, Data, PreviousName),
                    ExpectedDescription = maps:get(<<"description">>, Data, PreviousDescription),
                    verify_share_doc(
                        Providers, ShareId, ExpectedName, ExpectedDescription, ?SPACE_2,
                        FileGuid, FileType, UserId, Config
                    ),
                    api_test_memory:set(MemRef, previous_name, ExpectedName),
                    api_test_memory:set(MemRef, previous_description, ExpectedDescription),
                    true
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
build_update_share_prepare_rest_args_fun(ShareId) ->
    fun(#api_test_ctx{data = Data0}) ->
        {Id, Data1} = api_test_utils:maybe_substitute_bad_id(ShareId, Data0),

        #rest_args{
            method = patch,
            path = <<"shares/", Id/binary>>,
            headers = #{<<"content-type">> => <<"application/json">>},
            body = json_utils:encode(Data1)
        }
    end.


%% @private
build_update_share_prepare_gs_args_fun(ShareId) ->
    fun(#api_test_ctx{data = Data0}) ->
        {Id, Data1} = api_test_utils:maybe_substitute_bad_id(ShareId, Data0),

        #gs_args{
            operation = update,
            gri = #gri{type = op_share, id = Id, aspect = instance, scope = private},
            data = Data1
        }
    end.


delete_share_test(Config) ->
    [P2, P1] = Providers = ?config(op_worker_nodes, Config),
    SessIdP1 = ?USER_IN_BOTH_SPACES_SESS_ID(P1, Config),

    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    FilePath = filename:join(["/", ?SPACE_2, ?RANDOM_FILE_NAME()]),
    {ok, FileGuid} = api_test_utils:create_file(FileType, P1, SessIdP1, FilePath, 8#777),

    ShareIds = lists:reverse(lists:map(fun(_) ->
        {ok, ShareId} = lfm_proxy:create_share(P1, SessIdP1, {guid, FileGuid}, <<"share">>),
        ShareId
    end, lists:seq(1, 4))),

    file_test_utils:await_sync(P2, FileGuid),

    MemRef = api_test_memory:init(),
    api_test_memory:set(MemRef, shares, ShareIds),

    ?assert(api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SPACE_2_SCENARIOS(Config),
            verify_fun = build_verify_file_shares_fun(MemRef, Providers, ?USER_IN_BOTH_SPACES, FileGuid, Config),
            scenario_templates = [
                #scenario_template{
                    name = <<"Delete share for ", FileType/binary, " using /shares rest endpoint">>,
                    type = rest,
                    prepare_args_fun = build_delete_share_prepare_rest_args_fun(MemRef),
                    validate_result_fun = build_delete_share_validate_rest_call_result_fun(MemRef, Providers, Config)
                },
                #scenario_template{
                    name = <<"Delete share for ", FileType/binary, " using gs api">>,
                    type = gs,
                    prepare_args_fun = build_delete_share_prepare_gs_args_fun(MemRef),
                    validate_result_fun = build_delete_share_validate_gs_call_result_fun(MemRef, Providers, Config)
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
build_delete_share_prepare_rest_args_fun(MemRef) ->
    fun(#api_test_ctx{data = Data}) ->
        Shares = api_test_memory:get(MemRef, shares),
        ShareId = lists_utils:random_element(Shares),
        api_test_memory:set(MemRef, share_to_remove, ShareId),

        {Id, _} = api_test_utils:maybe_substitute_bad_id(ShareId, Data),

        #rest_args{
            method = delete,
            path = <<"shares/", Id/binary>>
        }
    end.


%% @private
build_delete_share_prepare_gs_args_fun(MemRef) ->
    fun(#api_test_ctx{data = Data0}) ->
        Shares = api_test_memory:get(MemRef, shares),
        ShareId = lists_utils:random_element(Shares),
        api_test_memory:set(MemRef, share_to_remove, ShareId),

        {Id, Data1} = api_test_utils:maybe_substitute_bad_id(ShareId, Data0),

        #gs_args{
            operation = delete,
            gri = #gri{type = op_share, id = Id, aspect = instance, scope = private},
            data = Data1
        }
    end.


%% @private
build_delete_share_validate_rest_call_result_fun(MemRef, Providers, Config) ->
    fun(#api_test_ctx{client = ?USER(UserId)}, {ok, RespCode, _, RespBody}) ->
        ?assertEqual({?HTTP_204_NO_CONTENT, #{}}, {RespCode, RespBody}),
        build_validate_delete_share_result(MemRef, UserId, Providers, Config)
    end.


%% @private
build_delete_share_validate_gs_call_result_fun(MemRef, Providers, Config) ->
    fun(#api_test_ctx{client = ?USER(UserId)}, Result) ->
        ?assertEqual(ok, Result),
        build_validate_delete_share_result(MemRef, UserId, Providers, Config)
    end.


%% @private
build_validate_delete_share_result(MemRef, UserId, Providers, Config) ->
    ShareId = api_test_memory:get(MemRef, share_to_remove),
    lists:foreach(fun(Node) ->
        ?assertEqual(
            ?ERROR_NOT_FOUND,
            rpc:call(Node, share_logic, get, [?SESS_ID(UserId, Node, Config), ShareId]),
            ?ATTEMPTS
        )
    end, Providers),
    api_test_memory:set(MemRef, shares, lists:delete(ShareId, api_test_memory:get(MemRef, shares))),

    ok.


%%%===================================================================
%%% Common share test utils
%%%===================================================================


%% @private
verify_share_doc(Providers, ShareId, ShareName, Description, SpaceId, FileGuid, FileType, UserId, Config) ->
    ExpPublicUrl = ?SHARE_PUBLIC_URL(ShareId),
    ExpHandle = ?SHARE_HANDLE(ShareId),
    ExpFileType = binary_to_atom(FileType, utf8),
    ShareFileGuid = file_id:guid_to_share_guid(FileGuid, ShareId),

    lists:foreach(fun(Node) ->
        ?assertMatch(
            {ok, #document{key = ShareId, value = #od_share{
                name = ShareName,
                description = Description,
                space = SpaceId,
                root_file = ShareFileGuid,
                public_url = ExpPublicUrl,
                file_type = ExpFileType,
                handle = ExpHandle
            }}},
            rpc:call(Node, share_logic, get, [?SESS_ID(UserId, Node, Config), ShareId])
        )
    end, Providers).


%% @private
assert_proper_gs_share_translation(ShareId, ShareName, Description, Scope, FileGuid, FileType, GsShareData) ->
    ShareFileGuid = file_id:guid_to_share_guid(FileGuid, ShareId),

    % Unfortunately there is no oz in api tests and so all *_logic modules are
    % mocked. Because of share_logic mocks created shares already have handle
    % and dummy public url.
    ExpBasicShareData = #{
        <<"revision">> => 1,
        <<"gri">> => gri:serialize(#gri{
            type = op_share,
            id = ShareId,
            aspect = instance,
            scope = Scope
        }),
        <<"name">> => ShareName,
        <<"description">> => Description,
        <<"fileType">> => FileType,
        <<"publicUrl">> => <<ShareId/binary, "_public_url">>,
        <<"rootFile">> => gri:serialize(#gri{
            type = op_file,
            id = ShareFileGuid,
            aspect = instance,
            scope = public
        })
    },
    ExpShareData = case Scope of
        public ->
            ExpBasicShareData#{<<"handle">> => null};
        private ->
            ExpBasicShareData#{
                <<"handle">> => gri:serialize(#gri{
                    type = op_handle,
                    id = <<ShareId/binary, "_handle_id">>,
                    aspect = instance,
                    % the share's handle is mocked (with a fake id), consequently
                    % the user is not a member of the handle and does not have access
                    % to its private scope, so public scope should be included in the GRI
                    scope = public
                }),
                <<"privateRootFile">> => gri:serialize(#gri{
                    type = op_file,
                    id = FileGuid,
                    aspect = instance,
                    scope = private
                })
            }
    end,

    ?assertEqual(ExpShareData, GsShareData).


%% @private
build_verify_file_shares_fun(MemRef, Providers, UserId, FileGuid, Config) ->
    fun(_, _) ->
        Shares = api_test_memory:get(MemRef, shares, []),
        ExpShares = lists:sort(Shares),

        GetFileSharesFun = fun(Node) ->
            SessId = ?SESS_ID(UserId, Node, Config),
            {ok, #file_attr{shares = FileShares}} = ?assertMatch(
                {ok, _},
                lfm_proxy:stat(Node, SessId, {guid, FileGuid})
            ),
            lists:sort(FileShares)
        end,

        lists:foreach(fun(Node) ->
            ?assertEqual(ExpShares, GetFileSharesFun(Node), ?ATTEMPTS)
        end, Providers),

        true
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
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms, timer:seconds(1)), % TODO - change to 2 seconds
            test_utils:set_env(Worker, ?APP_NAME, public_block_size_treshold, 0),
            test_utils:set_env(Worker, ?APP_NAME, public_block_percent_treshold, 0)
        end, ?config(op_worker_nodes, NewConfig2)),
        ssl:start(),
        hackney:start(),
        NewConfig3 = initializer:create_test_users_and_spaces(
            ?TEST_FILE(NewConfig2, "env_desc.json"),
            NewConfig2
        ),
        initializer:mock_auth_manager(NewConfig3, _CheckIfUserIsSupported = true),
        ssl:start(),
        hackney:start(),
        NewConfig3
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].


end_per_suite(Config) ->
    hackney:stop(),
    ssl:stop(),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:teardown_storage(Config).


init_per_testcase(_Case, Config) ->
    mock_share_logic(Config),
    ct:timetrap({minutes, 10}),
    lfm_proxy:init(Config).


end_per_testcase(_Case, Config) ->
    initializer:unmock_share_logic(Config),
    lfm_proxy:teardown(Config).


%%%===================================================================
%%% Share mocks
%%%===================================================================


-spec mock_share_logic(proplists:proplist()) -> ok.
mock_share_logic(Config) ->
    TestNode = node(),
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, share_logic),

    test_utils:mock_expect(Workers, share_logic, create, fun(_Auth, ShareId, Name, Description, SpaceId, ShareFileGuid, FileType) ->
        ShareDoc = #document{key = ShareId, value = #od_share{
            name = Name,
            description = Description,
            space = SpaceId,
            root_file = ShareFileGuid,
            public_url = ?SHARE_PUBLIC_URL(ShareId),
            file_type = FileType,
            handle = ?SHARE_HANDLE(ShareId)
        }},
        rpc:call(TestNode, ?MODULE, create_share, [Workers, ShareDoc])
    end),

    test_utils:mock_expect(Workers, share_logic, get, fun(_Auth, ShareId) ->
        od_share:get_from_cache(ShareId)
    end),

    test_utils:mock_expect(Workers, share_logic, get_public_data, fun(_Auth, ShareId) ->
        case od_share:get_from_cache(ShareId) of
            {ok, #document{value = Share} = Doc} ->
                {ok, Doc#document{value = Share#od_share{
                    space = undefined,
                    handle = undefined
                }}};
            Error ->
                Error
        end
    end),

    test_utils:mock_expect(Workers, share_logic, update, fun(Auth, ShareId, Data) ->
        {ok, #document{key = ShareId, value = Share}} = share_logic:get(Auth, ShareId),
        rpc:call(TestNode, ?MODULE, update_share, [Workers, #document{
            key = ShareId,
            value = Share#od_share{
                name = maps:get(<<"name">>, Data, Share#od_share.name),
                description = maps:get(<<"description">>, Data, Share#od_share.description)
            }}
        ])
    end),

    test_utils:mock_expect(Workers, share_logic, delete, fun(_Auth, ShareId) ->
        rpc:call(TestNode, ?MODULE, delete_share, [Workers, ShareId]),
        ok
    end).


create_share(Providers, ShareDoc = #document{key = ShareId, value = Record}) ->
    {_, []} = rpc:multicall(Providers, od_share, update_cache, [
        ShareId, fun(_) -> {ok, Record} end, ShareDoc
    ]),
    {ok, ShareId}.


update_share(Providers, NewShareDoc = #document{key = ShareId, value = NewRecord}) ->
    {_, []} = rpc:multicall(Providers, od_share, invalidate_cache, [ShareId]),
    {_, []} = rpc:multicall(Providers, od_share, update_cache, [
        ShareId, fun(_) -> {ok, NewRecord} end, NewShareDoc
    ]),
    ok.


delete_share(Providers, ShareId) ->
    {_, []} = rpc:multicall(Providers, od_share, invalidate_cache, [ShareId]),
    ok.
