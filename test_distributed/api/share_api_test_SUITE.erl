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
    update_share_test/1,
    delete_share_test/1
]).

all() ->
    ?ALL([
        create_share_test,
        update_share_test,
        delete_share_test
    ]).


-define(ATTEMPTS, 30).


%%%===================================================================
%%% Get file distribution test functions
%%%===================================================================


create_share_test(Config) ->
    [P2, P1] = Providers = ?config(op_worker_nodes, Config),
    SessIdP1 = ?USER_IN_BOTH_SPACES_SESS_ID(P1, Config),
    SessIdP2 = ?USER_IN_BOTH_SPACES_SESS_ID(P2, Config),

    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    FilePath = filename:join(["/", ?SPACE_2, ?RANDOM_FILE_NAME()]),
    {ok, FileGuid} = api_test_utils:create_file(FileType, P1, SessIdP1, FilePath, 8#777),
    api_test_utils:wait_for_file_sync(P2, SessIdP2, FileGuid),

    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),

    EnvRef = api_test_utils:init_env(),
    api_test_utils:set_env_var(EnvRef, shares, []),

    ?assert(api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SPACE_2_SCENARIOS(Config),
            verify_fun = create_verify_file_shares_fun(EnvRef, Providers, ?USER_IN_BOTH_SPACES, FileGuid, Config),
            scenario_templates = [
                #scenario_template{
                    name = <<"Create share for ", FileType/binary, " using /shares rest endpoint">>,
                    type = rest,
                    prepare_args_fun = create_prepare_create_share_rest_args_fun(),
                    validate_result_fun = create_validate_create_share_rest_call_result_fun(
                        EnvRef, Providers, FileType, ?SPACE_2, Config
                    )
                },
                #scenario_template{
                    name = <<"Create share for ", FileType/binary, " using gs api">>,
                    type = gs,
                    prepare_args_fun = create_prepare_create_share_gs_args_fun(),
                    validate_result_fun = create_validate_create_share_gs_call_result_fun(
                        EnvRef, Providers, FileType, Config
                    )
                }
            ],
            randomly_select_scenarios = true,
            data_spec = #data_spec{
                required = [<<"name">>, <<"fileId">>],
                correct_values = #{
                    <<"name">> => [<<"share1">>, <<"share2">>],
                    <<"fileId">> => [FileObjectId]
                },
                bad_values = [
                    {<<"name">>, 100, ?ERROR_BAD_VALUE_BINARY(<<"name">>)}
                ]
            }
        }
    ])).


%% @private
create_prepare_create_share_rest_args_fun() ->
    fun(#api_test_ctx{data = Data}) ->
        #rest_args{
            method = post,
            path = <<"shares">>,
            headers = #{<<"content-type">> => <<"application/json">>},
            body = json_utils:encode(Data)
        }
    end.


%% @private
create_prepare_create_share_gs_args_fun() ->
    fun(#api_test_ctx{data = Data}) ->
        #gs_args{
            operation = create,
            gri = #gri{type = op_share, aspect = instance, scope = private},
            data = Data
        }
    end.


%% @private
create_validate_create_share_rest_call_result_fun(EnvRef, Providers, FileType, SpaceId, Config) ->
    fun(#api_test_ctx{
        node = Node,
        client = ?USER(UserId),
        data = #{<<"name">> := ShareName, <<"fileId">> := FileObjectId}
    }, Result) ->
        {ok, _, Headers, Body} = ?assertMatch(
            {ok, ?HTTP_201_CREATED, #{<<"Location">> := _}, #{<<"shareId">> := _}},
            Result
        ),
        ShareId = maps:get(<<"shareId">>, Body),

        {ok, Shares} = api_test_utils:get_env_var(EnvRef, shares),
        api_test_utils:set_env_var(EnvRef, shares, [ShareId | Shares]),

        ExpLocation = list_to_binary(rpc:call(Node, oneprovider, get_rest_endpoint, [
            string:trim(filename:join([<<"/">>, <<"shares">>, ShareId]), leading, [$/])
        ])),
        ?assertEqual(ExpLocation, maps:get(<<"Location">>, Headers)),

        {ok, FileGuid} = file_id:objectid_to_guid(FileObjectId),

        verify_share_doc(
            Providers, ShareId, ShareName, SpaceId,
            FileGuid, FileType, UserId, Config
        )
    end.


%% @private
create_validate_create_share_gs_call_result_fun(EnvRef, Providers, FileType, Config) ->
    fun(#api_test_ctx{
        client = ?USER(UserId),
        data = #{<<"name">> := ShareName, <<"fileId">> := FileObjectId}
    }, Result) ->
        {ok, #{<<"gri">> := ShareGri} = ShareData} = ?assertMatch({ok, _}, Result),

        #gri{id = ShareId} = ?assertMatch(
            #gri{type = op_share, aspect = instance, scope = private},
            gri:deserialize(ShareGri)
        ),
        {ok, Shares} = api_test_utils:get_env_var(EnvRef, shares),
        api_test_utils:set_env_var(EnvRef, shares, [ShareId | Shares]),

        {ok, FileGuid} = file_id:objectid_to_guid(FileObjectId),
        assert_gs_share_data(ShareId, ShareName, private, FileGuid, FileType, ShareData),
        verify_share_doc(
            Providers, ShareId, ShareName, ?SPACE_2,
            FileGuid, FileType, UserId, Config
        )
    end.


update_share_test(Config) ->
    [P2, P1] = Providers = ?config(op_worker_nodes, Config),
    SessIdP1 = ?USER_IN_BOTH_SPACES_SESS_ID(P1, Config),
    SessIdP2 = ?USER_IN_BOTH_SPACES_SESS_ID(P2, Config),

    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    FilePath = filename:join(["/", ?SPACE_2, ?RANDOM_FILE_NAME()]),
    {ok, FileGuid} = api_test_utils:create_file(FileType, P1, SessIdP1, FilePath, 8#777),

    OriginalShareName = <<"share">>,
    {ok, ShareId} = lfm_proxy:create_share(P1, SessIdP1, {guid, FileGuid}, OriginalShareName),
    api_test_utils:wait_for_file_sync(P2, SessIdP2, FileGuid),

    ?assert(api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SPACE_2_SCENARIOS(Config),
            verify_fun = fun
                (expected_failure, _) ->
                    verify_share_doc(
                        Providers, ShareId, OriginalShareName, ?SPACE_2, FileGuid, FileType,
                        ?USER_IN_BOTH_SPACES, Config
                    ),
                    true;
                (expected_success, #api_test_ctx{client = ?USER(UserId), data = #{<<"name">> := ShareName}}) ->
                    verify_share_doc(Providers, ShareId, ShareName, ?SPACE_2, FileGuid, FileType, UserId, Config),
                    true
            end,
            scenario_templates = [
                #scenario_template{
                    name = <<"Update share for ", FileType/binary, " using /shares rest endpoint">>,
                    type = rest,
                    prepare_args_fun = create_prepare_update_share_rest_args_fun(ShareId),
                    validate_result_fun = fun(_, {ok, RespCode, _, RespBody}) ->
                        ?assertEqual({?HTTP_204_NO_CONTENT, #{}}, {RespCode, RespBody})
                    end
                },
                #scenario_template{
                    name = <<"Update share for ", FileType/binary, " using gs api">>,
                    type = gs,
                    prepare_args_fun = create_prepare_update_share_gs_args_fun(ShareId),
                    validate_result_fun = fun(_, Result) ->
                        ?assertEqual({ok, undefined}, Result)
                    end
                }
            ],
            data_spec = #data_spec{
                required = [<<"name">>],
                correct_values = #{
                    <<"name">> => [<<"szer">>, OriginalShareName]
                },
                bad_values = [
                    {<<"name">>, 100, ?ERROR_BAD_VALUE_BINARY(<<"name">>)},
                    {<<"name">>, <<>>, ?ERROR_BAD_VALUE_EMPTY(<<"name">>)},
                    {bad_id, <<"NonExistentShare">>, ?ERROR_NOT_FOUND}
                ]
            }
        }
    ])).


%% @private
create_prepare_update_share_rest_args_fun(ShareId) ->
    fun(#api_test_ctx{data = Data0}) ->
        {Id, Data1} = api_test_utils:maybe_substitute_id(ShareId, Data0),

        #rest_args{
            method = patch,
            path = <<"shares/", Id/binary>>,
            headers = #{<<"content-type">> => <<"application/json">>},
            body = json_utils:encode(Data1)
        }
    end.


%% @private
create_prepare_update_share_gs_args_fun(ShareId) ->
    fun(#api_test_ctx{data = Data0}) ->
        {Id, Data1} = api_test_utils:maybe_substitute_id(ShareId, Data0),

        #gs_args{
            operation = update,
            gri = #gri{type = op_share, id = Id, aspect = instance, scope = private},
            data = Data1
        }
    end.


delete_share_test(Config) ->
    [P2, P1] = Providers = ?config(op_worker_nodes, Config),
    SessIdP1 = ?USER_IN_BOTH_SPACES_SESS_ID(P1, Config),
    SessIdP2 = ?USER_IN_BOTH_SPACES_SESS_ID(P2, Config),

    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    FilePath = filename:join(["/", ?SPACE_2, ?RANDOM_FILE_NAME()]),
    {ok, FileGuid} = api_test_utils:create_file(FileType, P1, SessIdP1, FilePath, 8#777),

    ShareIds = lists:reverse(lists:map(fun(_) ->
        {ok, ShareId} = lfm_proxy:create_share(P1, SessIdP1, {guid, FileGuid}, <<"share">>),
        ShareId
    end, lists:seq(1, 4))),

    api_test_utils:wait_for_file_sync(P2, SessIdP2, FileGuid),

    EnvRef = api_test_utils:init_env(),
    api_test_utils:set_env_var(EnvRef, shares, ShareIds),

    ?assert(api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SPACE_2_SCENARIOS(Config),
            verify_fun = create_verify_file_shares_fun(EnvRef, Providers, ?USER_IN_BOTH_SPACES, FileGuid, Config),
            scenario_templates = [
                #scenario_template{
                    name = <<"Delete share for ", FileType/binary, " using /shares rest endpoint">>,
                    type = rest,
                    prepare_args_fun = create_prepare_delete_share_rest_args_fun(EnvRef),
                    validate_result_fun = create_validate_delete_share_rest_call_result_fun(EnvRef, Providers, Config)
                },
                #scenario_template{
                    name = <<"Delete share for ", FileType/binary, " using gs api">>,
                    type = gs,
                    prepare_args_fun = create_prepare_delete_share_gs_args_fun(EnvRef),
                    validate_result_fun = create_validate_delete_share_gs_call_result_fun(EnvRef, Providers, Config)
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
create_prepare_delete_share_rest_args_fun(EnvRef) ->
    fun(#api_test_ctx{data = Data}) ->
        {ok, Shares} = api_test_utils:get_env_var(EnvRef, shares),
        ShareId = lists_utils:random_element(Shares),
        api_test_utils:set_env_var(EnvRef, share_to_remove, ShareId),

        {Id, _} = api_test_utils:maybe_substitute_id(ShareId, Data),

        #rest_args{
            method = delete,
            path = <<"shares/", Id/binary>>
        }
    end.


%% @private
create_prepare_delete_share_gs_args_fun(EnvRef) ->
    fun(#api_test_ctx{data = Data0}) ->
        {ok, Shares} = api_test_utils:get_env_var(EnvRef, shares),
        ShareId = lists_utils:random_element(Shares),
        api_test_utils:set_env_var(EnvRef, share_to_remove, ShareId),

        {Id, Data1} = api_test_utils:maybe_substitute_id(ShareId, Data0),

        #gs_args{
            operation = delete,
            gri = #gri{type = op_share, id = Id, aspect = instance, scope = private},
            data = Data1
        }
    end.


%% @private
create_validate_delete_share_rest_call_result_fun(EnvRef, Providers, Config) ->
    fun(#api_test_ctx{client = ?USER(UserId)}, {ok, RespCode, _, RespBody}) ->
        ?assertEqual({?HTTP_204_NO_CONTENT, #{}}, {RespCode, RespBody}),
        validate_delete_share_result(EnvRef, UserId, Providers, Config)
    end.


%% @private
create_validate_delete_share_gs_call_result_fun(EnvRef, Providers, Config) ->
    fun(#api_test_ctx{client = ?USER(UserId)}, Result) ->
        ?assertEqual({ok, undefined}, Result),
        validate_delete_share_result(EnvRef, UserId, Providers, Config)
    end.


%% @private
validate_delete_share_result(EnvRef, UserId, Providers, Config) ->
    {ok, Shares} = api_test_utils:get_env_var(EnvRef, shares),
    {ok, ShareId} = api_test_utils:get_env_var(EnvRef, share_to_remove),
    lists:foreach(fun(Node) ->
        ?assertEqual(
            ?ERROR_NOT_FOUND,
            rpc:call(Node, share_logic, get, [?SESS_ID(UserId, Node, Config), ShareId]),
            ?ATTEMPTS
        )
    end, Providers),
    api_test_utils:set_env_var(EnvRef, shares, lists:delete(ShareId, Shares)),

    ok.


%%%===================================================================
%%% Common share test utils
%%%===================================================================


%% @private
assert_gs_share_data(ShareId, ShareName, Scope, FileGuid, FileType, GsShareData) ->
    ShareFileGuid = file_id:guid_to_share_guid(FileGuid, ShareId),

    % Unfortunately there is no oz in api tests and so all *_logic modules are mocked.
    % Because of share_logic mocks created shares already have handle and dummy public url.
    ExpShareData = #{
        <<"revision">> => 1,
        <<"gri">> => gri:serialize(#gri{
            type = op_share,
            id = ShareId,
            aspect = instance,
            scope = Scope
        }),
        <<"name">> => ShareName,
        <<"fileType">> => FileType,
        <<"publicUrl">> => <<ShareId/binary, "_public_url">>,
        <<"handle">> => gri:serialize(#gri{
            type = op_handle,
            id = <<ShareId/binary, "_handle_id">>,
            aspect = instance,
            scope = private
        }),
        <<"privateRootFile">> => gri:serialize(#gri{
            type = op_file,
            id = FileGuid,
            aspect = instance,
            scope = private
        }),
        <<"rootFile">> => gri:serialize(#gri{
            type = op_file,
            id = ShareFileGuid,
            aspect = instance,
            scope = public
        })
    },
    ?assertEqual(ExpShareData, GsShareData).


%% @private
verify_share_doc(Providers, ShareId, ShareName, SpaceId, FileGuid, FileType, UserId, Config) ->
    ExpPublicUrl = <<ShareId/binary, "_public_url">>,
    ExpHandle = <<ShareId/binary, "_handle_id">>,
    ExpFileType = binary_to_atom(FileType, utf8),
    ShareFileGuid = file_id:guid_to_share_guid(FileGuid, ShareId),

    lists:foreach(fun(Node) ->
        ?assertMatch(
            {ok, #document{key = ShareId, value = #od_share{
                name = ShareName,
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
create_verify_file_shares_fun(EnvRef, Providers, UserId, FileGuid, Config) ->
    fun(_, _) ->
        {ok, Shares} = api_test_utils:get_env_var(EnvRef, shares),
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

    test_utils:mock_expect(Workers, share_logic, create, fun(_Auth, ShareId, Name, SpaceId, ShareFileGuid, FileType) ->
        ShareDoc = #document{key = ShareId, value = #od_share{
            name = Name,
            space = SpaceId,
            root_file = ShareFileGuid,
            public_url = <<ShareId/binary, "_public_url">>,
            file_type = FileType,
            handle = <<ShareId/binary, "_handle_id">>
        }},
        rpc:call(TestNode, ?MODULE, create_share, [Workers, ShareDoc])
    end),
    test_utils:mock_expect(Workers, share_logic, get, fun(_Auth, ShareId) ->
        od_share:get_from_cache(ShareId)
    end),
    test_utils:mock_expect(Workers, share_logic, delete, fun(_Auth, ShareId) ->
        rpc:call(TestNode, ?MODULE, delete_share, [Workers, ShareId]),
        ok
    end),
    test_utils:mock_expect(Workers, share_logic, update_name, fun(Auth, ShareId, NewName) ->
        {ok, #document{key = ShareId, value = Share}} = share_logic:get(Auth, ShareId),
        rpc:call(TestNode, ?MODULE, update_share, [Workers, #document{
            key = ShareId,
            value = Share#od_share{name = NewName}}
        ])
    end).


create_share(Providers, ShareDoc = #document{key = ShareId, value = Record}) ->
    {_, []} = rpc:multicall(Providers, od_share, update_cache, [ShareId, fun(_) -> {ok, Record} end, ShareDoc]),
    {ok, ShareId}.


update_share(Providers, NewShareDoc = #document{key = ShareId, value = NewRecord}) ->
    {_, []} = rpc:multicall(Providers, od_share, invalidate_cache, [ShareId]),
    {_, []} = rpc:multicall(Providers, od_share, update_cache, [ShareId, fun(_) -> {ok, NewRecord} end, NewShareDoc]),
    ok.


delete_share(Providers, ShareId) ->
    {_, []} = rpc:multicall(Providers, od_share, invalidate_cache, [ShareId]),
    ok.
