%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests concerning file streaming (download/upload) API
%%% (REST + gs).
%%% @end
%%%-------------------------------------------------------------------
-module(file_stream_api_test_SUITE).
-author("Bartosz Walkowicz").

-include("file_api_test_utils.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/graph_sync/gri.hrl").
-include_lib("ctool/include/http/codes.hrl").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    gui_download_file_test/1
]).

all() -> [
    gui_download_file_test
].


-define(DEFAULT_READ_BLOCK_SIZE, 1024).
-define(GUI_DOWNLOAD_CODE_EXPIRATION_SECONDS, 20).

-define(ATTEMPTS, 30).


%%%===================================================================
%%% Get attrs test functions
%%%===================================================================


gui_download_file_test(Config) ->
    [P1Node] = api_test_env:get_provider_nodes(p1, Config),
    [P2Node] = api_test_env:get_provider_nodes(p2, Config),
    Providers = [P2Node, P1Node],

    UserSessIdP1 = api_test_env:get_user_session_id(user3, p1, Config),
    SpaceOwnerSessIdP1 = api_test_env:get_user_session_id(user2, p1, Config),

    DirPath = filename:join(["/", ?SPACE_2, ?RANDOM_FILE_NAME()]),
    {ok, DirGuid} = api_test_utils:create_file(<<"dir">>, P1Node, UserSessIdP1, DirPath, 8#704),
    {ok, DirShareId} = lfm_proxy:create_share(P1Node, SpaceOwnerSessIdP1, {guid, DirGuid}, <<"share">>),
    DirShareGuid = file_id:guid_to_share_guid(DirGuid, DirShareId),
    ?assertMatch(
        {ok, #file_attr{shares = [DirShareId]}},
        api_test_utils:get_file_attrs(P2Node, DirGuid),
        ?ATTEMPTS
    ),

    FileSize = 4 * 1024,
    Content = crypto:strong_rand_bytes(FileSize),

    MemRef = api_test_memory:init(),

    SetupFun = build_download_file_setup_fun(MemRef, Content, Config),
    ValidateCallResultFun = build_get_download_url_validate_gs_call_fun(MemRef, Content, Config),
    VerifyFun = build_download_file_verify_fun(MemRef, Content, Config),

    ?assert(onenv_api_test_runner:run_tests(Config, [
        #scenario_spec{
            name = <<"Download file using gui endpoint and gs private api">>,
            type = gs,
            target_nodes = Providers,
            client_spec = #client_spec{
                correct = [
                    user2,  % space owner - doesn't need any perms
                    user3   % files owner
                ],
                unauthorized = [nobody],
                forbidden_not_in_space = [user1],
                forbidden_in_space = [user4]
            },
            setup_fun = SetupFun,
            prepare_args_fun = build_get_download_url_prepare_gs_args_fun(MemRef, normal_mode, private),
            validate_result_fun = ValidateCallResultFun,
            verify_fun = VerifyFun,
            data_spec = api_test_utils:add_file_id_errors_for_operations_available_in_share_mode(
                DirGuid, undefined, #data_spec{
                    bad_values = [{bad_id, DirGuid, ?ERROR_POSIX(?EISDIR)}]
                }
            )
        },
        #scenario_spec{
            name = <<"Download shared file using gui endpoint and gs public api">>,
            type = gs,
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SHARES,
            setup_fun = SetupFun,
            prepare_args_fun = build_get_download_url_prepare_gs_args_fun(MemRef, share_mode, public),
            validate_result_fun = ValidateCallResultFun,
            verify_fun = VerifyFun,
            data_spec = api_test_utils:add_file_id_errors_for_operations_available_in_share_mode(
                DirGuid, DirShareId, #data_spec{
                    bad_values = [{bad_id, DirShareGuid, ?ERROR_POSIX(?EISDIR)}]
                }
            )
        },
        #scenario_spec{
            name = <<"Download shared file using gui endpoint and gs private api">>,
            type = gs_with_shared_guid_and_aspect_private,
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SHARES,
            setup_fun = SetupFun,
            prepare_args_fun = build_get_download_url_prepare_gs_args_fun(MemRef, share_mode, private),
            validate_result_fun = fun(_TestCaseCtx, Result) ->
                ?assertEqual(?ERROR_UNAUTHORIZED, Result)
            end
        }
    ])).


%% @private
-spec build_get_download_url_prepare_gs_args_fun(
    api_test_memory:mem_ref(),
    TestMode :: normal_mode | share_mode,
    gri:scope()
) ->
    onenv_api_test_runner:prepare_args_fun().
build_get_download_url_prepare_gs_args_fun(MemRef, TestMode, Scope) ->
    fun(#api_test_ctx{data = Data0}) ->
        BareGuid = api_test_memory:get(MemRef, file_guid),
        FileGuid = case TestMode of
            normal_mode ->
                BareGuid;
            share_mode ->
                ShareId = api_test_memory:get(MemRef, share_id),
                file_id:guid_to_share_guid(BareGuid, ShareId)
        end,
        {GriId, Data1} = api_test_utils:maybe_substitute_bad_id(FileGuid, Data0),

        #gs_args{
            operation = get,
            gri = #gri{type = op_file, id = GriId, aspect = download_url, scope = Scope},
            data = Data1
        }
    end.


%% @private
-spec build_get_download_url_validate_gs_call_fun(
    api_test_memory:mem_ref(),
    FileContent :: binary(),
    onenv_api_test_runner:ct_config()
) ->
    onenv_api_test_runner:setup_fun().
build_get_download_url_validate_gs_call_fun(MemRef, ExpContent, Config) ->
    FileSize = size(ExpContent),
    FirstBlockFetchedSize = min(FileSize, ?DEFAULT_READ_BLOCK_SIZE),

    [FileCreationNode] = api_test_env:get_provider_nodes(p1, Config),
    [P2Node] = api_test_env:get_provider_nodes(p2, Config),

    fun(#api_test_ctx{node = DownloadNode}, Result) ->
        FileGuid = api_test_memory:get(MemRef, file_guid),

        % Getting file download url should cause first file block to be synced on target provider.
        ExpDist = case FileCreationNode == DownloadNode of
            true -> [{FileCreationNode, FileSize}];
            false -> [{FileCreationNode, FileSize}, {DownloadNode, FirstBlockFetchedSize}]
        end,
        assert_distribution([FileCreationNode, P2Node], FileGuid, ExpDist),

        {ok, #{<<"fileUrl">> := FileDownloadUrl}} = ?assertMatch({ok, #{}}, Result),
        [_, DownloadCode] = binary:split(FileDownloadUrl, [<<"/download/">>]),

        case rand:uniform(2) of
            1 ->
                % File download code should be still usable after unsuccessful download
                block_file_streaming(DownloadNode, ?ERROR_POSIX(?EAGAIN)),
                ?assertEqual(?ERROR_POSIX(?EAGAIN), download_file_with_gui_endpoint(DownloadNode, FileDownloadUrl)),
                unblock_file_streaming(DownloadNode),
                ?assertMatch({ok, _}, get_file_download_code_doc(DownloadNode, DownloadCode, memory)),

                % But first successful download should make it unusable
                ?assertEqual({ok, ExpContent}, download_file_with_gui_endpoint(DownloadNode, FileDownloadUrl)),
                ?assertMatch(?ERROR_NOT_FOUND, get_file_download_code_doc(DownloadNode, DownloadCode, memory), ?ATTEMPTS),
                ?assertEqual(?ERROR_BAD_VALUE_ID_NOT_FOUND(<<"code">>), download_file_with_gui_endpoint(DownloadNode, FileDownloadUrl)),

                api_test_memory:set(MemRef, download_succeeded, true);
            2 ->
                % File download code should be unusable after expiration period
                timer:sleep(timer:seconds(?GUI_DOWNLOAD_CODE_EXPIRATION_SECONDS)),

                % File download code should be deleted from db but stay in memory as couch
                % unfortunately doesn't remove expired docs from memory
                ?assertMatch(
                    ?ERROR_NOT_FOUND,
                    get_file_download_code_doc(DownloadNode, DownloadCode, disc),
                    ?ATTEMPTS
                ),
                ?assertMatch({ok, _}, get_file_download_code_doc(DownloadNode, DownloadCode, memory)),

                % Still after request, which will fail, it should be deleted also from memory
                ?assertEqual(?ERROR_BAD_VALUE_ID_NOT_FOUND(<<"code">>), download_file_with_gui_endpoint(DownloadNode, FileDownloadUrl)),
                ?assertMatch(?ERROR_NOT_FOUND, get_file_download_code_doc(DownloadNode, DownloadCode, memory)),

                api_test_memory:set(MemRef, download_succeeded, false)
        end
    end.


%% @private
-spec block_file_streaming(node(), errors:error()) -> ok.
block_file_streaming(OpNode, ErrorReturned) ->
    test_node_starter:load_modules([OpNode], [?MODULE]),
    ok = test_utils:mock_new(OpNode, http_download_utils),
    ok = test_utils:mock_expect(OpNode, http_download_utils, stream_file, fun(_, _, _, Req) ->
        http_req:send_error(ErrorReturned, Req)
    end).


%% @private
-spec unblock_file_streaming(node()) -> ok.
unblock_file_streaming(OpNode) ->
    ok = test_utils:mock_unload(OpNode).


%% @private
-spec get_file_download_code_doc(
    node(),
    file_download_code:code(),
    Location :: memory | disc
) ->
    {ok, file_download_code:doc()} | {error, term()}.
get_file_download_code_doc(Node, DownloadCode, Location) ->
    Ctx0 = rpc:call(Node, file_download_code, get_ctx, []),
    Ctx1 = case Location of
        memory -> Ctx0;
        disc -> Ctx0#{memory_driver => undefined}
    end,
    rpc:call(Node, datastore_model, get, [Ctx1, DownloadCode]).


%% @private
-spec download_file_with_gui_endpoint(node(), FileDownloadUrl :: binary()) ->
    {ok, Content :: binary()} | {error, term()}.
download_file_with_gui_endpoint(Node, FileDownloadUrl) ->
    CaCerts = op_test_rpc:get_cert_chain_pems(Node),
    Opts = [{ssl_options, [{cacerts, CaCerts}]}],

    case http_client:request(get, FileDownloadUrl, #{}, <<>>, Opts) of
        {ok, ?HTTP_200_OK, _RespHeaders, RespBody} ->
            {ok, RespBody};
        {ok, _RespCode, _RespHeaders, RespBody} ->
            errors:from_json(maps:get(<<"error">>, json_utils:decode(RespBody)));
        {error, _} = Error ->
            Error
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec build_download_file_setup_fun(
    api_test_memory:mem_ref(),
    FileContent :: binary(),
    onenv_api_test_runner:ct_config()
) ->
    onenv_api_test_runner:setup_fun().
build_download_file_setup_fun(MemRef, Content, Config) ->
    [P1Node] = api_test_env:get_provider_nodes(p1, Config),
    [P2Node] = api_test_env:get_provider_nodes(p2, Config),
    Providers = [P1Node, P2Node],

    UserSessIdP1 = api_test_env:get_user_session_id(user3, p1, Config),
    SpaceOwnerSessIdP1 = api_test_env:get_user_session_id(user2, p1, Config),

    FileSize = size(Content),

    fun() ->
        FilePath = filename:join(["/", ?SPACE_2, ?RANDOM_FILE_NAME()]),
        {ok, FileGuid} = api_test_utils:create_file(<<"file">>, P1Node, UserSessIdP1, FilePath, 8#704),
        {ok, ShareId} = lfm_proxy:create_share(P1Node, SpaceOwnerSessIdP1, {guid, FileGuid}, <<"share">>),
        api_test_utils:write_file(P1Node, UserSessIdP1, FileGuid, 0, Content),

        ?assertMatch(
            {ok, #file_attr{size = FileSize, shares = [ShareId]}},
            api_test_utils:get_file_attrs(P2Node, FileGuid),
            ?ATTEMPTS
        ),
        assert_distribution(Providers, FileGuid, [{P1Node, FileSize}]),

        api_test_memory:set(MemRef, file_guid, FileGuid),
        api_test_memory:set(MemRef, share_id, ShareId)
    end.


%% @private
-spec build_download_file_verify_fun(
    api_test_memory:mem_ref(),
    FileContent :: binary(),
    onenv_api_test_runner:ct_config()
) ->
    onenv_api_test_runner:verify_fun().
build_download_file_verify_fun(MemRef, Content, Config) ->
    [P1Node] = api_test_env:get_provider_nodes(p1, Config),
    [P2Node] = api_test_env:get_provider_nodes(p2, Config),
    Providers = [P1Node, P2Node],

    FileSize = size(Content),
    FirstBlockFetchedSize = min(FileSize, ?DEFAULT_READ_BLOCK_SIZE),

    fun
        (expected_failure, _) ->
            FileGuid = api_test_memory:get(MemRef, file_guid),
            assert_distribution(Providers, FileGuid, [{P1Node, FileSize}]);
        (expected_success, #api_test_ctx{node = DownloadNode}) ->
            FileGuid = api_test_memory:get(MemRef, file_guid),
            case P1Node == DownloadNode of
                true ->
                    assert_distribution(Providers, FileGuid, [{P1Node, FileSize}]);
                false ->
                    ExpDist = case api_test_memory:get(MemRef, download_succeeded) of
                        true -> [{P1Node, FileSize}, {DownloadNode, FileSize}];
                        false -> [{P1Node, FileSize}, {DownloadNode, FirstBlockFetchedSize}]
                    end,
                    assert_distribution(Providers, FileGuid, ExpDist)
            end
    end.


%% @private
-spec assert_distribution([node()], file_id:file_guid(), [{node(), non_neg_integer()}]) ->
    true | no_return().
assert_distribution(Nodes, FileGuid, ExpSizePerProvider) ->
    ExpDistribution = lists:sort(lists:map(fun({Node, ExpSize}) ->
        #{
            <<"blocks">> => [[0, ExpSize]],
            <<"providerId">> => op_test_rpc:get_provider_id(Node),
            <<"totalBlocksSize">> => ExpSize
        }
    end, ExpSizePerProvider)),

    FetchDistributionFun = fun(Node, Guid) ->
        {ok, Distribution} = lfm_proxy:get_file_distribution(Node, ?ROOT_SESS_ID, {guid, Guid}),
        lists:sort(lists:filter(fun(#{<<"totalBlocksSize">> := Size}) -> Size /= 0 end, Distribution))
    end,

    lists:foreach(fun(Node) ->
        ?assertEqual(ExpDistribution, FetchDistributionFun(Node, FileGuid), ?ATTEMPTS)
    end, Nodes),

    true.


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    ssl:start(),
    hackney:start(),
    api_test_env:init_per_suite(Config, #onenv_test_config{envs = [
        {op_worker, op_worker, [
            {fuse_session_grace_period_seconds, 24 * 60 * 60},
            {default_download_read_block_size, ?DEFAULT_READ_BLOCK_SIZE},

            % Ensure replica_synchronizer will not fetch more data than requested
            {minimal_sync_request, ?DEFAULT_READ_BLOCK_SIZE},
            {synchronizer_prefetch, false},

            {download_code_expiration_interval_seconds, ?GUI_DOWNLOAD_CODE_EXPIRATION_SECONDS}
        ]}
    ]}).


end_per_suite(_Config) ->
    hackney:stop(),
    ssl:stop().


init_per_testcase(gui_download_file_test = Case, Config) ->
    Providers = ?config(op_worker_nodes, Config),
    % TODO VFS-6828 - call needed to preload file_middleware module and add 'download_url' atom
    % to known/existing atoms. Otherwise gs_ws_handler may fail to decode this request (gri)
    % if it is the first request made.
    rpc:multicall(Providers, file_middleware, operation_supported, [get, download_url, private]),
    init_per_testcase(?DEFAULT_CASE(Case), Config);
init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 20}),
    lfm_proxy:init(Config).


end_per_testcase(gui_download_file_test, Config) ->
    lists:foreach(
        fun(OpNode) -> unblock_file_streaming(OpNode) end,
        ?config(op_worker_nodes, Config)
    ),
    end_per_testcase(default, Config);
end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).
