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

-include("file_metadata_api_test_utils.hrl").
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


-define(DEFAULT_READ_BLOCK_SIZE, 10485760).  % 10 MB.

-define(ATTEMPTS, 30).


%%%===================================================================
%%% Get attrs test functions
%%%===================================================================


gui_download_file_test(Config) ->
    [P1Node] = api_test_env:get_provider_nodes(p1, Config),
    [P2Node] = api_test_env:get_provider_nodes(p2, Config),
    Providers = [P2Node, P1Node],

    SpaceOwnerSessIdP1 = api_test_env:get_user_session_id(user2, p1, Config),
    UserSessIdP1 = api_test_env:get_user_session_id(user3, p1, Config),
    UserSessIdP2 = api_test_env:get_user_session_id(user3, p2, Config),

    DirPath = filename:join(["/", ?SPACE_2, ?RANDOM_FILE_NAME()]),
    {ok, DirGuid} = api_test_utils:create_file(<<"dir">>, P1Node, UserSessIdP1, DirPath, 8#707),
    api_test_utils:wait_for_file_sync(P2Node, UserSessIdP2, DirGuid),

    FilePath = filename:join(["/", ?SPACE_2, ?RANDOM_FILE_NAME()]),
    {ok, FileGuid} = api_test_utils:create_file(<<"file">>, P1Node, UserSessIdP1, FilePath, 8#707),
    {ok, FileShareId} = lfm_proxy:create_share(P1Node, SpaceOwnerSessIdP1, {guid, FileGuid}, <<"share">>),
    ShareFileGuid = file_id:guid_to_share_guid(FileGuid, FileShareId),
    api_test_utils:wait_for_file_sync(P2Node, UserSessIdP2, FileGuid),

    FileSize = 20 * 1024 * 1024,  % 20 MB
    Content = crypto:strong_rand_bytes(FileSize),

    SetupFun = fun() ->
        % Overwrite file content to invalidate block on other providers
        api_test_utils:write_file(P1Node, UserSessIdP1, FileGuid, 0, Content),
        assert_distribution(P2Node, FileGuid, [{P1Node, FileSize}])
    end,
    ValidateCallResultFun = build_get_attrs_validate_gs_call_fun(P1Node, FileGuid, Content),

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
            prepare_args_fun = build_get_download_url_prepare_gs_args_fun(FileGuid, private),
            validate_result_fun = ValidateCallResultFun,
            data_spec = api_test_utils:add_file_id_errors_for_operations_available_in_share_mode(
                FileGuid, undefined, undefined
            )
        },
        #scenario_spec{
            name = <<"Download shared file using gui endpoint and gs public api">>,
            type = gs,
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SHARES,
            setup_fun = SetupFun,
            prepare_args_fun = build_get_download_url_prepare_gs_args_fun(ShareFileGuid, public),
            validate_result_fun = ValidateCallResultFun,
            data_spec = api_test_utils:add_file_id_errors_for_operations_available_in_share_mode(
                FileGuid, FileShareId, undefined
            )
        },
        #scenario_spec{
            name = <<"Download shared file using gui endpoint and gs private api">>,
            type = gs_with_shared_guid_and_aspect_private,
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SHARES,
            prepare_args_fun = build_get_download_url_prepare_gs_args_fun(ShareFileGuid, private),
            validate_result_fun = fun(_TestCaseCtx, Result) ->
                ?assertEqual(?ERROR_UNAUTHORIZED, Result)
            end
        }
    ])).


%% @private
-spec build_get_download_url_prepare_gs_args_fun(file_id:file_guid(), gri:scope()) ->
    onenv_api_test_runner:prepare_args_fun().
build_get_download_url_prepare_gs_args_fun(FileGuid, Scope) ->
    fun(#api_test_ctx{data = Data0}) ->
        {GriId, Data1} = api_test_utils:maybe_substitute_bad_id(FileGuid, Data0),

        #gs_args{
            operation = get,
            gri = #gri{type = op_file, id = GriId, aspect = download_url, scope = Scope},
            data = Data1
        }
    end.


%% @private
build_get_attrs_validate_gs_call_fun(FileCreationNode, FileGuid, ExpContent) ->
    FileSize = size(ExpContent),
    FirstBlockFetchedSize = min(FileSize, ?DEFAULT_READ_BLOCK_SIZE),

    fun(#api_test_ctx{node = TargetNode}, Result) ->
        % Getting file download url should cause first file block to be synced on target provider.
        ExpDist = case FileCreationNode == TargetNode of
            true -> [{FileCreationNode, FileSize}];
            false -> [{FileCreationNode, FileSize}, {TargetNode, FirstBlockFetchedSize}]
        end,
        assert_distribution(TargetNode, FileGuid, ExpDist),

        % Downloading file using received url should succeed (and fully sync file content)
        % without any auth with the first use.
        {ok, #{<<"fileUrl">> := FileDownloadUrl}} = ?assertMatch({ok, #{}}, Result),
        ?assertEqual({ok, ExpContent}, download_file_with_gui_endpoint(TargetNode, FileDownloadUrl)),
        assert_distribution(
            TargetNode, FileGuid, lists:usort([{FileCreationNode, FileSize}, {TargetNode, FileSize}])
        ),

        % But second try should fail as file download url is only one time use thing
        ?assertEqual(?ERROR_BAD_DATA(<<"code">>), download_file_with_gui_endpoint(TargetNode, FileDownloadUrl))
    end.


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
-spec assert_distribution(node(), file_id:file_guid(), [{node(), non_neg_integer()}]) -> ok.
assert_distribution(Node, FileGuid, ExpSizePerProvider) ->
    ExpDistribution = lists:sort(lists:map(fun({Node, ExpSize}) ->
        #{
            <<"blocks">> => [[0, ExpSize]],
            <<"providerId">> => op_test_rpc:get_provider_id(Node),
            <<"totalBlocksSize">> => ExpSize
        }
    end, ExpSizePerProvider)),

    FetchDistributionFun = fun(Guid) ->
        {ok, Distribution} = lfm_proxy:get_file_distribution(Node, ?ROOT_SESS_ID, {guid, Guid}),
        lists:sort(lists:filter(fun(#{<<"totalBlocksSize">> := Size}) -> Size /= 0 end, Distribution))
    end,

    ?assertEqual(ExpDistribution, FetchDistributionFun(FileGuid), ?ATTEMPTS),
    ok.


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    ssl:start(),
    hackney:start(),
    api_test_env:init_per_suite(Config, #onenv_test_config{envs = [
        {op_worker, op_worker, [
            {fuse_session_grace_period_seconds, 24 * 60 * 60},
            {default_download_read_block_size, ?DEFAULT_READ_BLOCK_SIZE}
        ]}
    ]}).


end_per_suite(_Config) ->
    hackney:stop(),
    ssl:stop().


init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 10}),
    lfm_proxy:init(Config).


end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).
