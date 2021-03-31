%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests concerning file streaming API (REST + gs).
%%% @end
%%%-------------------------------------------------------------------
-module(api_file_stream_test_SUITE).
-author("Bartosz Walkowicz").

-include("api_file_test_utils.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("onenv_file_test_utils.hrl").
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
    gui_download_file_test/1,
    gui_download_dir_test/1,
    gui_download_multiple_files_test/1,
    gui_download_different_filetypes_test/1,
    gui_large_dir_download_test/1,
    gui_download_files_between_spaces_test/1,
    rest_download_file_test/1,
    rest_download_dir_test/1
]).

groups() -> [
    {all_tests, [parallel], [
        gui_download_file_test,
        gui_download_dir_test,
        gui_download_multiple_files_test,
        gui_download_different_filetypes_test,
        gui_large_dir_download_test,
        gui_download_files_between_spaces_test,
        rest_download_file_test,
        rest_download_dir_test
    ]}
].

all() -> [
    {group, all_tests}
].

-define(DEFAULT_READ_BLOCK_SIZE, 1024).
-define(GUI_DOWNLOAD_CODE_EXPIRATION_SECONDS, 20).

-define(ATTEMPTS, 30).

-define(FILESIZE, 4 * ?DEFAULT_READ_BLOCK_SIZE).
-define(CONTENT, ?CONTENT(?FILESIZE)).
-define(CONTENT(FileSize), crypto:strong_rand_bytes(FileSize)).

-type test_mode() :: normal_mode | share_mode.

-type offset() :: non_neg_integer().
-type size() :: non_neg_integer().
-type files_strategy() :: check_files_content | no_files.

%%%===================================================================
%%% GUI download test functions
%%%===================================================================

gui_download_file_test(Config) ->
    ClientSpec = #client_spec{
        correct = [
            user2,  % space owner - doesn't need any perms
            user3   % files owner
        ],
        unauthorized = [nobody],
        forbidden_not_in_space = [user1],
        forbidden_in_space = [user4]
    },
    FileSpec = #file_spec{mode = 8#604, content = ?CONTENT, shares = [#share_spec{}]},
    gui_download_test_base(Config, FileSpec, ClientSpec, <<"File">>).

gui_download_dir_test(Config) ->
    ClientSpec = #client_spec{
        correct = [
            user2,  % space owner - doesn't need any perms
            user3,  % files owner
            user4   % forbidden user in space is allowed to download an empty tarball - files without access are ignored
        ],
        unauthorized = [nobody],
        forbidden_not_in_space = [user1]
    },
    DirSpec = #dir_spec{mode = 8#705, shares = [#share_spec{}], children = [#dir_spec{}, #file_spec{content = ?CONTENT}]},
    gui_download_test_base(Config, DirSpec, ClientSpec, <<"Dir">>).

gui_download_multiple_files_test(Config) ->
    ClientSpec = #client_spec{
        correct = [
            user2,  % space owner - doesn't need any perms
            user3,  % files owner
            user4   % forbidden user in space is allowed to download an empty tarball - files without access are ignored
        ],
        unauthorized = [nobody],
        forbidden_not_in_space = [user1]
    },
    DirSpec = [
        #file_spec{mode = 8#604, shares = [#share_spec{}], content = ?CONTENT},
        #file_spec{mode = 8#604, shares = [#share_spec{}], content = ?CONTENT},
        #file_spec{mode = 8#604, shares = [#share_spec{}], content = ?CONTENT},
        #file_spec{mode = 8#604, shares = [#share_spec{}], content = ?CONTENT}
    ],
    gui_download_test_base(Config, DirSpec, ClientSpec, <<"Multiple files">>).

gui_download_different_filetypes_test(Config) ->
    ClientSpec = #client_spec{
        correct = [
            user2,  % space owner - doesn't need any perms
            user3,  % files owner
            user4   % forbidden user in space is allowed to download an empty tarball - files without access are ignored
        ],
        unauthorized = [nobody],
        forbidden_not_in_space = [user1]
    },
    DirSpec = [
        #symlink_spec{shares = [#share_spec{}], link_path = <<"some_link_path">>}, 
        #dir_spec{mode = 8#705, shares = [#share_spec{}], children = [#dir_spec{}, #file_spec{content = ?CONTENT}]},
        #file_spec{mode = 8#604, shares = [#share_spec{}], content = ?CONTENT}
    ],
    gui_download_test_base(Config, DirSpec, ClientSpec, <<"Different filetypes">>).


gui_large_dir_download_test(Config) ->
    ClientSpec = #client_spec{
        correct = [
            user2  % space owner - doesn't need any perms
        ]
    },
    ChildrenSpecGen = fun
        F(0) -> [];
        F(Depth) -> 
            Children = F(Depth - 1),
            [#dir_spec{children = Children}, #dir_spec{children = Children}, #file_spec{content = ?CONTENT}]
    end,
    DirSpec = #dir_spec{mode = 8#705, shares = [#share_spec{}], children = ChildrenSpecGen(8)},
    gui_download_test_base(Config, DirSpec, ClientSpec, <<"Large file">>).


gui_download_files_between_spaces_test(_Config) ->
    ClientSpec = #client_spec{
        correct = [
            user3,  % files owner
            user4   % forbidden user in space is allowed to download an empty tarball - files without access are ignored
        ],
        unauthorized = [nobody]
    },
    MemRef = api_test_memory:init(),
    
    SpaceId1 = oct_background:get_space_id(space_krk_par),
    SpaceId2 = oct_background:get_space_id(space_krk),
    Provider = oct_background:get_provider_id(krakow),
    Spec = [
        #file_spec{mode = 8#604, shares = [#share_spec{}], content = ?CONTENT},
        #file_spec{mode = 8#604, shares = [#share_spec{}], content = ?CONTENT}
    ],
    
    SetupFun = fun() ->
        Object = lists:map(fun(SingleFileSpec) ->
            onenv_file_test_utils:create_and_sync_file_tree(user3, SpaceId1, SingleFileSpec, krakow),
            onenv_file_test_utils:create_and_sync_file_tree(user3, SpaceId2, SingleFileSpec, krakow)
        end, Spec),
        api_test_memory:set(MemRef, file_tree_object, Object)
    end,
    ValidateCallResultFun = build_get_download_url_validate_gs_call_fun(MemRef),
    
    DataSpec = #data_spec{
        required = [<<"file_ids">>],
        % correct values are injected in function maybe_inject_guids/3 based on provided FileTreeSpec
        correct_values = #{<<"file_ids">> => [injected_guids]}
    },
    ?assert(onenv_api_test_runner:run_tests([
        #scenario_spec{
            name = <<"Download files betweenn spaces using gui endpoint and gs private api">>,
            type = gs,
            target_nodes = [Provider],
            client_spec = ClientSpec,
            setup_fun = SetupFun,
            prepare_args_fun = build_get_download_url_prepare_gs_args_fun(MemRef, normal_mode, private),
            validate_result_fun = ValidateCallResultFun,
            data_spec = DataSpec
        }
    ])).
        

%%%===================================================================
%%% GUI download test base and spec functions
%%%===================================================================

%% @private
gui_download_test_base(Config, FileTreeSpec, ClientSpec, ScenarioPrefix) ->
    Providers = ?config(op_worker_nodes, Config),

    {_FileType, _DirPath, DirGuid, DirShareId} = api_test_utils:create_and_sync_shared_file_in_space_krk_par(
        <<"dir">>, 8#704
    ),

    MemRef = api_test_memory:init(),

    SetupFun = build_download_file_setup_fun(MemRef, FileTreeSpec), 
    ValidateCallResultFun = build_get_download_url_validate_gs_call_fun(MemRef), 
    VerifyFun = build_download_file_with_gui_endpoint_verify_fun(MemRef),

    DataSpec = #data_spec{
        required = [<<"file_ids">>],
        % correct values are injected in function maybe_inject_guids/3 based on provided FileTreeSpec
        correct_values = #{<<"file_ids">> => [injected_guids]}, 
        bad_values = [
            {<<"file_ids">>, [<<"incorrect_guid">>], ?ERROR_BAD_VALUE_IDENTIFIER(<<"file_ids">>)},
            {<<"file_ids">>, <<"not_a_list">>, ?ERROR_BAD_VALUE_LIST_OF_BINARIES(<<"file_ids">>)}
        ]
    },
    ?assert(onenv_api_test_runner:run_tests([
        #scenario_spec{
            name = <<"[", ScenarioPrefix/binary, "] Download file using gui endpoint and gs private api">>,
            type = gs,
            target_nodes = Providers,
            client_spec = ClientSpec,
            setup_fun = SetupFun,
            prepare_args_fun = build_get_download_url_prepare_gs_args_fun(MemRef, normal_mode, private),
            validate_result_fun = ValidateCallResultFun,
            verify_fun = VerifyFun,
            data_spec = api_test_utils:add_file_id_errors_for_operations_available_in_share_mode(
                <<"file_ids">>, DirGuid, undefined, DataSpec
            )
        },
        #scenario_spec{
            name = <<"[", ScenarioPrefix/binary, "] Download shared file using gui endpoint and gs public api">>,
            type = gs,
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SHARES,
            setup_fun = SetupFun,
            prepare_args_fun = build_get_download_url_prepare_gs_args_fun(MemRef, share_mode, public),
            validate_result_fun = ValidateCallResultFun,
            verify_fun = VerifyFun,
            data_spec = api_test_utils:add_file_id_errors_for_operations_available_in_share_mode(
                <<"file_ids">>, DirGuid, DirShareId, DataSpec
            )
        },
        #scenario_spec{
            name = <<"[", ScenarioPrefix/binary, "] Download shared file using gui endpoint and gs private api">>,
            type = gs_with_shared_guid_and_aspect_private,
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SHARES,
            setup_fun = SetupFun,
            prepare_args_fun = build_get_download_url_prepare_gs_args_fun(MemRef, share_mode, private),
            validate_result_fun = fun(#api_test_ctx{client = Client}, Result) ->
                case Client of
                    ?NOBODY -> ?assertEqual(?ERROR_UNAUTHORIZED, Result);
                    _ -> ?assertEqual(?ERROR_FORBIDDEN, Result)
                end
            end,
            data_spec = DataSpec
        }
    ])).


%% @private
-spec build_get_download_url_prepare_gs_args_fun(api_test_memory:mem_ref(), test_mode(), gri:scope()) ->
    onenv_api_test_runner:prepare_args_fun().
build_get_download_url_prepare_gs_args_fun(MemRef, TestMode, Scope) ->
    fun(#api_test_ctx{data = Data0}) ->
        api_test_memory:set(MemRef, scope, Scope),
        Data1 = maybe_inject_guids(MemRef, Data0, TestMode),
        #gs_args{
            operation = get,
            gri = #gri{type = op_file, aspect = download_url, scope = Scope},
            data = Data1
        }
    end.


%% @private
maybe_inject_guids(MemRef, Data, TestMode) when is_map(Data) ->
    case maps:get(<<"file_ids">>, Data, undefined) of
        injected_guids ->
            FileTreeObject = api_test_memory:get(MemRef, file_tree_object),
            Guids = lists:map(fun(#object{guid = Guid, shares = Shares}) ->
                case TestMode of
                    normal_mode -> Guid;
                    share_mode ->
                        case Shares of
                            [ShareId | _] -> file_id:guid_to_share_guid(Guid, ShareId);
                            _ -> Guid
                        end  
                end
            end, utils:ensure_list(FileTreeObject)),
            {FinalGuids, UpdatedData} = case maps:take(bad_id, Data) of
                {BadId, LeftoverData} -> {[BadId], LeftoverData};
                error -> {Guids, Data}
            end,
            UpdatedData#{<<"file_ids">> => FinalGuids};
        _ -> Data
    end;
maybe_inject_guids(_MemRef, Data, _TestMode) ->
    Data.


%% @private
-spec build_get_download_url_validate_gs_call_fun(api_test_memory:mem_ref()) -> 
    onenv_api_test_runner:validate_call_result_fun().
build_get_download_url_validate_gs_call_fun(MemRef) -> 
    fun(#api_test_ctx{node = DownloadNode, client = Client}, Result) ->
        [#object{guid = Guid} | _] = FileTreeObject = api_test_memory:get(MemRef, file_tree_object),

        {ok, #{<<"fileUrl">> := FileDownloadUrl}} = ?assertMatch({ok, #{}}, Result),
        [_, DownloadCode] = binary:split(FileDownloadUrl, [<<"/download/">>]),

        case rand:uniform(2) of
            1 ->
                User4Id = oct_background:get_user_id(user4),
                % File download code should be still usable after unsuccessful download
                case Client of
                    % user4 does not have access to files, so list of files to download passed 
                    % to file_download_utils is empty, hence it cannot be blocked for specific guid
                    ?USER(User4Id) -> ok;
                    _ ->
                        block_file_streaming(DownloadNode, Guid),
                        ?assertEqual(?ERROR_POSIX(?EAGAIN), download_file_with_gui_endpoint(DownloadNode, FileDownloadUrl)),
                        unblock_file_streaming(DownloadNode, Guid),
                        ?assertMatch({ok, _}, get_file_download_code_doc(DownloadNode, DownloadCode, memory))
                end,

                % But first successful download should make it unusable
                case FileTreeObject of
                    [#object{type = ?REGULAR_FILE_TYPE, content = ExpContent1}] ->
                        ?assertEqual({ok, ExpContent1}, download_file_with_gui_endpoint(DownloadNode, FileDownloadUrl));
                    _ ->
                        {ok, Bytes} = ?assertMatch({ok, _}, download_file_with_gui_endpoint(DownloadNode, FileDownloadUrl)),
                        % user4 does not have access to files, so downloaded tarball contains only dir entry
                        lists:foreach(fun(Object) ->
                            case {Client, api_test_memory:get(MemRef, scope)} of
                                {?USER(User4Id), private} ->
                                    check_tarball(Bytes, Object, no_files);
                                _ ->
                                    check_tarball(Bytes, Object)
                            end
                        end, utils:ensure_list(FileTreeObject))
                end,
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
-spec block_file_streaming(node(), fslogic_worker:file_guid()) -> ok.
block_file_streaming(OpNode, Guid) ->
    {Uuid, _, _} = file_id:unpack_share_guid(Guid),
    rpc:call(OpNode, node_cache, put, [{block_file, Uuid}, true]).


%% @private
-spec unblock_file_streaming(node(), fslogic_worker:file_guid()) -> ok.
unblock_file_streaming(OpNode, Guid) ->
    {Uuid, _, _} = file_id:unpack_share_guid(Guid),
    rpc:call(OpNode, node_cache, clear, [{block_file, Uuid}]).


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
    CaCerts = opw_test_rpc:get_cert_chain_ders(Node),
    Opts = [{ssl_options, [{cacerts, CaCerts}]}],

    case http_client:request(get, FileDownloadUrl, #{}, <<>>, Opts) of
        {ok, ?HTTP_200_OK, _RespHeaders, RespBody} ->
            {ok, RespBody};
        {ok, _RespCode, _RespHeaders, RespBody} ->
            errors:from_json(maps:get(<<"error">>, json_utils:decode(RespBody)));
        {error, _} = Error ->
            Error
    end.


%% @private
-spec build_download_file_with_gui_endpoint_verify_fun(api_test_memory:mem_ref()) ->
    onenv_api_test_runner:verify_fun().
build_download_file_with_gui_endpoint_verify_fun(MemRef) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    Providers = [P1Node, P2Node],

    fun(ExpTestResult, ApiTestCtx) ->
        FileTreeObject = api_test_memory:get(MemRef, file_tree_object),
        case FileTreeObject of
            [#object{type = ?REGULAR_FILE_TYPE, guid = FileGuid, content = Content}] ->
                FileSize = size(Content),
                check_single_file_download_distribution(MemRef, ExpTestResult, ApiTestCtx, FileGuid, FileSize, Providers, P1Node);
            _ ->
                ExpTestResult1 = case {ExpTestResult, api_test_memory:get(MemRef, download_succeeded, undefined)} of
                    {expected_failure, _} ->  expected_failure;
                    {_, false} ->  expected_failure;
                    {_, true} ->  expected_success
                end,
                lists:foreach(fun(Object) ->
                    check_tarball_download_distribution(MemRef, ExpTestResult1, ApiTestCtx, Object, Providers, P1Node)
                end, utils:ensure_list(FileTreeObject))
        end
    end.


%% @private
-spec check_single_file_download_distribution(
    api_test_memory:mem_ref(), expected_success | expected_failure, onenv_api_test_runner:api_test_ctx(), 
    file_id:guid(), file_meta:size(), [oneprovider:id()], node()
) -> ok.
check_single_file_download_distribution(_MemRef, expected_failure, _, FileGuid, FileSize, Providers, P1Node) ->
    file_test_utils:await_distribution(Providers, FileGuid, [{P1Node, FileSize}]);
check_single_file_download_distribution(_MemRef, expected_success, #api_test_ctx{node = P1Node}, FileGuid, FileSize, Providers, P1Node) ->
    file_test_utils:await_distribution(Providers, FileGuid, [{P1Node, FileSize}]);
check_single_file_download_distribution(MemRef, expected_success, #api_test_ctx{node = DownloadNode}, FileGuid, FileSize, Providers, P1Node) ->
    FirstBlockFetchedSize = min(FileSize, ?DEFAULT_READ_BLOCK_SIZE),
    ExpDist = case api_test_memory:get(MemRef, download_succeeded) of
        true -> [{P1Node, FileSize}, {DownloadNode, FileSize}];
        false -> [{P1Node, FileSize}, {DownloadNode, FirstBlockFetchedSize}]
    end, 
    file_test_utils:await_distribution(Providers, FileGuid, ExpDist).


%% @private
-spec check_tarball_download_distribution(
    api_test_memory:mem_ref(), expected_success | expected_failure, onenv_api_test_runner:api_test_ctx(),
    onenv_file_test_utils:object_spec(), [oneprovider:id()], node()
) -> ok.
check_tarball_download_distribution(_MemRef, _ExpTestResult, _ApiTestCtx, #object{type = ?SYMLINK_TYPE}, _Providers, _P1Node) ->
    ok;
check_tarball_download_distribution(_MemRef, expected_failure, _, #object{type = ?REGULAR_FILE_TYPE} = Object, Providers, P1Node) ->
    #object{guid = FileGuid, content = Content} = Object,
    FileSize = size(Content),
    file_test_utils:await_distribution(Providers, FileGuid, [{P1Node, FileSize}]);
check_tarball_download_distribution(_MemRef, expected_success, #api_test_ctx{node = P1Node}, #object{type = ?REGULAR_FILE_TYPE} = Object, Providers, P1Node) ->
    #object{guid = FileGuid, content = Content} = Object,
    FileSize = size(Content),
    file_test_utils:await_distribution(Providers, FileGuid, [{P1Node, FileSize}]);
check_tarball_download_distribution(MemRef, expected_success, ApiTestCtx, #object{type = ?REGULAR_FILE_TYPE} = Object, Providers, P1Node) ->
    #api_test_ctx{node = DownloadNode, client = Client} = ApiTestCtx,
    #object{guid = FileGuid, content = Content} = Object,
    FileSize = size(Content),
    User4Id = oct_background:get_user_id(user4),
    ExpDist = case {Client, api_test_memory:get(MemRef, scope)} of
        {?USER(User4Id), private} -> [{P1Node, FileSize}];
        _ -> [{P1Node, FileSize}, {DownloadNode, FileSize}]
    end, 
    file_test_utils:await_distribution(Providers, FileGuid, ExpDist);
check_tarball_download_distribution(MemRef, ExpTestResult, ApiTestCtx, #object{type = ?DIRECTORY_TYPE, children = Children}, Providers, P1Node) ->
    lists:foreach(fun(Child) ->
        check_tarball_download_distribution(MemRef, ExpTestResult, ApiTestCtx, Child, Providers, P1Node)
    end, Children).


%%%===================================================================
%%% REST download test functions
%%%===================================================================

rest_download_file_test(Config) ->
    Providers = ?config(op_worker_nodes, Config),
    
    {_FileType, _DirPath, DirGuid, DirShareId} = api_test_utils:create_and_sync_shared_file_in_space_krk_par(
        <<"dir">>, 8#704
    ),
    
    MemRef = api_test_memory:init(),
    
    FileSize = 4 * ?DEFAULT_READ_BLOCK_SIZE,
    SetupFun = build_download_file_setup_fun(MemRef, #file_spec{mode = 8#604, content = ?CONTENT(FileSize), shares = [#share_spec{}]}),
    ValidateCallResultFun = build_rest_download_file_validate_call_fun(MemRef, Config),
    VerifyFun = build_rest_download_file_verify_fun(MemRef, FileSize),
    
    AllRangesToTest = [
        {<<"bytes=10-20">>, ?HTTP_206_PARTIAL_CONTENT, [{10, 11}]},
        {
            <<
                "bytes=",
                (integer_to_binary(FileSize - 100))/binary,
                "-",
                (integer_to_binary(FileSize + 100))/binary
            >>,
            ?HTTP_206_PARTIAL_CONTENT, [{FileSize - 100, 100}]
        },
        {<<"bytes=100-300,500-500,-300">>, ?HTTP_206_PARTIAL_CONTENT, [
            {100, 201}, {500, 1}, {FileSize - 300, 300}
        ]},
        
        {<<"unicorns">>, ?HTTP_416_RANGE_NOT_SATISFIABLE},
        {<<"bytes:5-10">>, ?HTTP_416_RANGE_NOT_SATISFIABLE},
        {<<"bytes=5=10">>, ?HTTP_416_RANGE_NOT_SATISFIABLE},
        {<<"bytes=-15-10">>, ?HTTP_416_RANGE_NOT_SATISFIABLE},
        {<<"bytes=10-5">>, ?HTTP_416_RANGE_NOT_SATISFIABLE},
        {<<"bytes=-5-">>, ?HTTP_416_RANGE_NOT_SATISFIABLE},
        {<<"bytes=10--5">>, ?HTTP_416_RANGE_NOT_SATISFIABLE},
        {<<"bytes=10-15-">>, ?HTTP_416_RANGE_NOT_SATISFIABLE},
        
        {<<"bytes=5000000-5100000">>, ?HTTP_416_RANGE_NOT_SATISFIABLE}
    ],
    AllRangesToTestNum = length(AllRangesToTest),
    
    % Randomly split range to test so to shorten test execution time by not
    % repeating every combination several times
    RangesToTestPart1 = lists_utils:random_sublist(
        AllRangesToTest, AllRangesToTestNum div 2, AllRangesToTestNum div 2
    ),
    RangesToTestPart2 = AllRangesToTest -- RangesToTestPart1,
    
    ?assert(onenv_api_test_runner:run_tests([
        #scenario_spec{
            name = <<"Download file using rest endpoint">>,
            type = rest,
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SPACE_KRK_PAR,

            setup_fun = SetupFun,
            prepare_args_fun = build_rest_download_prepare_args_fun(MemRef, normal_mode),
            validate_result_fun = ValidateCallResultFun,
            verify_fun = VerifyFun,

            data_spec = api_test_utils:add_file_id_errors_for_operations_available_in_share_mode(
                DirGuid, undefined, #data_spec{
                    optional = [<<"range">>],
                    correct_values = #{<<"range">> => RangesToTestPart1}
                }
            )
        },
        #scenario_spec{
            name = <<"Download shared file using rest endpoint">>,
            type = {rest_with_shared_guid, file_id:guid_to_space_id(DirGuid)},
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SHARES,
            
            setup_fun = SetupFun,
            prepare_args_fun = build_rest_download_prepare_args_fun(MemRef, share_mode),
            validate_result_fun = ValidateCallResultFun,
            verify_fun = VerifyFun,
            
            data_spec = api_test_utils:add_file_id_errors_for_operations_available_in_share_mode(
                DirGuid, DirShareId, #data_spec{
                    optional = [<<"range">>],
                    correct_values = #{<<"range">> => RangesToTestPart2}
                }
            )
        }
    ])).


rest_download_dir_test(Config) ->
    Providers = ?config(op_worker_nodes, Config),
    
    {_FileType, _DirPath, DirGuid, DirShareId} = api_test_utils:create_and_sync_shared_file_in_space_krk_par(
        <<"dir">>, 8#704
    ),
    
    MemRef = api_test_memory:init(),
    
    DirSpec = #dir_spec{mode = 8#705, shares = [#share_spec{}], children = [
        #symlink_spec{link_path = <<"link_path">>}, 
        #dir_spec{}, 
        #file_spec{content = ?CONTENT}
    ]},
    SetupFun = build_download_file_setup_fun(MemRef, DirSpec),
    ValidateCallResultFun = fun(#api_test_ctx{client = Client}, {ok, RespCode, _RespHeaders, RespBody}) ->
        ?assertEqual(?HTTP_200_OK, RespCode),
        [FileTreeObject] = api_test_memory:get(MemRef, file_tree_object),
        User4Id = oct_background:get_user_id(user4),
        case {Client, api_test_memory:get(MemRef, test_mode)} of
            {?USER(User4Id), normal_mode} -> check_tarball(RespBody, FileTreeObject, no_files);
            _ -> check_tarball(RespBody, FileTreeObject)
        end
    end,
    
    ?assert(onenv_api_test_runner:run_tests([
        #scenario_spec{
            name = <<"Download dir using rest endpoint">>,
            type = rest,
            target_nodes = Providers,
            client_spec = #client_spec{
                correct = [
                    user2,  % space owner - doesn't need any perms
                    user3,  % files owner
                    user4   % forbidden user in space is allowed to download an empty tarball - files without access are ignored
                ],
                unauthorized = [nobody],
                forbidden_not_in_space = [user1]
            },
            
            setup_fun = SetupFun,
            prepare_args_fun = build_rest_download_prepare_args_fun(MemRef, normal_mode),
            validate_result_fun = ValidateCallResultFun,
            
            data_spec = api_test_utils:add_file_id_errors_for_operations_available_in_share_mode(
                DirGuid, undefined, #data_spec{})
        },
        #scenario_spec{
            name = <<"Download shared dir using rest endpoint">>,
            type = {rest_with_shared_guid, file_id:guid_to_space_id(DirGuid)},
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SHARES,
            
            setup_fun = SetupFun,
            prepare_args_fun = build_rest_download_prepare_args_fun(MemRef, share_mode),
            validate_result_fun = ValidateCallResultFun,
            
            data_spec = api_test_utils:add_file_id_errors_for_operations_available_in_share_mode(
                DirGuid, DirShareId, #data_spec{}
            )
        }
    ])).


%%%===================================================================
%%% REST download spec functions
%%%===================================================================

%% @private
-spec build_rest_download_prepare_args_fun(api_test_memory:mem_ref(), test_mode()) ->
    onenv_api_test_runner:prepare_args_fun().
build_rest_download_prepare_args_fun(MemRef, TestMode) ->
    fun(#api_test_ctx{data = Data0}) ->
        api_test_memory:set(MemRef, test_mode, TestMode),
        [#object{guid = Guid, shares = Shares}] = api_test_memory:get(MemRef, file_tree_object),
        FileGuid = case TestMode of
            normal_mode -> Guid;
            share_mode ->
                case Shares of
                    [ShareId | _] -> file_id:guid_to_share_guid(Guid, ShareId);
                    _ -> Guid
                end
        end,
        {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),
        {Id, Data1} = api_test_utils:maybe_substitute_bad_id(FileObjectId, Data0),

        #rest_args{
            method = get,
            path = <<"data/", Id/binary, "/content">>,
            headers = case maps:get(<<"range">>, Data1, undefined) of
                undefined -> #{};
                Range -> #{<<"range">> => element(1, Range)}
            end
        }
    end.


%% @private
-spec build_rest_download_file_validate_call_fun(
    api_test_memory:mem_ref(),
    onenv_api_test_runner:ct_config()
) ->
    onenv_api_test_runner:validate_call_result_fun().
build_rest_download_file_validate_call_fun(MemRef, _Config) ->
    fun(#api_test_ctx{data = Data}, {ok, RespCode, RespHeaders, RespBody}) ->
        [#object{content = ExpContent}] = api_test_memory:get(MemRef, file_tree_object),
        FileSize = size(ExpContent),
        FileSizeBin = integer_to_binary(FileSize),
        case maps:get(<<"range">>, Data, undefined) of
            undefined ->
                ?assertEqual(?HTTP_200_OK, RespCode),
                ?assertEqual(ExpContent, RespBody);

            {_, ?HTTP_206_PARTIAL_CONTENT, [{RangeStart, RangeLen}]} ->
                ExpContentRange = str_utils:format_bin("bytes ~B-~B/~B", [
                    RangeStart, RangeStart + RangeLen - 1, FileSize
                ]),
                ExpPartialContent = binary:part(ExpContent, RangeStart, RangeLen),

                ?assertEqual(?HTTP_206_PARTIAL_CONTENT, RespCode),
                ?assertMatch(#{?HDR_CONTENT_RANGE := ExpContentRange}, RespHeaders),
                ?assertEqual(ExpPartialContent, RespBody);

            {_, ?HTTP_206_PARTIAL_CONTENT, Ranges} ->
                ?assertEqual(?HTTP_206_PARTIAL_CONTENT, RespCode),

                #{?HDR_CONTENT_TYPE := <<"multipart/byteranges; boundary=", Boundary/binary>>} = ?assertMatch(
                    #{?HDR_CONTENT_TYPE := <<"multipart/byteranges", _/binary>>}, RespHeaders
                ),

                Parts = lists:map(fun({RangeStart, RangeLen}) ->
                    PartContentRange = str_utils:format_bin("bytes ~B-~B/~B", [
                        RangeStart, RangeStart + RangeLen - 1, FileSize
                    ]),
                    PartContent = binary:part(ExpContent, RangeStart, RangeLen),

                    <<
                        "--", Boundary/binary,
                        "\r\ncontent-type: application/octet-stream"
                        "\r\ncontent-range: ", PartContentRange/binary,
                        "\r\n\r\n", PartContent/binary
                    >>
                end, Ranges),

                ExpPartialContent = str_utils:join_binary(lists:flatten([
                    Parts, <<"\r\n--", Boundary/binary, "--">>
                ])),
                ?assertEqual(ExpPartialContent, RespBody);

            {_, ?HTTP_416_RANGE_NOT_SATISFIABLE} ->
                ?assertEqual(?HTTP_416_RANGE_NOT_SATISFIABLE, RespCode),
                ?assertMatch(#{?HDR_CONTENT_RANGE := <<"bytes */", FileSizeBin/binary>>}, RespHeaders),
                ?assertEqual(<<>>, RespBody)
        end
    end.


%% @private
-spec build_rest_download_file_verify_fun(
    api_test_memory:mem_ref(),
    file_meta:size()
) ->
    onenv_api_test_runner:verify_fun().
build_rest_download_file_verify_fun(MemRef, FileSize) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    Providers = [P1Node, P2Node],

    fun
        (expected_failure, _) ->
            [#object{guid = FileGuid}] = api_test_memory:get(MemRef, file_tree_object),
            file_test_utils:await_distribution(Providers, FileGuid, [{P1Node, FileSize}]);
        (expected_success, #api_test_ctx{node = DownloadNode, data = Data}) ->
            [#object{guid = FileGuid}] = api_test_memory:get(MemRef, file_tree_object),

            case DownloadNode of
                ?ONEZONE_TARGET_NODE ->
                    % The request was made via Onezone's shared data redirector,
                    % we do not have information where it was redirected here.
                    % Still, the target node is randomized, and other cases
                    % cover all possible resulting distributions.
                    ok;
                P1Node ->
                    file_test_utils:await_distribution(Providers, FileGuid, [{P1Node, FileSize}]);
                _Other ->
                    case maps:get(<<"range">>, utils:ensure_defined(Data, #{}), undefined) of
                        undefined ->
                            file_test_utils:await_distribution(Providers, FileGuid, [
                                {P1Node, FileSize}, {DownloadNode, FileSize}
                            ]);

                        {_, ?HTTP_206_PARTIAL_CONTENT, [{RangeStart, _RangeLen} = Range]} ->
                            file_test_utils:await_distribution(Providers, FileGuid, [
                                {P1Node, FileSize},
                                {DownloadNode, [#file_block{
                                    offset = RangeStart,
                                    size = get_fetched_block_size(Range, FileSize)
                                }]}
                            ]);

                        {_, ?HTTP_206_PARTIAL_CONTENT, Ranges} ->
                            Blocks = fslogic_blocks:consolidate(lists:sort(lists:map(
                                fun({RangeStart, _RangeLen} = Range) ->
                                    #file_block{
                                        offset = RangeStart,
                                        size = get_fetched_block_size(Range, FileSize)
                                    }
                                end, Ranges
                            ))),
                            file_test_utils:await_distribution(Providers, FileGuid, [
                                {P1Node, FileSize},
                                {DownloadNode, Blocks}
                            ]);

                        {_, ?HTTP_416_RANGE_NOT_SATISFIABLE} ->
                            file_test_utils:await_distribution(Providers, FileGuid, [{P1Node, FileSize}])
                    end
            end
    end.


%% @private
-spec get_fetched_block_size({offset(), size()}, file_meta:size()) -> size().
get_fetched_block_size({RangeStart, RangeLen}, FileSize) ->
    % posix storage has no block size so fetched blocks will not be tailored to
    % block boundaries but they will not be smaller than 'minimal_sync_request'
    % (only exception to this is fetching last bytes of file).
    PreferableFetchBlockSize = max(RangeLen, ?DEFAULT_READ_BLOCK_SIZE),

    case (RangeStart + PreferableFetchBlockSize) > FileSize of
        true -> FileSize - (RangeStart rem FileSize);
        false -> PreferableFetchBlockSize
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec build_download_file_setup_fun(
    api_test_memory:mem_ref(),
    onenv_file_test_utils:object_spec() | [onenv_file_test_utils:file_spec()]
) ->
    onenv_api_test_runner:setup_fun().
build_download_file_setup_fun(MemRef, Spec) ->
    SpaceId = oct_background:get_space_id(space_krk_par),
    
    fun() ->
        Object = lists:map(fun(SingleFileSpec) ->
            onenv_file_test_utils:create_and_sync_file_tree(user3, SpaceId, SingleFileSpec, krakow)
        end, utils:ensure_list(Spec)),
        api_test_memory:set(MemRef, file_tree_object, Object)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Compares provided tarball (as gzip compressed Bytes) against expected 
%% structure provided as FileTreeObject.
%% Files strategy describes how files check should be performed:
%%      files_exist -> checks that appropriate files are in tarball and have proper content;
%%      no_files -> checks that there are no files.
%% @end
%%--------------------------------------------------------------------
-spec check_tarball(binary(), binary()) -> ok.
check_tarball(Bytes, FileTreeObject) ->
    check_tarball(Bytes, FileTreeObject, check_files_content).


%% @private
-spec check_tarball(binary(), binary(), files_strategy()) -> ok.
check_tarball(Bytes, FileTreeObject, FilesStrategy) ->
    TmpDir = mochitemp:mkdtemp(),
    ?assertEqual(ok, erl_tar:extract({binary, Bytes}, [compressed, {cwd, TmpDir}])),
    check_extracted_tarball_structure(FileTreeObject, FilesStrategy, TmpDir, root_dir).


%% @private
-spec check_extracted_tarball_structure(onenv_file_test_utils:object_spec(), files_strategy(), binary(), child | root_dir) -> 
    ok.
check_extracted_tarball_structure(#object{type = ?DIRECTORY_TYPE} = Object, FilesStrategy, CurrentPath, DirType) ->
    #object{name = Dirname, children = Children} = Object,
    {ok, TmpDirContentAfter} = file:list_dir(CurrentPath),
    ExpectDir = FilesStrategy == check_files_content orelse DirType == root_dir,
    ?assertEqual(ExpectDir, lists:member(binary_to_list(Dirname), TmpDirContentAfter)),
    lists:foreach(fun(Child) ->
        check_extracted_tarball_structure(Child, FilesStrategy, filename:join(CurrentPath, Dirname), child)
    end, Children);
check_extracted_tarball_structure(#object{type = ?REGULAR_FILE_TYPE} = Object, check_files_content, CurrentPath, _) ->
    #object{name = Filename, content = ExpContent} = Object,
    ?assertEqual({ok, ExpContent}, file:read_file(filename:join(CurrentPath, Filename)));
check_extracted_tarball_structure(#object{type = ?SYMLINK_TYPE} = Object, check_files_content, CurrentPath, _) ->
    #object{name = Filename, link_path = LinkPath} = Object,
    ?assertEqual({ok, binary_to_list(LinkPath)}, file:read_link(filename:join(CurrentPath, Filename)));
check_extracted_tarball_structure(#object{name = Filename}, no_files, CurrentPath, _ParentDirType) ->
    {ok, TmpDirContentAfter} = file:list_dir(CurrentPath),
    ?assertEqual(false, lists:member(binary_to_list(Filename), TmpDirContentAfter)).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    oct_background:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "api_tests",
        envs = [
            {op_worker, op_worker, [
                {fuse_session_grace_period_seconds, 24 * 60 * 60},
                {default_download_read_block_size, ?DEFAULT_READ_BLOCK_SIZE},

                % Ensure replica_synchronizer will not fetch more data than requested
                {minimal_sync_request, ?DEFAULT_READ_BLOCK_SIZE},
                {synchronizer_prefetch, false},

                {public_block_percent_threshold, 1},

                {download_code_expiration_interval_seconds, ?GUI_DOWNLOAD_CODE_EXPIRATION_SECONDS},
    
                {tarball_streaming_traverse_master_jobs_limit, 1},
                {tarball_streaming_traverse_slave_jobs_limit, 1}
            ]}
        ],
        posthook = fun(NewConfig) ->
            User3Id = oct_background:get_user_id(user3),
            lists:foreach(fun(SpaceId) ->
                ozw_test_rpc:space_set_user_privileges(SpaceId, User3Id, [
                    ?SPACE_MANAGE_SHARES | privileges:space_member()
                ])
            end, oct_background:get_provider_supported_spaces(krakow)),
            
            ProviderNodes = oct_background:get_all_providers_nodes(),
            lists:foreach(fun(OpNode) -> 
                test_node_starter:load_modules([OpNode], [?MODULE]),
                ok = test_utils:mock_new(OpNode, file_download_utils),
                ErrorFun = fun(SessionId, FileAttrs, Callback, Req) ->
                    ShouldBlock = lists:any(fun(#file_attr{guid = Guid}) ->
                        {Uuid, _, _} = file_id:unpack_share_guid(Guid),
                        node_cache:get({block_file, Uuid}, false)
                    end, utils:ensure_list(FileAttrs)),
                    case ShouldBlock of
                        true -> http_req:send_error(?ERROR_POSIX(?EAGAIN), Req);
                        false -> meck:passthrough([SessionId, FileAttrs, Callback, Req])
                    end
                end,
                ok = test_utils:mock_expect(OpNode, file_download_utils, download_single_file, ErrorFun),
                ok = test_utils:mock_expect(OpNode, file_download_utils, download_tarball, ErrorFun)
            end, ProviderNodes),
            NewConfig
        end
    }).


end_per_suite(_Config) ->
    Nodes = oct_background:get_all_providers_nodes(),
    test_utils:mock_unload(Nodes),
    oct_background:end_per_suite().


init_per_group(_Group, Config) ->
    lfm_proxy:init(Config, false).


end_per_group(_Group, Config) ->
    lfm_proxy:teardown(Config).


init_per_testcase(gui_download_file_test = Case, Config) ->
    Providers = ?config(op_worker_nodes, Config),
    % TODO VFS-6828 - call needed to preload file_middleware module and add 'download_url' atom
    % to known/existing atoms. Otherwise gs_ws_handler may fail to decode this request (gri)
    % if it is the first request made.
    rpc:multicall(Providers, file_middleware, operation_supported, [get, download_url, private]),
    init_per_testcase(?DEFAULT_CASE(Case), Config);
init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 20}),
    Config.

end_per_testcase(_Case, _Config) ->
    ok.


