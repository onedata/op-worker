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
-include_lib("ctool/include/graph_sync/gri.hrl").
-include_lib("ctool/include/http/codes.hrl").
-include_lib("ctool/include/http/headers.hrl").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    gui_download_file_test/1,
    gui_download_dir_test/1,
    gui_download_multi_file_test/1,
    rest_download_file_test/1
]).

all() -> [
    gui_download_file_test,
    gui_download_dir_test,
    gui_download_multi_file_test,
    rest_download_file_test
].


-define(DEFAULT_READ_BLOCK_SIZE, 1024).
-define(GUI_DOWNLOAD_CODE_EXPIRATION_SECONDS, 20).

-define(ATTEMPTS, 30).


-type test_mode() :: normal_mode | share_mode.

-type offset() :: non_neg_integer().
-type size() :: non_neg_integer().


%%%===================================================================
%%% File download test functions
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
    gui_download_test_base(Config, <<"file">>, ClientSpec).

gui_download_dir_test(Config) ->
    ClientSpec = #client_spec{
        correct = [
            user2,  % space owner - doesn't need any perms
            user3,  % files owner
            user4   % forbidden user in space is allowed to download an empty archive - files without access are ignored
        ],
        unauthorized = [nobody],
        forbidden_not_in_space = [user1]
    },
    gui_download_test_base(Config, <<"dir">>, ClientSpec).

gui_download_test_base(Config, FileType, ClientSpec) ->
    Providers = ?config(op_worker_nodes, Config),

    {_FileType, _DirPath, DirGuid, DirShareId} = api_test_utils:create_and_sync_shared_file_in_space_krk_par(
        <<"dir">>, 8#704
    ),
    FileSize = 4 * ?DEFAULT_READ_BLOCK_SIZE,
    Content = crypto:strong_rand_bytes(FileSize),

    MemRef = api_test_memory:init(),
    DownloadType = case FileType of
        <<"file">> -> single_file;
        <<"dir">> -> archive
    end,

    SetupFun = build_download_file_setup_fun(MemRef, Content, FileType),
    ValidateCallResultFun = build_get_download_url_validate_gs_call_fun(MemRef, Content, DownloadType),
    VerifyFun = build_download_file_with_gui_endpoint_verify_fun(MemRef, Content, DownloadType), 

    DataSpec = #data_spec{
        required = [<<"file_ids">>],
        correct_values = #{<<"file_ids">> => [[guid]]},
        bad_values = [
            {<<"file_ids">>, [<<"incorrect_guid">>], ?ERROR_BAD_VALUE_IDENTIFIER(<<"file_ids">>)},
            {<<"file_ids">>, <<"not_a_list">>, ?ERROR_BAD_VALUE_LIST_OF_BINARIES(<<"file_ids">>)}
        ]
    },
    ?assert(onenv_api_test_runner:run_tests([
        #scenario_spec{
            name = <<"Download file using gui endpoint and gs private api">>,
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
            name = <<"Download shared file using gui endpoint and gs public api">>,
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
            name = <<"Download shared file using gui endpoint and gs private api">>,
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


gui_download_multi_file_test(Config) ->
    Providers = ?config(op_worker_nodes, Config),
    
    FileSize = 4 * ?DEFAULT_READ_BLOCK_SIZE,
    Content = crypto:strong_rand_bytes(FileSize),
    
    MemRef = api_test_memory:init(),
    
    SetupFun = build_download_file_setup_fun(MemRef, Content, <<"dir">>), 
    ValidateCallResultFun = build_get_download_url_validate_gs_call_fun(MemRef, Content, archive),
    VerifyFun = build_download_file_with_gui_endpoint_verify_fun(MemRef, Content, archive),
    
    DataSpec = #data_spec{
        required = [<<"file_ids">>],
        correct_values = #{<<"file_ids">> => [[file_guid, file_guid2]]},
        bad_values = [
            {<<"file_ids">>, [file_guid, <<"incorrect_guid">>], ?ERROR_BAD_VALUE_IDENTIFIER(<<"file_ids">>)}
        ]
    },
    ?assert(onenv_api_test_runner:run_tests([
        #scenario_spec{
            name = <<"Download file using gui endpoint and gs private api">>,
            type = gs,
            target_nodes = Providers,
            client_spec = #client_spec{
                correct = [
                    user2,  % space owner - doesn't need any perms
                    user3,  % files owner
                    user4   % forbidden user in space is allowed to download an empty archive - files without access are ignored
                ],
                unauthorized = [nobody],
                forbidden_not_in_space = [user1]
            },
            setup_fun = SetupFun,
            prepare_args_fun = build_get_download_url_prepare_gs_args_fun(MemRef, normal_mode, private),
            validate_result_fun = ValidateCallResultFun,
            verify_fun = VerifyFun,
            data_spec = DataSpec
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
        List when is_list(List) -> 
            UpdatedData = Data#{<<"file_ids">> => lists:map(
                fun (Placeholder) when is_atom(Placeholder) ->
                        Guid = get_file_guid(MemRef, Placeholder, TestMode),
                        {FinalId, _} = api_test_utils:maybe_substitute_bad_id(Guid, Data),
                        FinalId;
                    (Other) -> Other 
                end, List)
            },
            maps:without([bad_id], UpdatedData);
        _ -> Data
    end;
maybe_inject_guids(_MemRef, Data, _TestMode) ->
    Data.


%% @private
-spec build_get_download_url_validate_gs_call_fun(
    api_test_memory:mem_ref(),
    FileContent :: binary(),
    single_file | archive
) ->
    onenv_api_test_runner:validate_call_result_fun().
build_get_download_url_validate_gs_call_fun(MemRef, ExpContent, DownloadType) ->
    [FileCreationNode] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),

    fun(#api_test_ctx{node = DownloadNode, data = Data, client = Client}, Result) ->
        FileGuid = api_test_memory:get(MemRef, guid),

        case DownloadType of
            single_file ->
                FileSize = size(ExpContent),
                FirstBlockFetchedSize = min(FileSize, ?DEFAULT_READ_BLOCK_SIZE),
                % Getting file download url should cause first file block to be synced on target provider.
                ExpDist = case FileCreationNode == DownloadNode of
                    true -> [{FileCreationNode, FileSize}];
                    false -> [{FileCreationNode, FileSize}, {DownloadNode, FirstBlockFetchedSize}]
                end,
                file_test_utils:await_distribution([FileCreationNode, P2Node], FileGuid, ExpDist);
            archive ->
                ok
        end,

        {ok, #{<<"fileUrl">> := FileDownloadUrl}} = ?assertMatch({ok, #{}}, Result),
        [_, DownloadCode] = binary:split(FileDownloadUrl, [<<"/download/">>]),

        case rand:uniform(2) of
            1 ->
                % File download code should be still usable after unsuccessful download
                block_file_streaming(DownloadNode, ?ERROR_POSIX(?EAGAIN), DownloadType),
                ?assertEqual(?ERROR_POSIX(?EAGAIN), download_file_with_gui_endpoint(DownloadNode, FileDownloadUrl)),
                unblock_file_streaming(DownloadNode),
                ?assertMatch({ok, _}, get_file_download_code_doc(DownloadNode, DownloadCode, memory)),

                % But first successful download should make it unusable
                case DownloadType of
                    single_file ->
                        ?assertEqual({ok, ExpContent}, download_file_with_gui_endpoint(DownloadNode, FileDownloadUrl));
                    archive ->
                        User4Id = oct_background:get_user_id(user4),
                        {ok, Bytes} = ?assertMatch({ok, _}, download_file_with_gui_endpoint(DownloadNode, FileDownloadUrl)),
                        % user4 does not have access to files, so downloaded archive contains only dir entry
                        case {Client, length(maps:get(<<"file_ids">>, Data)), api_test_memory:get(MemRef, scope)} of
                            {?USER(User4Id), 1, private} ->
                                check_archive(MemRef, Bytes, ExpContent, directory, no_files);
                            {_, 1, _} ->
                                check_archive(MemRef, Bytes, ExpContent, directory);
                            {?USER(User4Id), _, private} -> 
                                check_archive(MemRef, Bytes, ExpContent, multi_file, no_files);
                            {_, _, _} ->
                                check_archive(MemRef, Bytes, ExpContent, multi_file)
                        end
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
-spec block_file_streaming(node(), errors:error(), single_file | archive) -> ok.
block_file_streaming(OpNode, ErrorReturned, single_file) ->
    test_node_starter:load_modules([OpNode], [?MODULE]),
    ok = test_utils:mock_new(OpNode, http_download_utils),
    ok = test_utils:mock_expect(OpNode, http_download_utils, stream_file, fun(_, _, _, Req) ->
        http_req:send_error(ErrorReturned, Req)
    end);
block_file_streaming(OpNode, ErrorReturned, archive) ->
    test_node_starter:load_modules([OpNode], [?MODULE]),
    ok = test_utils:mock_new(OpNode, http_download_utils),
    ok = test_utils:mock_expect(OpNode, http_download_utils, stream_tarball, fun(_, _, _, Req) ->
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
-spec build_download_file_with_gui_endpoint_verify_fun(
    api_test_memory:mem_ref(),
    FileContent :: binary(),
    single_file | archive
) ->
    onenv_api_test_runner:verify_fun().
build_download_file_with_gui_endpoint_verify_fun(MemRef, Content, DownloadType) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    Providers = [P1Node, P2Node],

    FileSize = size(Content),
    FirstBlockFetchedSize = min(FileSize, ?DEFAULT_READ_BLOCK_SIZE),

    fun
        (expected_failure, _) ->
            FileGuid = api_test_memory:get(MemRef, file_guid),
            file_test_utils:await_distribution(Providers, FileGuid, [{P1Node, FileSize}]);
        (expected_success, #api_test_ctx{node = DownloadNode, client = Client}) ->
            FileGuid = api_test_memory:get(MemRef, file_guid),
            case P1Node == DownloadNode of
                true ->
                    file_test_utils:await_distribution(Providers, FileGuid, [{P1Node, FileSize}]);
                false ->
                    ExpDist = case {api_test_memory:get(MemRef, download_succeeded), DownloadType} of
                        {true, single_file} -> 
                            [{P1Node, FileSize}, {DownloadNode, FileSize}];
                        {true, archive} ->
                            User4Id = oct_background:get_user_id(user4),
                            case {Client, api_test_memory:get(MemRef, scope)} of
                                {?USER(User4Id), private} -> [{P1Node, FileSize}];
                                _ -> [{P1Node, FileSize}, {DownloadNode, FileSize}]
                            end;
                        {false, single_file} -> 
                            [{P1Node, FileSize}, {DownloadNode, FirstBlockFetchedSize}];
                        {false, archive} -> 
                            [{P1Node, FileSize}]
                    end,
                    file_test_utils:await_distribution(Providers, FileGuid, ExpDist)
            end
    end.


rest_download_file_test(Config) ->
    Providers = ?config(op_worker_nodes, Config),

    {_FileType, _DirPath, DirGuid, DirShareId} = api_test_utils:create_and_sync_shared_file_in_space_krk_par(
        <<"dir">>, 8#704
    ),
    {ok, DirObjectId} = file_id:guid_to_objectid(DirGuid),

    DirShareGuid = file_id:guid_to_share_guid(DirGuid, DirShareId),
    {ok, DirShareObjectId} = file_id:guid_to_objectid(DirShareGuid),

    FileSize = 4 * ?DEFAULT_READ_BLOCK_SIZE,
    Content = crypto:strong_rand_bytes(FileSize),

    MemRef = api_test_memory:init(),

    SetupFun = build_download_file_setup_fun(MemRef, Content, <<"file">>),
    ValidateCallResultFun = build_rest_download_file_validate_call_fun(MemRef, Content, Config),
    VerifyFun = build_download_file_verify_fun(MemRef, FileSize),

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
            prepare_args_fun = build_rest_download_file_prepare_args_fun(MemRef, normal_mode),
            validate_result_fun = ValidateCallResultFun,
            verify_fun = VerifyFun,

            data_spec = api_test_utils:add_file_id_errors_for_operations_available_in_share_mode(
                DirGuid, undefined, #data_spec{
                    optional = [<<"range">>],
                    correct_values = #{<<"range">> => RangesToTestPart1},
                    bad_values = [{bad_id, DirObjectId, {rest, ?ERROR_POSIX(?EISDIR)}}]
                }
            )
        },
        #scenario_spec{
            name = <<"Download shared file using rest endpoint">>,
            type = {rest_with_shared_guid, file_id:guid_to_space_id(DirGuid)},
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SHARES,

            setup_fun = SetupFun,
            prepare_args_fun = build_rest_download_file_prepare_args_fun(MemRef, share_mode),
            validate_result_fun = ValidateCallResultFun,
            verify_fun = VerifyFun,

            data_spec = api_test_utils:add_file_id_errors_for_operations_available_in_share_mode(
                DirGuid, DirShareId, #data_spec{
                    optional = [<<"range">>],
                    correct_values = #{<<"range">> => RangesToTestPart2},
                    bad_values = [{bad_id, DirShareObjectId, {rest, ?ERROR_POSIX(?EISDIR)}}]
                }
            )
        }
    ])).


%% @private
-spec build_rest_download_file_prepare_args_fun(api_test_memory:mem_ref(), test_mode()) ->
    onenv_api_test_runner:prepare_args_fun().
build_rest_download_file_prepare_args_fun(MemRef, TestMode) ->
    fun(#api_test_ctx{data = Data0}) ->
        FileGuid = get_file_guid(MemRef, TestMode),
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
    FileContent :: binary(),
    onenv_api_test_runner:ct_config()
) ->
    onenv_api_test_runner:validate_call_result_fun().
build_rest_download_file_validate_call_fun(_MemRef, ExpContent, _Config) ->
    FileSize = size(ExpContent),
    FileSizeBin = integer_to_binary(FileSize),

    fun(#api_test_ctx{data = Data}, {ok, RespCode, RespHeaders, RespBody}) ->
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
-spec build_download_file_verify_fun(
    api_test_memory:mem_ref(),
    file_meta:size()
) ->
    onenv_api_test_runner:verify_fun().
build_download_file_verify_fun(MemRef, FileSize) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    Providers = [P1Node, P2Node],

    fun
        (expected_failure, _) ->
            FileGuid = api_test_memory:get(MemRef, guid),
            file_test_utils:await_distribution(Providers, FileGuid, [{P1Node, FileSize}]);
        (expected_success, #api_test_ctx{node = DownloadNode, data = Data}) ->
            FileGuid = api_test_memory:get(MemRef, guid),

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
    FileContent :: binary(),
    api_test_utils:file_type()
) ->
    onenv_api_test_runner:setup_fun().
build_download_file_setup_fun(MemRef, Content, FileType) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    Providers = [P1Node, P2Node],

    UserSessIdP1 = oct_background:get_user_session_id(user3, krakow),
    SpaceOwnerSessIdP1 = oct_background:get_user_session_id(user2, krakow),

    FileSize = size(Content),

    fun() ->
        FilePath = filename:join(["/", ?SPACE_KRK_PAR, ?RANDOM_FILE_NAME()]),
        {ok, Guid} = api_test_utils:create_file(FileType, P1Node, UserSessIdP1, FilePath, 8#705),
        {ok, ShareId} = lfm_proxy:create_share(P1Node, SpaceOwnerSessIdP1, {guid, Guid}, <<"share">>),
        [FileGuid, FileGuid2] = FileGuids = case FileType of
            <<"file">> ->
                FilePath1 = filename:join(["/", ?SPACE_KRK_PAR, ?RANDOM_FILE_NAME()]),
                {ok, G} = api_test_utils:create_file(<<"file">>, P1Node, UserSessIdP1, FilePath1, 8#604),
                [Guid, G];
            <<"dir">> ->
                {ok, NestedGuid} = lfm_proxy:create(P1Node, UserSessIdP1, Guid, ?RANDOM_FILE_NAME(), 8#604),
                {ok, NestedGuid2} = lfm_proxy:create(P1Node, UserSessIdP1, Guid, ?RANDOM_FILE_NAME(), 8#604),
                [NestedGuid, NestedGuid2]
        end,
    
        lists:foreach(fun(G) ->
            api_test_utils:write_file(P1Node, UserSessIdP1, G, 0, Content)
        end, FileGuids),
    
        lists:foreach(fun(G) ->
            ?assertMatch(
                {ok, #file_attr{size = FileSize}},
                file_test_utils:get_attrs(P2Node, G),
                ?ATTEMPTS
            )
        end, FileGuids),
        ?assertMatch(
            {ok, #file_attr{shares = [ShareId]}},
            file_test_utils:get_attrs(P2Node, Guid),
            ?ATTEMPTS
        ),
        file_test_utils:await_distribution(Providers, FileGuid, [{P1Node, FileSize}]),

        api_test_memory:set(MemRef, guid, Guid),
        api_test_memory:set(MemRef, file_guid, FileGuid),
        api_test_memory:set(MemRef, file_guid2, FileGuid2),
        api_test_memory:set(MemRef, share_id, ShareId)
    end.


%% @private
-spec get_file_guid(api_test_memory:mem_ref(), test_mode()) -> file_id:file_guid().
get_file_guid(MemRef, TestMode) ->
    get_file_guid(MemRef, guid, TestMode).


%% @private
-spec get_file_guid(api_test_memory:mem_ref(), atom(), test_mode()) -> file_id:file_guid().
get_file_guid(MemRef, Placeholder, TestMode) ->
    BareGuid = api_test_memory:get(MemRef, Placeholder),
    case TestMode of
        normal_mode ->
            BareGuid;
        share_mode ->
            ShareId = api_test_memory:get(MemRef, share_id),
            file_id:guid_to_share_guid(BareGuid, ShareId)
    end.


%% @private
-spec check_archive(api_test_memory:mem_ref(), binary(), binary(), directory | multi_file) -> ok.
check_archive(MemRef, Bytes, ExpContent, Type) ->
    check_archive(MemRef, Bytes, ExpContent, Type, files_exist).

%% @private
-spec check_archive(api_test_memory:mem_ref(), binary(), binary(), directory | multi_file, ignore_file | check_files) -> ok.
check_archive(MemRef, Bytes, ExpContent, directory, FilesStrategy) ->
    ?assertEqual(ok, erl_tar:extract({binary, Bytes}, [compressed, {cwd, "/tmp/"}])),
    {ok, TmpDirContentAfter} = file:list_dir("/tmp"),
    DirGuid = api_test_memory:get(MemRef, guid),
    [Node] = oct_background:get_provider_nodes(krakow),
    {ok, #file_attr{name = Dirname}} = lfm_proxy:stat(Node, ?ROOT_SESS_ID, {guid, DirGuid}),
    ?assertEqual(true, lists:member(binary_to_list(Dirname), TmpDirContentAfter)),
    check_archive_files_content(MemRef, ExpContent, Dirname, FilesStrategy);
check_archive(MemRef, Bytes, ExpContent, multi_file, FilesStrategy) ->
    ?assertEqual(ok, erl_tar:extract({binary, Bytes}, [compressed, {cwd, "/tmp/"}])),
    check_archive_files_content(MemRef, ExpContent, <<>>, FilesStrategy).


%% @private
-spec check_archive_files_content(api_test_memory:mem_ref(), binary(), binary(), no_files | files_exist) -> ok.
check_archive_files_content(MemRef, ExpContent, ParentName, FilesStrategy) ->
    {ok, DirContentAfter} = file:list_dir(filename:join("/tmp", ParentName)),
    [Node] = oct_background:get_provider_nodes(krakow),
    lists:foreach(fun(Placeholder) ->
        Guid = api_test_memory:get(MemRef, Placeholder),
        {ok, #file_attr{name = Filename}} = lfm_proxy:stat(Node, ?ROOT_SESS_ID, {guid, Guid}),
        case FilesStrategy of
            no_files -> ?assertEqual(false, lists:member(binary_to_list(Filename), DirContentAfter));
            files_exist -> ?assertEqual({ok, ExpContent}, file:read_file(filename:join(["/tmp", ParentName, Filename])))
        end
    end, [file_guid, file_guid2]).

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

                {download_code_expiration_interval_seconds, ?GUI_DOWNLOAD_CODE_EXPIRATION_SECONDS}
            ]}
        ]
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


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
    lists:foreach(fun unblock_file_streaming/1, ?config(op_worker_nodes, Config)),
    end_per_testcase(default, Config);
end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).


