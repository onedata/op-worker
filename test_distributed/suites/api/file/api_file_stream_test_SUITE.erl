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
-include("modules/fslogic/file_details.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("onenv_test_utils.hrl").
-include("proto/oneclient/common_messages.hrl").
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
    gui_download_incorrect_uuid_test/1,
    gui_download_tarball_with_hardlinks_test/1,
    gui_download_tarball_with_symlink_loop_test/1,
    rest_download_file_test/1,
    rest_download_file_at_path_test/1,
    rest_download_dir_test/1
]).

groups() -> [
    {all_tests, [parallel], [
%%        gui_download_file_test,
%%        gui_download_dir_test,
%%        gui_download_multiple_files_test,
%%        gui_download_different_filetypes_test,
%%        gui_large_dir_download_test,
%%        gui_download_files_between_spaces_test,
%%        gui_download_incorrect_uuid_test,
%%        gui_download_tarball_with_hardlinks_test,
%%        gui_download_tarball_with_symlink_loop_test,
%%        rest_download_file_test
        rest_download_file_at_path_test
%%        rest_download_dir_test
    ]}
].

all() -> [
    {group, all_tests}
].

-define(DEFAULT_READ_BLOCK_SIZE, 1024).
-define(GUI_DOWNLOAD_CODE_EXPIRATION_SECONDS, 20).

-define(ATTEMPTS, 30).

-define(FILESIZE, 4 * ?DEFAULT_READ_BLOCK_SIZE).
-define(RAND_CONTENT(), ?RAND_CONTENT(rand:uniform(10) * ?FILESIZE)).
-define(RAND_CONTENT(FileSize), crypto:strong_rand_bytes(FileSize)).

-define(TARBALL_DOWNLOAD_CLIENT_SPEC,
    #client_spec{
        correct = [
            user2,  % space owner - doesn't need any perms
            user3,  % files owner
            user4   % forbidden user in space is allowed to download an empty tarball - files without access are ignored
        ],
        unauthorized = [nobody],
        forbidden_not_in_space = [user1]
    }
).

-define(ALL_RANGES_TO_TEST, [
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
]).

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
    FileSpec = #file_spec{mode = 8#604, content = ?RAND_CONTENT(), shares = [#share_spec{}]},
    gui_download_test_base(Config, FileSpec, ClientSpec, <<"File">>, uninterrupted_download).

gui_download_dir_test(Config) ->
    ClientSpec = ?TARBALL_DOWNLOAD_CLIENT_SPEC,
    DirSpec = #dir_spec{mode = 8#705, shares = [#share_spec{}], children = [#dir_spec{}, #file_spec{content = ?RAND_CONTENT()}]},
    gui_download_test_base(Config, DirSpec, ClientSpec, <<"Dir">>).

gui_download_multiple_files_test(Config) ->
    ClientSpec = ?TARBALL_DOWNLOAD_CLIENT_SPEC,
    DirSpec = [
        #file_spec{mode = 8#604, shares = [#share_spec{}], content = ?RAND_CONTENT()},
        #file_spec{mode = 8#604, shares = [#share_spec{}], content = ?RAND_CONTENT()},
        #file_spec{mode = 8#604, shares = [#share_spec{}], content = ?RAND_CONTENT()},
        #file_spec{mode = 8#604, shares = [#share_spec{}], content = ?RAND_CONTENT()}
    ],
    gui_download_test_base(Config, DirSpec, ClientSpec, <<"Multiple files">>).

gui_download_different_filetypes_test(Config) ->
    ClientSpec = ?TARBALL_DOWNLOAD_CLIENT_SPEC,
    DirSpec = [
        #symlink_spec{shares = [#share_spec{}], symlink_value = make_symlink_target()},
        #dir_spec{mode = 8#705, shares = [#share_spec{}], children = [#dir_spec{}, #file_spec{content = ?RAND_CONTENT()}]},
        #file_spec{mode = 8#604, shares = [#share_spec{}], content = ?RAND_CONTENT()}
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
            [#dir_spec{children = Children}, #dir_spec{children = Children}, #file_spec{content = ?RAND_CONTENT(100)}]
    end,
    DirSpec = #dir_spec{mode = 8#705, shares = [#share_spec{}], children = ChildrenSpecGen(7)},
    gui_download_test_base(Config, DirSpec, ClientSpec, <<"Large dir">>).


gui_download_files_between_spaces_test(_Config) ->
    ClientSpec = #client_spec{
        correct = [
            user3,  % files owner
            user4   % forbidden user in space is allowed to download an empty tarball - files without access are ignored
        ],
        % user1 and user2 are only in one of specified spaces, therefore download should fail
        forbidden_not_in_space = [user1, user2],
        unauthorized = [nobody]
    },
    MemRef = api_test_memory:init(),

    SpaceId1 = oct_background:get_space_id(space_krk_par),
    SpaceId2 = oct_background:get_space_id(space_krk),
    Spec = #file_spec{mode = 8#604, shares = [#share_spec{}], content = ?RAND_CONTENT()},

    SetupFun = fun() ->
        Object = lists:map(fun(SpaceId) ->
            onenv_file_test_utils:create_and_sync_file_tree(user3, SpaceId, Spec, krakow)
        end, [SpaceId1, SpaceId2]),
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
            name = <<"Download files between spaces using gui endpoint and gs private api">>,
            type = gs,
            target_nodes = [krakow],
            client_spec = ClientSpec,
            setup_fun = SetupFun,
            prepare_args_fun = build_get_download_url_prepare_gs_args_fun(MemRef, normal_mode, private),
            validate_result_fun = ValidateCallResultFun,
            data_spec = DataSpec
        }
    ])).


gui_download_incorrect_uuid_test(Config) ->
    MemRef = api_test_memory:init(),
    SpaceId = oct_background:get_space_id(space_krk_par),
    ValidateCallResultFun = fun(#api_test_ctx{node = DownloadNode}, Result) ->
        {ok, #{<<"fileUrl">> := FileDownloadUrl}} = ?assertMatch({ok, #{}}, Result),
        ?assertMatch(?ERROR_POSIX(?ENOENT), download_file_using_download_code_with_resumes(DownloadNode, FileDownloadUrl))
    end,

    DataSpec = #data_spec{
        required = [<<"file_ids">>],
        correct_values = #{<<"file_ids">> => [
            [file_id:pack_guid(<<"incorrent_uuid0">>, SpaceId), file_id:pack_guid(<<"incorrent_uuid1">>, SpaceId)]
        ]}
    },
    ?assert(onenv_api_test_runner:run_tests([
        #scenario_spec{
            name = <<"Download tarball with incorrent uuid using gui endpoint and gs private api">>,
            type = gs,
            target_nodes = ?config(op_worker_nodes, Config),
            client_spec = ?TARBALL_DOWNLOAD_CLIENT_SPEC,
            prepare_args_fun = build_get_download_url_prepare_gs_args_fun(MemRef, normal_mode, private),
            validate_result_fun = ValidateCallResultFun,
            data_spec = DataSpec
        }
    ])).


gui_download_tarball_with_symlink_loop_test(Config) ->
    MemRef = api_test_memory:init(),
    SpaceId = oct_background:get_space_id(space_krk_par),
    #object{guid = DirGuid} = DirObject = onenv_file_test_utils:create_and_sync_file_tree(user3, SpaceId, #dir_spec{}, krakow),
    Spec = [
        #file_spec{content = ?RAND_CONTENT(), mode = 8#604},
        #dir_spec{mode = 8#705, children = [#symlink_spec{symlink_value = make_symlink_target(SpaceId, DirObject)}]},
        #symlink_spec{symlink_value = make_symlink_target(SpaceId, DirObject)}
    ],
    [FileObject, #object{guid = ChildDirGuid, children = [#object{name = SymlinkName}]} = ChildDirObject, _SymlinkObject] =
        onenv_file_test_utils:create_and_sync_file_tree(user3, DirGuid, Spec, krakow),

    ExpectedObject = ChildDirObject#object{
        children = [
            DirObject#object{
                name = SymlinkName,
                children = [
                    ChildDirObject#object{children = []},
                    FileObject
                ]
            }
        ]
    },
    api_test_memory:set(MemRef, file_tree_object, [ExpectedObject]),

    ValidateCallResultFun = build_get_download_url_validate_gs_call_fun(MemRef),

    DataSpec = #data_spec{
        required = [<<"file_ids">>],
        correct_values = #{<<"file_ids">> => [
            [ChildDirGuid]
        ]}
    },
    ?assert(onenv_api_test_runner:run_tests([
        #scenario_spec{
            name = <<"Download tarball with symlink loop using gui endpoint and gs private api">>,
            type = gs,
            target_nodes = ?config(op_worker_nodes, Config),
            client_spec = #client_spec{
                correct = [
                    user2,  % space owner - doesn't need any perms
                    user3  % files owner
                ],
                unauthorized = [nobody],
                forbidden_not_in_space = [user1]
            },
            prepare_args_fun = build_get_download_url_prepare_gs_args_fun(MemRef, normal_mode, private),
            validate_result_fun = ValidateCallResultFun,
            data_spec = DataSpec
        }
    ])).


gui_download_tarball_with_hardlinks_test(Config) ->
    Providers = ?config(op_worker_nodes, Config),
    SpaceId = oct_background:get_space_id(space_krk_par),

    MemRef = api_test_memory:init(),
    Spec = #dir_spec{mode = 8#705, children = [#file_spec{content = ?RAND_CONTENT(), mode = 8#604}]},

    SetupFun = fun() ->
        #object{guid = DirGuid, children = [#object{guid = FileGuid, content = Content}] = Children} = DirObject =
            onenv_file_test_utils:create_and_sync_file_tree(user3, SpaceId, Spec, krakow),
        SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
        {ok, LinkObject1} = make_hardlink(Config, FileGuid, SpaceGuid),
        {ok, LinkObject2} = make_hardlink(Config, FileGuid, DirGuid),
        api_test_memory:set(MemRef, file_tree_object, [
            LinkObject1#object{content = Content},
            DirObject#object{children = [LinkObject2#object{content = Content} | Children]}
        ])
    end,

    ValidateCallResultFun = build_get_download_url_validate_gs_call_fun(MemRef),
    VerifyFun = build_download_file_verify_fun(MemRef),

    DataSpec = #data_spec{
        required = [<<"file_ids">>],
        % correct values are injected in function maybe_inject_guids/3 based on provided FileTreeSpec
        correct_values = #{<<"file_ids">> => [injected_guids]}
    },
    ?assert(onenv_api_test_runner:run_tests([
        #scenario_spec{
            name = <<"Download tarball containing hardlinks using gui endpoint and gs private api">>,
            type = gs,
            target_nodes = Providers,
            client_spec = ?TARBALL_DOWNLOAD_CLIENT_SPEC,
            setup_fun = SetupFun,
            prepare_args_fun = build_get_download_url_prepare_gs_args_fun(MemRef, normal_mode, private),
            validate_result_fun = ValidateCallResultFun,
            verify_fun = VerifyFun,
            data_spec = DataSpec
        }
    ])).

%%%===================================================================
%%% GUI download test base and spec functions
%%%===================================================================

%% @private
-spec gui_download_test_base(
    test_config:config(),
    onenv_file_test_utils:object_spec() | [onenv_file_test_utils:file_spec()],
    onenv_api_test_runner:client_spec(),
    binary()
) ->
    ok.
gui_download_test_base(Config, FileTreeSpec, ClientSpec, ScenarioPrefix) ->
    gui_download_test_base(Config, FileTreeSpec, ClientSpec, ScenarioPrefix, simulate_failures).

%% @private
-spec gui_download_test_base(
    test_config:config(),
    onenv_file_test_utils:object_spec() | [onenv_file_test_utils:file_spec()],
    onenv_api_test_runner:client_spec(),
    binary(),
    simulate_failures | uninterrupted_download
) ->
    ok.
gui_download_test_base(Config, FileTreeSpec, ClientSpec, ScenarioPrefix, DownloadType) ->
    Providers = ?config(op_worker_nodes, Config),

    SpaceId = oct_background:get_space_id(space_krk_par),
    #object{guid = DirGuid, shares = [DirShareId]} =
        onenv_file_test_utils:create_and_sync_file_tree(
            user3, SpaceId, #dir_spec{shares = [#share_spec{}]}, krakow),

    MemRef = api_test_memory:init(),
    api_test_memory:set(MemRef, download_type, DownloadType),

    SetupFun = build_download_file_setup_fun(MemRef, FileTreeSpec),
    ValidateCallResultFun = build_get_download_url_validate_gs_call_fun(MemRef),
    VerifyFun = build_download_file_verify_fun(MemRef),

    DataSpec = #data_spec{
        required = [<<"file_ids">>],
        optional = [<<"follow_symlinks">>],
        correct_values = #{
            % correct values are injected in function maybe_inject_guids/3 based on provided FileTreeSpec
            <<"file_ids">> => [injected_guids],
            <<"follow_symlinks">> => [true, false]
        },
        bad_values = [
            {<<"file_ids">>, [<<"incorrect_guid">>], ?ERROR_BAD_VALUE_IDENTIFIER(<<"file_ids">>)},
            {<<"file_ids">>, [file_id:pack_guid(<<"uuid">>, <<"incorrent_space_id">>)],
                {error_fun,
                    fun(#api_test_ctx{node = Node}) ->
                        ?ERROR_SPACE_NOT_SUPPORTED_BY(?GET_DOMAIN_BIN(Node))
                    end}
            },
            {<<"file_ids">>, <<"not_a_list">>, ?ERROR_BAD_VALUE_LIST_OF_BINARIES(<<"file_ids">>)},
            {<<"follow_symlinks">>, <<"not_a_boolean">>, ?ERROR_BAD_VALUE_BOOLEAN(<<"follow_symlinks">>)}
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
        api_test_memory:set(MemRef, follow_symlinks, maps:get(<<"follow_symlinks">>, Data0, true)),
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
        _ ->
            Data
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

        DownloadFunction = case api_test_memory:get(MemRef, download_type, simulate_failures) of
            simulate_failures -> fun download_file_using_download_code_with_resumes/2;
            uninterrupted_download -> fun download_file_using_download_code/2
        end,
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
                        ?assertEqual(?ERROR_POSIX(?EAGAIN), DownloadFunction(DownloadNode, FileDownloadUrl)),
                        unblock_file_streaming(DownloadNode, Guid),
                        ?assertMatch({ok, _}, get_file_download_code_doc(DownloadNode, DownloadCode, memory))
                end,

                % But first successful download should make it unusable
                case FileTreeObject of
                    [#object{type = ?REGULAR_FILE_TYPE, content = ExpContent1}] ->
                        ?assertEqual({ok, ExpContent1}, DownloadFunction(DownloadNode, FileDownloadUrl));
                    _ ->
                        {ok, Bytes} = ?assertMatch({ok, _}, DownloadFunction(DownloadNode, FileDownloadUrl)),
                        % user4 does not have access to files, so downloaded tarball contains only dir entry
                        lists:foreach(fun(Object) ->
                            case {Client, api_test_memory:get(MemRef, scope)} of
                                {?USER(User4Id), private} ->
                                    check_tarball(MemRef, Bytes, Object, no_files);
                                _ ->
                                    check_tarball(MemRef, Bytes, Object)
                            end
                        end, utils:ensure_list(FileTreeObject))
                end,
                % file download code is still usable for some time to allow for resuming after download of last chunk failed
                timer:sleep(timer:seconds(?GUI_DOWNLOAD_CODE_EXPIRATION_SECONDS)),
                ?assertMatch(?ERROR_NOT_FOUND, get_file_download_code_doc(DownloadNode, DownloadCode, memory), ?ATTEMPTS),
                ?assertEqual(?ERROR_BAD_VALUE_ID_NOT_FOUND(<<"code">>), DownloadFunction(DownloadNode, FileDownloadUrl)),

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
                ?assertEqual(?ERROR_BAD_VALUE_ID_NOT_FOUND(<<"code">>), DownloadFunction(DownloadNode, FileDownloadUrl)),
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
-spec build_download_file_verify_fun(api_test_memory:mem_ref()) ->
    onenv_api_test_runner:verify_fun().
build_download_file_verify_fun(MemRef) ->
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
                    {expected_failure, _} -> expected_failure;
                    {_, false} -> expected_failure;
                    {_, _} -> expected_success
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
    ExpDist = case api_test_memory:get(MemRef, download_succeeded, true) of
        true -> [{P1Node, FileSize}, {DownloadNode, FileSize}];
        false -> [{P1Node, FileSize}, {DownloadNode, FirstBlockFetchedSize}]
    end,
    file_test_utils:await_distribution(Providers, FileGuid, ExpDist).


%% @private
-spec check_tarball_download_distribution(
    api_test_memory:mem_ref(), expected_success | expected_failure, onenv_api_test_runner:api_test_ctx(),
    onenv_file_test_utils:object_spec(), [oneprovider:id()], node()
) -> ok.
check_tarball_download_distribution(_, _, #api_test_ctx{node = ?ONEZONE_TARGET_NODE}, _, _, _) ->
    % The request was made via Onezone's shared data redirector,
    % we do not have information where it was redirected here.
    % Still, the target node is randomized, and other cases
    % cover all possible resulting distributions.
    ok;
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

    SpaceId = oct_background:get_space_id(space_krk_par),
    #object{guid = DirGuid, shares = [DirShareId]} =
        onenv_file_test_utils:create_and_sync_file_tree(
            user3, SpaceId, #dir_spec{shares = [#share_spec{}]}, krakow),

    MemRef = api_test_memory:init(),

    FileSize = 4 * ?DEFAULT_READ_BLOCK_SIZE,
    SetupFun = build_download_file_setup_fun(MemRef, #file_spec{mode = 8#604, content = ?RAND_CONTENT(FileSize), shares = [#share_spec{}]}),
    ValidateCallResultFun = build_rest_download_file_validate_call_fun(MemRef, Config),
    VerifyFun = build_rest_download_file_verify_fun(MemRef, FileSize),

    AllRangesToTestNum = length(?ALL_RANGES_TO_TEST),

    % Randomly split range to test so to shorten test execution time by not
    % repeating every combination several times
    RangesToTestPart1 = lists_utils:random_sublist(
        ?ALL_RANGES_TO_TEST, AllRangesToTestNum div 2, AllRangesToTestNum div 2
    ),
    RangesToTestPart2 = ?ALL_RANGES_TO_TEST -- RangesToTestPart1,

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


rest_download_file_at_path_test(Config) ->
    Providers = ?config(op_worker_nodes, Config),
    FileSize = 4 * ?DEFAULT_READ_BLOCK_SIZE,

    SpaceId = oct_background:get_space_id(space_krk_par),

    #object{guid = BaseDirGuid} = BaseDirObject = onenv_file_test_utils:create_and_sync_file_tree(
        user3, SpaceId, #dir_spec{
            shares = [#share_spec{}]
        }, krakow),

    MemRef = api_test_memory:init(),

    api_test_memory:set(MemRef, base_dir_object, BaseDirObject),

    SetupFun = build_download_file_setup_fun(MemRef, #dir_spec{
        shares = [#share_spec{}],
        children = [#file_spec{mode = 8#604, content = ?RAND_CONTENT(FileSize), shares = [#share_spec{}]}]
    }),
    ValidateCallResultFun = build_rest_download_file_validate_call_fun(MemRef, Config),
    VerifyFun = build_rest_download_file_at_path_verify_fun(MemRef, FileSize),

    AllRangesToTestNum = length(?ALL_RANGES_TO_TEST),

    % Randomly split range to test so to shorten test execution time by not
    % repeating every combination several times
    RangesToTestPart1 = lists_utils:random_sublist(
        ?ALL_RANGES_TO_TEST, AllRangesToTestNum div 2, AllRangesToTestNum div 2
    ),

    ?assert(onenv_api_test_runner:run_tests([
        #scenario_spec{
            name = <<"Download file at path using rest endpoint">>,
            type = rest,
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SPACE_KRK_PAR,

            setup_fun = SetupFun,
            prepare_args_fun = build_rest_download_file_at_path_prepare_args_fun(MemRef, normal_mode),
            validate_result_fun = ValidateCallResultFun,
            verify_fun = VerifyFun,

            data_spec = api_test_utils:add_file_id_errors_for_operations_available_in_share_mode(
                BaseDirGuid, undefined, #data_spec{
                    optional = [<<"range">>, <<"path">>],
                    correct_values = #{
                        <<"range">> => RangesToTestPart1,
                        <<"path">> => [
                            only_filename_path_placeholder,
                            directory_and_filename_path_placeholder,
                            space_id_with_directory_and_filename_path_placeholder
                        ]
                    }
                }
            )
        }
    ])).


%% @private
-spec build_rest_download_file_at_path_prepare_args_fun(api_test_memory:mem_ref(), test_mode()) ->
    onenv_api_test_runner:prepare_args_fun().
build_rest_download_file_at_path_prepare_args_fun(MemRef, TestMode) ->
    fun(#api_test_ctx{data = Data0}) ->
        SpaceId = oct_background:get_space_id(space_krk_par),

        [#object{guid = DirGuid, name = DirName, children = [
            #object{guid = FileGuid, name = FileName} = FileObject
        ]}] = api_test_memory:get(MemRef, file_tree_object),

        api_test_memory:set(MemRef, test_file, FileObject),

        {ParentGuidOrSpaceId, Path} = case maps:get(<<"path">>, Data0, undefined) of
            only_filename_path_placeholder ->
                {DirGuid, FileName};
            directory_and_filename_path_placeholder ->
                SpaceRootDirGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
                {SpaceRootDirGuid, filepath_utils:join([DirName, FileName])};
            space_id_with_directory_and_filename_path_placeholder ->
                {space_id, filepath_utils:join([DirName, FileName])};
            undefined ->
                {FileGuid, <<"">>}
        end,

        ParentId = case ParentGuidOrSpaceId of
            space_id ->
                SpaceId;
            Guid ->
                {ok, ObjectId} = file_id:guid_to_objectid(Guid),
                ObjectId
        end,

        api_test_memory:set(MemRef, test_mode, TestMode),
        api_test_memory:set(MemRef, follow_symlinks, maps:get(<<"follow_symlinks">>, Data0, true)),
        ParentId2 = case TestMode of
            normal_mode ->
                api_test_memory:set(MemRef, scope, private),
                ParentId

        end,

        {Id, Data1} = api_test_utils:maybe_substitute_bad_id(ParentId2, Data0),
        DataWithoutPath = maps:remove(<<"path">>, Data1),

        RestPath = filepath_utils:join([<<"data">>, Id, <<"path">>, Path]),
        #rest_args{
            method = get,
            path = http_utils:append_url_parameters(
                RestPath,
                maps:with([<<"follow_symlinks">>], DataWithoutPath)
            ),
            headers = case maps:get(?HDR_RANGE, DataWithoutPath, undefined) of
                undefined -> #{};
                Range -> #{?HDR_RANGE => element(1, Range)}
            end
        }
    end.

%% @private
-spec build_rest_download_file_at_path_verify_fun(
    api_test_memory:mem_ref(),
    file_meta:size()
) ->
    onenv_api_test_runner:verify_fun().
build_rest_download_file_at_path_verify_fun(MemRef, FileSize) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    Providers = [P1Node, P2Node],

    fun
        (expected_failure, _) ->
            [#object{guid = DirGuid, children = [
                #object{guid = FileGuid}
            ]}] = api_test_memory:get(MemRef, file_tree_object),
            file_test_utils:await_distribution(Providers, [FileGuid], [{P1Node, FileSize}]);
        (expected_success, #api_test_ctx{node = DownloadNode, data = Data}) ->
            [#object{guid = DirGuid, children = [
                #object{guid = FileGuid}
            ]}] = api_test_memory:get(MemRef, file_tree_object),

            case DownloadNode of
                ?ONEZONE_TARGET_NODE ->
                    % The request was made via Onezone's shared data redirector,
                    % we do not have information where it was redirected here.
                    % Still, the target node is randomized, and other cases
                    % cover all possible resulting distributions.
                    ok;
                P1Node ->
                    ct:pal("p1node", []),
                    file_test_utils:await_distribution(Providers, FileGuid, [{P1Node, FileSize}]);
                _Other ->
                    ct:pal("other", []),
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
                            file_test_utils:await_distribution(Providers, [FileGuid], [{P1Node, FileSize}])
                    end
            end
    end.




rest_download_dir_test(Config) ->
    Providers = ?config(op_worker_nodes, Config),

    SpaceId = oct_background:get_space_id(space_krk_par),
    #object{guid = DirGuid, shares = [DirShareId]} =
        onenv_file_test_utils:create_and_sync_file_tree(
            user3, SpaceId, #dir_spec{shares = [#share_spec{}]}, krakow),

    MemRef = api_test_memory:init(),

    DirSpec = #dir_spec{mode = 8#705, shares = [#share_spec{}], children = [
        #symlink_spec{symlink_value = make_symlink_target()},
        #dir_spec{},
        #file_spec{content = ?RAND_CONTENT()}
    ]},
    SetupFun = build_download_file_setup_fun(MemRef, DirSpec),
    ValidateCallResultFun = fun(#api_test_ctx{client = Client}, {ok, RespCode, _RespHeaders, RespBody}) ->
        ?assertEqual(?HTTP_200_OK, RespCode),
        [FileTreeObject] = api_test_memory:get(MemRef, file_tree_object),
        User4Id = oct_background:get_user_id(user4),
        case {Client, api_test_memory:get(MemRef, test_mode)} of
            {?USER(User4Id), normal_mode} -> check_tarball(MemRef, RespBody, FileTreeObject, no_files);
            _ -> check_tarball(MemRef, RespBody, FileTreeObject)
        end
    end,

    DataSpec = #data_spec{
        optional = [<<"follow_symlinks">>],
        correct_values = #{
            <<"follow_symlinks">> => [true, false]
        }
    },

    ?assert(onenv_api_test_runner:run_tests([
        #scenario_spec{
            name = <<"Download dir using rest endpoint">>,
            type = rest,
            target_nodes = Providers,
            client_spec = ?TARBALL_DOWNLOAD_CLIENT_SPEC,

            setup_fun = SetupFun,
            prepare_args_fun = build_rest_download_prepare_args_fun(MemRef, normal_mode),
            validate_result_fun = ValidateCallResultFun,
            verify_fun = build_download_file_verify_fun(MemRef),

            % correct data is set up in build_rest_download_prepare_args_fun/2
            data_spec = api_test_utils:add_file_id_errors_for_operations_available_in_share_mode(
                DirGuid, undefined, DataSpec)
        },
        #scenario_spec{
            name = <<"Download shared dir using rest endpoint">>,
            type = {rest_with_shared_guid, file_id:guid_to_space_id(DirGuid)},
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SHARES,

            setup_fun = SetupFun,
            prepare_args_fun = build_rest_download_prepare_args_fun(MemRef, share_mode),
            validate_result_fun = ValidateCallResultFun,
            verify_fun = build_download_file_verify_fun(MemRef),

            % correct data is set up in build_rest_download_prepare_args_fun/2
            data_spec = api_test_utils:add_file_id_errors_for_operations_available_in_share_mode(
                DirGuid, DirShareId, DataSpec
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
        api_test_memory:set(MemRef, follow_symlinks, maps:get(<<"follow_symlinks">>, Data0, true)),
        [#object{guid = Guid, shares = Shares}] = api_test_memory:get(MemRef, file_tree_object),
        FileGuid = case TestMode of
            normal_mode ->
                api_test_memory:set(MemRef, scope, private),
                Guid;
            share_mode ->
                api_test_memory:set(MemRef, scope, public),
                case Shares of
                    [ShareId | _] -> file_id:guid_to_share_guid(Guid, ShareId);
                    _ -> Guid
                end
        end,
        {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),
        {Id, Data1} = api_test_utils:maybe_substitute_bad_id(FileObjectId, Data0),

        RestPath = <<"data/", Id/binary, "/content">>,
        #rest_args{
            method = get,
            path = http_utils:append_url_parameters(
                RestPath,
                maps:with([<<"follow_symlinks">>], Data1)
            ),
            headers = case maps:get(?HDR_RANGE, Data1, undefined) of
                undefined -> #{};
                Range -> #{?HDR_RANGE => element(1, Range)}
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

        ExpContent = case api_test_memory:get(MemRef, file_tree_object) of
            % download file at path testcase
            [#object{children = [#object{content = Content}]}] -> Content;
            % download file by id testcase
            [#object{content = Content}] -> Content
        end,

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
        Object = onenv_file_test_utils:create_and_sync_file_tree(
            user3, SpaceId, utils:ensure_list(Spec), krakow
        ),
        api_test_memory:set(MemRef, file_tree_object, Object)
    end.


%% @private
-spec download_file_using_download_code(node(), FileDownloadUrl :: binary()) ->
    {ok, Content :: binary()} | {error, term()}.
download_file_using_download_code(Node, FileDownloadUrl) ->
    check_download_result(http_client:request(get, FileDownloadUrl, #{}, <<>>, get_download_opts(Node))).


%% @private
-spec download_file_using_download_code_with_resumes(node(), FileDownloadUrl :: binary()) ->
    {ok, Content :: binary()} | {error, term()}.
download_file_using_download_code_with_resumes(Node, FileDownloadUrl) ->
    InitDownloadFun = fun(Begin) ->
        {ok, _Ref} = http_client:request_return_stream(get, FileDownloadUrl, build_range_header(Begin), <<>>, get_download_opts(Node))
    end,
    Self = self(),
    spawn(fun() -> InitDownloadFun(0), failing_download_client(Self) end),
    check_download_result(async_download(#{}, InitDownloadFun)).

%% @private
-spec get_download_opts(node()) -> http_client:opts().
get_download_opts(Node) ->
    CaCerts = opw_test_rpc:get_cert_chain_ders(Node),
    [{ssl_options, [{cacerts, CaCerts}]}, {recv_timeout, infinity}].


%% @private
-spec check_download_result(http_client:response()) -> {ok, http_client:body()} | {error, term()}.
check_download_result({ok, ?HTTP_200_OK, _RespHeaders, RespBody}) ->
    {ok, RespBody};
check_download_result({ok, ?HTTP_206_PARTIAL_CONTENT, _RespHeaders, RespBody}) ->
    {ok, RespBody};
check_download_result({ok, _RespCode, _RespHeaders, RespBody}) ->
    errors:from_json(maps:get(<<"error">>, json_utils:decode(RespBody)));
check_download_result({error, _} = Error) ->
    Error.


%% @private
-spec build_range_header(non_neg_integer()) -> http_client:headers().
build_range_header(Begin) ->
    #{?HDR_RANGE => <<"bytes=", (integer_to_binary(Begin))/binary, "-">>}.


%% @private
-spec async_download(map(), fun((non_neg_integer()) -> {ok, term()})) -> http_client:response().
async_download(ResultMap, DownloadFun) ->
    receive
        {hackney_response, _Ref, {status, StatusInt, _Reason}} ->
            async_download(ResultMap#{status => StatusInt}, DownloadFun);
        {hackney_response, _Ref, {headers, Headers}} ->
            async_download(ResultMap#{headers => Headers}, DownloadFun);
        {hackney_response, _Ref, done} ->
            {ok, maps:get(status, ResultMap), maps:get(headers, ResultMap), maps:get(body, ResultMap)};
        {hackney_response, _Ref, Bin} ->
            PrevBin = maps:get(body, ResultMap, <<>>),
            async_download(ResultMap#{body => <<PrevBin/binary, Bin/binary>>}, DownloadFun);
        killed ->
            Self = self(),
            spawn(fun() -> DownloadFun(byte_size(maps:get(body, ResultMap, <<>>))), failing_download_client(Self) end),
            async_download(ResultMap, DownloadFun)
    end.


%% @private
-spec failing_download_client(pid()) -> boolean().
failing_download_client(Pid) ->
    failing_download_client(Pid, 8).


%% @private
-spec failing_download_client(pid(), non_neg_integer()) -> boolean().
failing_download_client(Pid, 0) ->
    Pid ! killed,
    exit(kill);
failing_download_client(Pid, ChunksUntilFail) ->
    receive
        {hackney_response, _Ref, done} = Msg ->
            Pid ! Msg;
        Msg ->
            Pid ! Msg,
            failing_download_client(Pid, ChunksUntilFail - 1)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Compares provided tarball (as gzip compressed Bytes) against expected 
%% structure provided as FileTreeObject.
%% Files strategy describes how files check should be performed:
%%      check_files_content -> checks that appropriate files are in tarball and have proper content;
%%      no_files -> checks that there are no files.
%% @end
%%--------------------------------------------------------------------
-spec check_tarball(api_test_memory:mem_ref(), binary(), binary()) -> ok.
check_tarball(MemRef, Bytes, FileTreeObject) ->
    check_tarball(MemRef, Bytes, FileTreeObject, check_files_content).


%% @private
-spec check_tarball(api_test_memory:mem_ref(), binary(), binary(), files_strategy()) -> ok.
check_tarball(MemRef, Bytes, FileTreeObject, FilesStrategy) ->
    check_extracted_tarball_structure(MemRef, FileTreeObject, FilesStrategy, unpack_tarball(Bytes), root_dir).


%% @private
-spec check_extracted_tarball_structure(
    api_test_memory:mem_ref(), onenv_file_test_utils:object_spec(), files_strategy(), binary(), child | root_dir
) ->
    ok.
check_extracted_tarball_structure(MemRef, #object{type = ?DIRECTORY_TYPE} = Object, FilesStrategy, CurrentPath, DirType) ->
    #object{name = Dirname, children = Children} = Object,
    {ok, TmpDirContentAfter} = file:list_dir(CurrentPath),
    ExpectDir = FilesStrategy == check_files_content orelse DirType == root_dir,
    ?assertEqual(ExpectDir, lists:member(binary_to_list(Dirname), TmpDirContentAfter)),
    lists:foreach(fun(Child) ->
        check_extracted_tarball_structure(MemRef, Child, FilesStrategy, filename:join(CurrentPath, Dirname), child)
    end, Children);
check_extracted_tarball_structure(_MemRef, #object{type = ?REGULAR_FILE_TYPE} = Object, check_files_content, CurrentPath, _) ->
    #object{name = Filename, content = ExpContent} = Object,
    ?assertEqual({ok, ExpContent}, file:read_file(filename:join(CurrentPath, Filename)));
check_extracted_tarball_structure(MemRef, #object{type = ?SYMLINK_TYPE} = Object, check_files_content, CurrentPath, _) ->
    check_symlink(MemRef, CurrentPath, Object, api_test_memory:get(MemRef, scope, private));
check_extracted_tarball_structure(MemRef, #object{type = ?SYMLINK_TYPE} = Object, _, CurrentPath, root_dir) ->
    check_symlink(MemRef, CurrentPath, Object, api_test_memory:get(MemRef, scope, private));
check_extracted_tarball_structure(_MemRef, #object{name = Filename}, no_files, CurrentPath, _ParentDirType) ->
    {ok, TmpDirContentAfter} = file:list_dir(CurrentPath),
    ?assertEqual(false, lists:member(binary_to_list(Filename), TmpDirContentAfter)).


%% @private
-spec check_symlink(api_test_memory:mem_ref(), file_meta:path(), onenv_file_test_utils:object_spec(), public | private) -> ok.
check_symlink(MemRef, CurrentPath, Object, private) ->
    #object{name = Filename, symlink_value = LinkPath} = Object,
    case api_test_memory:get(MemRef, follow_symlinks) of
        true ->
            % link target files has its name as content
            ?assertEqual({ok, filename:basename(LinkPath)}, file:read_file(filename:join(CurrentPath, Filename)));
        false ->
            ?assertEqual({ok, binary_to_list(LinkPath)}, file:read_link(filename:join(CurrentPath, Filename)))
    end;
check_symlink(_MemRef, CurrentPath, Object, public) ->
    #object{name = Filename} = Object,
    % link targets outside share scope - it should not be downloaded
    ?assertEqual({error, enoent}, file:read_file(filename:join(CurrentPath, Filename))).


%% @private
-spec unpack_tarball(binary()) -> binary().
unpack_tarball(Bytes) ->
    TmpDir = mochitemp:mkdtemp(),
    ok = erl_tar:extract({binary, Bytes}, [{cwd, TmpDir}]),
    TmpDir.


%% @private
-spec make_hardlink(test_config:config(), fslogic_worker:file_guid(), fslogic_worker:file_guid()) ->
    {ok, onenv_file_test_utils:object_spec()}.
make_hardlink(Config, TargetGuid, ParentGuid) ->
    UserSessId = oct_background:get_user_session_id(user3, krakow),
    [Node | _] = oct_background:get_provider_nodes(krakow),
    LinkName = generator:gen_name(),
    {ok, #file_attr{guid = LinkGuid}} = lfm_proxy:make_link(
        Node, UserSessId, ?FILE_REF(TargetGuid), ?FILE_REF(ParentGuid), LinkName
    ),
    Providers = ?config(op_worker_nodes, Config),
    lists:foreach(fun(Worker) ->
        SessId = oct_background:get_user_session_id(user3, rpc:call(Worker, oneprovider, get_id, [])),
        ?assertMatch({ok, _}, lfm_proxy:stat(Worker, SessId, ?FILE_REF(LinkGuid)), ?ATTEMPTS)
    end, Providers),
    onenv_file_test_utils:get_object_attributes(Node, UserSessId, LinkGuid).


%% @private
-spec make_symlink_target() -> file_meta_symlinks:symlink().
make_symlink_target() ->
    SpaceId = oct_background:get_space_id(space_krk_par),
    Name = ?RANDOM_FILE_NAME(),
    Object = onenv_file_test_utils:create_and_sync_file_tree(
        user3, SpaceId, #file_spec{name = Name, content = Name}, krakow
    ),
    make_symlink_target(SpaceId, Object).


%% @private
-spec make_symlink_target(od_space:id(), onenv_file_test_utils:object_spec()) ->
    file_meta_symlinks:symlink().
make_symlink_target(SpaceId, Object) ->
    make_symlink_target(SpaceId, <<"">>, Object).


%% @private
-spec make_symlink_target(od_space:id(), file_meta:path(), onenv_file_test_utils:object_spec()) ->
    file_meta_symlinks:symlink().
make_symlink_target(SpaceId, ParentPath, #object{name = Name}) ->
    SpaceIdSymlinkPrefix = ?SYMLINK_SPACE_ID_ABS_PATH_PREFIX(SpaceId),
    filename:join([SpaceIdSymlinkPrefix, ParentPath, Name]).

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
                {max_download_buffer_size, 2 * ?DEFAULT_READ_BLOCK_SIZE},

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
                ErrorFun = fun(FileAttrs, Req) ->
                    ShouldBlock = lists:any(fun(#file_attr{guid = Guid}) ->
                        {Uuid, _, _} = file_id:unpack_share_guid(Guid),
                        node_cache:get({block_file, Uuid}, false)
                    end, utils:ensure_list(FileAttrs)),
                    case ShouldBlock of
                        true -> http_req:send_error(?ERROR_POSIX(?EAGAIN), Req);
                        false -> passthrough
                    end
                end,
                ok = test_utils:mock_expect(OpNode, file_download_utils, download_single_file,
                    fun(SessionId, FileAttrs, Callback, Req) ->
                        case ErrorFun(FileAttrs, Req) of
                            passthrough -> meck:passthrough([SessionId, FileAttrs, Callback, Req]);
                            Res -> Res
                        end
                    end),
                ok = test_utils:mock_expect(OpNode, file_download_utils, download_tarball,
                    fun(Id, SessionId, FileAttrs, FollowSymlinks, Req) ->
                        case ErrorFun(FileAttrs, Req) of
                            passthrough -> meck:passthrough([Id, SessionId, FileAttrs, FollowSymlinks, Req]);
                            Res -> Res
                        end
                    end)
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
    utils:rpc_multicall(Providers, file_middleware_plugin, resolve_handler, [get, download_url, private]),
    init_per_testcase(?DEFAULT_CASE(Case), Config);
init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 40}),
    Config.

end_per_testcase(_Case, _Config) ->
    ok.


