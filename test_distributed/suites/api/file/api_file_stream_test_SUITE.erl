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
-include("modules/logical_file_manager/lfm.hrl").
-include("onenv_test_utils.hrl").
-include("proto/oneclient/common_messages.hrl").
-include_lib("ctool/include/test/performance.hrl").
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
    gui_download_files_between_spaces_test/1,
    gui_download_incorrect_uuid_test/1,
    gui_download_tarball_with_hardlinks_test/1,
    gui_download_tarball_with_symlink_loop_test/1,
    rest_download_file_test/1,
    rest_download_file_at_path_test/1,
    rest_download_dir_test/1,
    rest_download_dir_at_path_test/1,

    sync_first_file_block_test/1
]).

% Exported for performance tests
-export([
    gui_download_file_test_base/1,
    gui_download_dir_test_base/1,
    gui_download_multiple_files_test_base/1
]).

groups() -> [
    {parallel_tests, [parallel], [
        gui_download_file_test,
        gui_download_dir_test,
        gui_download_multiple_files_test,
        gui_download_different_filetypes_test,
        gui_download_files_between_spaces_test,
        gui_download_incorrect_uuid_test,
        gui_download_tarball_with_hardlinks_test,
        gui_download_tarball_with_symlink_loop_test,
        rest_download_file_test,
        rest_download_file_at_path_test,
        rest_download_dir_test,
        rest_download_dir_at_path_test
    ]},
    {sequential_tests, [], [
        sync_first_file_block_test
    ]}
].

-define(STANDARD_CASES, [
    {group, parallel_tests},
    {group, sequential_tests}
]).

-define(PERFORMANCE_CASES, [
    gui_download_file_test,
    gui_download_dir_test,
    gui_download_multiple_files_test
]).

all() -> ?ALL(?STANDARD_CASES, ?PERFORMANCE_CASES).

-define(DEFAULT_READ_BLOCK_SIZE, 1024).
-define(GUI_DOWNLOAD_CODE_EXPIRATION_SECONDS, 20).

-define(ATTEMPTS, 30).

-define(FILESIZE, 4 * ?DEFAULT_READ_BLOCK_SIZE).
-define(RAND_CONTENT(), ?RAND_CONTENT(rand:uniform(3) * ?FILESIZE)).
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

-define(ALL_RANGES_TO_TEST(FileSize), [
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
    ?PERFORMANCE(Config, [
        {repeats, 1},
        {success_rate, 100},
        {parameters, [
            [{name, test_type}, {value, standard}, {description, "Type of test"}]
        ]},
        {description, "Tests download of file via GUI"},
        {config, [{name, performance},
            {parameters, [
                [{name, test_type}, {value, performance}]
            ]}
        ]}
    ]).
gui_download_file_test_base(Config) ->
    ClientSpec = #client_spec{
        correct = [
            user2,  % space owner - doesn't need any perms
            user3   % files owner
        ],
        unauthorized = [nobody],
        forbidden_not_in_space = [user1],
        forbidden_in_space = [user4]
    },
    Content = case ?config(test_type, Config) of
        standard -> ?RAND_CONTENT();
        performance -> ?RAND_CONTENT(20 * ?FILESIZE)
    end,
    FileSpec = #file_spec{mode = 8#604, content = Content, shares = [#share_spec{}]},
    gui_download_test_base(Config, FileSpec, ClientSpec, <<"File">>, #{download_type => uninterrupted_download}).

gui_download_dir_test(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 1},
        {success_rate, 100},
        {parameters, [
            [{name, test_type}, {value, standard}, {description, "Type of test"}]
        ]},
        {description, "Tests download of directory via GUI"},
        {config, [{name, performance},
            {parameters, [
                [{name, test_type}, {value, performance}]
            ]}
        ]}
    ]).
gui_download_dir_test_base(Config) ->
    {ClientSpec, DirSpec} = case ?config(test_type, Config) of
        standard ->
            {?TARBALL_DOWNLOAD_CLIENT_SPEC, #dir_spec{mode = 8#705,
                shares = [#share_spec{}], children = [#dir_spec{}, #file_spec{content = ?RAND_CONTENT()}]}};
        performance ->
            CS = #client_spec{
                correct = [
                    user2  % space owner - doesn't need any perms
                ]
            },
            ChildrenSpecGen = fun
                F(0) -> [];
                F(Depth) ->
                    Children = F(Depth - 1),
                    [#dir_spec{children = Children},
                        #dir_spec{children = Children}, #file_spec{content = ?RAND_CONTENT(100)}]
            end,
            DS = #dir_spec{mode = 8#705, shares = [#share_spec{}], children = ChildrenSpecGen(7)},
            {CS, DS}
    end,
    gui_download_test_base(Config, DirSpec, ClientSpec, <<"Dir">>).

gui_download_multiple_files_test(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 1},
        {success_rate, 100},
        {parameters, [
            [{name, test_type}, {value, standard}, {description, "Type of test"}]
        ]},
        {description, "Tests download of file via GUI"},
        {config, [{name, performance},
            {parameters, [
                [{name, test_type}, {value, performance}]
            ]}
        ]}
    ]).
gui_download_multiple_files_test_base(Config) ->
    GetContentFun = case ?config(test_type, Config) of
        standard -> fun() -> ?RAND_CONTENT() end;
        performance -> fun() -> ?RAND_CONTENT(20 * ?FILESIZE) end
    end,
    ClientSpec = ?TARBALL_DOWNLOAD_CLIENT_SPEC,
    DirSpec = [
        #file_spec{mode = 8#604, shares = [#share_spec{}], content = GetContentFun()},
        #file_spec{mode = 8#604, shares = [#share_spec{}], content = GetContentFun()},
        #file_spec{mode = 8#604, shares = [#share_spec{}], content = GetContentFun()},
        #file_spec{mode = 8#604, shares = [#share_spec{}], content = GetContentFun()}
    ],
    gui_download_test_base(Config, DirSpec, ClientSpec, <<"Multiple files">>).

gui_download_different_filetypes_test(Config) ->
    ClientSpec = ?TARBALL_DOWNLOAD_CLIENT_SPEC,
    Name = ?RANDOM_FILE_NAME(),
    DirSpec = [
        #symlink_spec{symlink_value = make_symlink_target()},
        #dir_spec{mode = 8#705, shares = [#share_spec{}], children = [
            #dir_spec{},
            #file_spec{content = Name, name = Name, custom_label = Name},
            #dir_spec{
                children = [#symlink_spec{symlink_value = {custom_label, Name}}]
            }
        ]},
        #file_spec{mode = 8#604, shares = [#share_spec{}], content = ?RAND_CONTENT()}
    ],
    gui_download_test_base(Config, DirSpec, ClientSpec, <<"Different filetypes">>).

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
        ?assertMatch(
            ?ERROR_POSIX(?ENOENT),
            download_file_using_download_code_with_resumes(MemRef, DownloadNode, FileDownloadUrl)
        )
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
        SpaceGuid = fslogic_file_id:spaceid_to_space_dir_guid(SpaceId),
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
    gui_download_test_base(Config, FileTreeSpec, ClientSpec, ScenarioPrefix, #{}).

%% @private
-spec gui_download_test_base(
    test_config:config(),
    onenv_file_test_utils:object_spec() | [onenv_file_test_utils:file_spec()],
    onenv_api_test_runner:client_spec(),
    binary(),
    #{
        download_type => simulate_failures | uninterrupted_download
    }
) ->
    ok.
gui_download_test_base(Config, FileTreeSpec, ClientSpec, ScenarioPrefix, Opts) ->
    Providers = ?config(op_worker_nodes, Config),

    SpaceId = oct_background:get_space_id(space_krk_par),
    #object{guid = DirGuid, shares = [DirShareId]} =
        onenv_file_test_utils:create_and_sync_file_tree(
            user3, SpaceId, #dir_spec{shares = [#share_spec{}]}, krakow),

    MemRef = api_test_memory:init(),
    api_test_memory:set(MemRef, download_type, maps:get(download_type, Opts, simulate_failures)),

    BuildSetupFun = fun
        (shared_files) ->
            % filter out symlinks that cannot be shared
            build_download_file_setup_fun(MemRef, lists:filter(fun
                (#symlink_spec{}) -> false;
                (_OtherTypeSpec) -> true
            end, utils:ensure_list(FileTreeSpec)));
        (private_files) ->
            build_download_file_setup_fun(MemRef, FileTreeSpec)
    end,
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
                        ?ERROR_SPACE_NOT_SUPPORTED_BY(SpaceId, ?GET_DOMAIN_BIN(Node))
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
            setup_fun = BuildSetupFun(private_files),
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
            setup_fun = BuildSetupFun(shared_files),
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
            setup_fun = BuildSetupFun(shared_files),
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
            simulate_failures -> fun download_file_using_download_code_with_resumes/3;
            uninterrupted_download -> fun download_file_using_download_code/3
        end,
        case rand:uniform(2) of
            1 ->
                User4Id = oct_background:get_user_id(user4),
                % File download code should be still usable after unsuccessful download
                case Client of
                    % user4 does not have access to files, so list of files to download passed 
                    % to file_content_download_utils is empty, hence it cannot be blocked for specific guid
                    ?USER(User4Id) -> ok;
                    _ ->
                        block_file_streaming(DownloadNode, Guid),
                        ?assertEqual(?ERROR_POSIX(?EAGAIN), DownloadFunction(MemRef, DownloadNode, FileDownloadUrl)),
                        unblock_file_streaming(DownloadNode, Guid),
                        ?assertMatch({ok, _}, get_file_download_code_doc(DownloadNode, DownloadCode, memory))
                end,

                % But first successful download should make it unusable
                case FileTreeObject of
                    [#object{type = ?REGULAR_FILE_TYPE, content = ExpContent1}] ->
                        ?assertEqual({ok, ExpContent1}, DownloadFunction(MemRef, DownloadNode, FileDownloadUrl));
                    _ ->
                        {ok, Bytes} = ?assertMatch({ok, _}, DownloadFunction(MemRef, DownloadNode, FileDownloadUrl)),
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
                ?assertEqual(?ERROR_BAD_VALUE_ID_NOT_FOUND(<<"code">>), DownloadFunction(MemRef, DownloadNode, FileDownloadUrl)),

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
                ?assertEqual(?ERROR_BAD_VALUE_ID_NOT_FOUND(<<"code">>), DownloadFunction(MemRef, DownloadNode, FileDownloadUrl)),
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

            % tested file is in a single level file tree
            [#object{type = ?REGULAR_FILE_TYPE, guid = FileGuid, content = Content}] ->
                FileSize = size(Content),
                check_single_file_download_distribution(MemRef, ExpTestResult, ApiTestCtx, FileGuid, FileSize, Providers, P1Node, P2Node);

            % tested file is a directory in a two-level file tree
            [#object{type = ?DIRECTORY_TYPE, children = [
                #object{type = ?DIRECTORY_TYPE, children = [
                    #object{type = ?SYMLINK_TYPE},
                    #object{type = ?DIRECTORY_TYPE},
                    #object{type = ?REGULAR_FILE_TYPE}
                ]}]} = TestedTreeObject] ->
                ExpTestResult1 = case {ExpTestResult, api_test_memory:get(MemRef, download_succeeded, undefined)} of
                    {expected_failure, _} -> expected_failure;
                    {_, false} -> expected_failure;
                    {_, _} -> expected_success
                end,
                lists:foreach(fun(Object) ->
                    check_tarball_download_distribution(MemRef, ExpTestResult1, ApiTestCtx, Object, Providers, P1Node, P2Node)
                end, utils:ensure_list(TestedTreeObject));
            % tested file is a directory in a single level file-tree
            _ ->
                ExpTestResult1 = case {ExpTestResult, api_test_memory:get(MemRef, download_succeeded, undefined)} of
                    {expected_failure, _} -> expected_failure;
                    {_, false} -> expected_failure;
                    {_, _} -> expected_success
                end,
                lists:foreach(fun(Object) ->
                    check_tarball_download_distribution(MemRef, ExpTestResult1, ApiTestCtx, Object, Providers, P1Node, P2Node)
                end, utils:ensure_list(FileTreeObject))
        end
    end.

%% @private
-spec check_single_file_download_distribution(
    api_test_memory:mem_ref(), expected_success | expected_failure, onenv_api_test_runner:api_test_ctx(),
    file_id:file_guid(), file_meta:size(), [oneprovider:id()], node(), node()
) -> ok.
check_single_file_download_distribution(_MemRef, expected_failure, _, FileGuid, FileSize, Providers, P1Node, P2Node) ->
    file_test_utils:await_distribution(Providers, FileGuid, [{P1Node, FileSize}, {P2Node, 0}]);
check_single_file_download_distribution(_MemRef, expected_success, #api_test_ctx{node = P1Node}, FileGuid, FileSize, Providers, P1Node, P2Node) ->
    file_test_utils:await_distribution(Providers, FileGuid, [{P1Node, FileSize}, {P2Node, 0}]);
check_single_file_download_distribution(MemRef, expected_success, #api_test_ctx{node = DownloadNode}, FileGuid, FileSize, Providers, P1Node, _P2Node) ->
    FirstBlockFetchedSize = min(FileSize, ?DEFAULT_READ_BLOCK_SIZE),
    ExpDist = case api_test_memory:get(MemRef, download_succeeded, true) of
        true -> [{P1Node, FileSize}, {DownloadNode, FileSize}];
        false -> [{P1Node, FileSize}, {DownloadNode, FirstBlockFetchedSize}]
    end,
    file_test_utils:await_distribution(Providers, FileGuid, ExpDist).


%% @private
-spec check_tarball_download_distribution(
    api_test_memory:mem_ref(), expected_success | expected_failure, onenv_api_test_runner:api_test_ctx(),
    onenv_file_test_utils:object_spec(), [oneprovider:id()], node(), node()
) -> ok.
check_tarball_download_distribution(_, _, #api_test_ctx{node = ?ONEZONE_TARGET_NODE}, _, _, _, _) ->
    % The request was made via Onezone's shared data redirector,
    % we do not have information where it was redirected here.
    % Still, the target node is randomized, and other cases
    % cover all possible resulting distributions.
    ok;
check_tarball_download_distribution(_MemRef, _ExpTestResult, _ApiTestCtx, #object{type = ?SYMLINK_TYPE}, _Providers, _P1Node, _P2Node) ->
    ok;
check_tarball_download_distribution(_MemRef, expected_failure, _, #object{type = ?REGULAR_FILE_TYPE} = Object, Providers, P1Node, P2Node) ->
    #object{guid = FileGuid, content = Content} = Object,
    FileSize = size(Content),
    file_test_utils:await_distribution(Providers, FileGuid, [{P1Node, FileSize}, {P2Node, 0}]);
check_tarball_download_distribution(_MemRef, expected_success, #api_test_ctx{node = P1Node}, #object{type = ?REGULAR_FILE_TYPE} = Object, Providers, P1Node, P2Node) ->
    #object{guid = FileGuid, content = Content} = Object,
    FileSize = size(Content),
    file_test_utils:await_distribution(Providers, FileGuid, [{P1Node, FileSize}, {P2Node, 0}]);
check_tarball_download_distribution(MemRef, expected_success, ApiTestCtx, #object{type = ?REGULAR_FILE_TYPE} = Object, Providers, P1Node, P2Node) ->
    #api_test_ctx{node = DownloadNode, client = Client} = ApiTestCtx,
    #object{guid = FileGuid, content = Content} = Object,
    FileSize = size(Content),
    User4Id = oct_background:get_user_id(user4),
    ExpDist = case {Client, api_test_memory:get(MemRef, scope)} of
        {?USER(User4Id), private} -> [{P1Node, FileSize}, {P2Node, 0}];
        _ -> [{P1Node, FileSize}, {DownloadNode, FileSize}]
    end,
    file_test_utils:await_distribution(Providers, FileGuid, ExpDist);
check_tarball_download_distribution(MemRef, ExpTestResult, ApiTestCtx, #object{type = ?DIRECTORY_TYPE, children = Children}, Providers, P1Node, P2Node) ->
    lists:foreach(fun(Child) ->
        check_tarball_download_distribution(MemRef, ExpTestResult, ApiTestCtx, Child, Providers, P1Node, P2Node)
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
    Content = ?RAND_CONTENT(FileSize),
    api_test_memory:set(MemRef, expected_content, Content),
    SetupFun = build_download_file_setup_fun(MemRef, #file_spec{mode = 8#604, content = Content, shares = [#share_spec{}]}),
    ValidateCallResultFun = build_rest_download_file_validate_call_fun(MemRef),
    VerifyFun = build_rest_download_file_verify_fun(MemRef, FileSize),

    AllRangesToTestNum = length(?ALL_RANGES_TO_TEST(FileSize)),

    % Randomly split range to test so to shorten test execution time by not
    % repeating every combination several times
    RangesToTestPart1 = lists_utils:random_sublist(
        ?ALL_RANGES_TO_TEST(FileSize), AllRangesToTestNum div 2, AllRangesToTestNum div 2
    ),
    RangesToTestPart2 = ?ALL_RANGES_TO_TEST(FileSize) -- RangesToTestPart1,

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

    Content = ?RAND_CONTENT(FileSize),
    api_test_memory:set(MemRef, expected_content, Content),
    SetupFun = build_download_file_setup_fun(MemRef, #dir_spec{
        shares = [#share_spec{}],
        children = [#file_spec{mode = 8#604, content = Content, shares = [#share_spec{}]}]
    }),
    ValidateCallResultFun = build_rest_download_file_validate_call_fun(MemRef),
    VerifyFun = build_rest_download_file_verify_fun(MemRef, FileSize),

    AllRangesToTestNum = length(?ALL_RANGES_TO_TEST(FileSize)),

    % Randomly split range to test so to shorten test execution time by not
    % repeating every combination several times
    RangesToTestPart1 = lists_utils:random_sublist(
        ?ALL_RANGES_TO_TEST(FileSize), AllRangesToTestNum div 2, AllRangesToTestNum div 2
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
                            filename_only_relative_to_parent_dir_placeholder,
                            directory_and_filename_relative_to_space_root_dir_placeholder,
                            directory_and_filename_relative_to_space_id_placeholder
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

        [#object{guid = DirGuid, name = ParentDirName, children = [
            #object{guid = FileGuid, name = FileName} = TargetFileTreeObject
        ]}] = api_test_memory:get(MemRef, file_tree_object),

        ExpectedDownloadedFileName = case TargetFileTreeObject of
            #object{type = ?DIRECTORY_TYPE, name = DirName} -> <<DirName/binary, ".tar">>;
            #object{name = FileName} -> FileName
        end,
        api_test_memory:set(MemRef, expected_downloaded_file_name, ExpectedDownloadedFileName),

        {ParentGuidOrSpaceId, Path} = case maps:get(<<"path">>, Data0, undefined) of
            filename_only_relative_to_parent_dir_placeholder ->
                {DirGuid, FileName};
            directory_and_filename_relative_to_space_root_dir_placeholder ->
                SpaceRootDirGuid = fslogic_file_id:spaceid_to_space_dir_guid(SpaceId),
                {SpaceRootDirGuid, filepath_utils:join([ParentDirName, FileName])};
            directory_and_filename_relative_to_space_id_placeholder ->
                {space_id, filepath_utils:join([ParentDirName, FileName])};
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

        RestPath = str_utils:join_as_binaries([<<"data">>, Id, <<"path">>, Path], <<"/">>),
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
    ValidateCallResultFun = fun(#api_test_ctx{client = Client}, {ok, RespCode, RespHeaders, RespBody}) ->
        ?assertEqual(?HTTP_200_OK, RespCode),
        check_content_disposition_header(MemRef, RespHeaders),
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


rest_download_dir_at_path_test(_Config) ->
    Providers = oct_background:get_all_providers_nodes(),

    SpaceId = oct_background:get_space_id(space_krk_par),
    #object{guid = DirGuid} =
        onenv_file_test_utils:create_and_sync_file_tree(
            user3, SpaceId, #dir_spec{shares = [#share_spec{}]}, krakow),

    MemRef = api_test_memory:init(),
    TargetDirSpec = #dir_spec{mode = 8#705, shares = [#share_spec{}], children = [
        #symlink_spec{symlink_value = make_symlink_target()},
        #dir_spec{},
        #file_spec{content = ?RAND_CONTENT()}
    ]},
    DirTreeSpec = #dir_spec{mode = 8#705, shares = [#share_spec{}], children = [TargetDirSpec]},
    SetupFun = build_download_file_setup_fun(MemRef, DirTreeSpec),
    ValidateCallResultFun = fun(#api_test_ctx{client = Client}, {ok, RespCode, RespHeaders, RespBody}) ->
        ?assertEqual(?HTTP_200_OK, RespCode),
        check_content_disposition_header(MemRef, RespHeaders),
        [#object{children = [DirObject]}] = api_test_memory:get(MemRef, file_tree_object),
        User4Id = oct_background:get_user_id(user4),
        case {Client, api_test_memory:get(MemRef, test_mode)} of
            {?USER(User4Id), normal_mode} -> check_tarball(MemRef, RespBody, DirObject, no_files);
            _ -> check_tarball(MemRef, RespBody, DirObject)
        end
    end,

    DataSpec = #data_spec{
        optional = [<<"follow_symlinks">>, <<"path">>],
        correct_values = #{
            <<"follow_symlinks">> => [true, false],
            <<"path">> => [
                filename_only_relative_to_parent_dir_placeholder,
                directory_and_filename_relative_to_space_root_dir_placeholder,
                directory_and_filename_relative_to_space_id_placeholder
            ]
        }
    },

    ?assert(onenv_api_test_runner:run_tests([
        #scenario_spec{
            name = <<"Download dir at path using rest endpoint">>,
            type = rest,
            target_nodes = Providers,
            client_spec = #client_spec{
                correct = [
                    user2,  % space owner - doesn't need any perms
                    user3  % files owner
                ],
                unauthorized = [nobody],
                forbidden_not_in_space = [user1]},

            setup_fun = SetupFun,
            prepare_args_fun = build_rest_download_file_at_path_prepare_args_fun(MemRef, normal_mode),
            validate_result_fun = ValidateCallResultFun,
            verify_fun = build_download_file_verify_fun(MemRef),

            % correct data is set up in build_rest_download_prepare_args_fun/2
            data_spec = api_test_utils:add_file_id_errors_for_operations_available_in_share_mode(
                DirGuid, undefined, DataSpec)
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
        [#object{guid = Guid, shares = Shares} = FileTreeObject] = api_test_memory:get(MemRef, file_tree_object),

        ExpectedDownloadedFileName = case FileTreeObject of
            #object{type = ?DIRECTORY_TYPE, name = DirName} -> <<DirName/binary, ".tar">>;
            #object{name = FileName} -> FileName
        end,
        api_test_memory:set(MemRef, expected_downloaded_file_name, ExpectedDownloadedFileName),

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
-spec build_rest_download_file_validate_call_fun(api_test_memory:mem_ref()) ->
    onenv_api_test_runner:validate_call_result_fun().
build_rest_download_file_validate_call_fun(MemRef) ->
    fun(#api_test_ctx{data = Data}, {ok, RespCode, RespHeaders, RespBody}) ->
        ExpContent = api_test_memory:get(MemRef, expected_content),
        FileSize = size(ExpContent),
        FileSizeBin = integer_to_binary(FileSize),
        Successful = case maps:get(<<"range">>, Data, undefined) of
            undefined ->
                ?assertEqual(?HTTP_200_OK, RespCode),
                ?assertEqual(ExpContent, RespBody),
                true;

            {_, ?HTTP_206_PARTIAL_CONTENT, [{RangeStart, RangeLen}]} ->
                ExpContentRange = str_utils:format_bin("bytes ~B-~B/~B", [
                    RangeStart, RangeStart + RangeLen - 1, FileSize
                ]),
                ExpPartialContent = binary:part(ExpContent, RangeStart, RangeLen),

                ?assertEqual(?HTTP_206_PARTIAL_CONTENT, RespCode),
                ?assertMatch(#{?HDR_CONTENT_RANGE := ExpContentRange}, RespHeaders),
                ?assertEqual(ExpPartialContent, RespBody),
                true;

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
                ?assertEqual(ExpPartialContent, RespBody),
                true;

            {_, ?HTTP_416_RANGE_NOT_SATISFIABLE} ->
                ?assertEqual(?HTTP_416_RANGE_NOT_SATISFIABLE, RespCode),
                ?assertMatch(#{?HDR_CONTENT_RANGE := <<"bytes */", FileSizeBin/binary>>}, RespHeaders),
                ?assertEqual(<<>>, RespBody),
                false
        end,
        Successful andalso check_content_disposition_header(MemRef, RespHeaders)
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
            FileGuid = case api_test_memory:get(MemRef, file_tree_object) of
                [#object{children = [#object{guid = Guid}]}] -> Guid;
                [#object{guid = Guid}] -> Guid
            end,
            file_test_utils:await_distribution(Providers, FileGuid, [{P1Node, FileSize}, {P2Node, 0}]);
        (expected_success, #api_test_ctx{node = DownloadNode, data = Data}) ->
            FileGuid = case api_test_memory:get(MemRef, file_tree_object) of
                [#object{children = [#object{guid = Guid}]}] -> Guid;
                [#object{guid = Guid}] -> Guid
            end,

            case DownloadNode of
                ?ONEZONE_TARGET_NODE ->
                    % The request was made via Onezone's shared data redirector,
                    % we do not have information where it was redirected here.
                    % Still, the target node is randomized, and other cases
                    % cover all possible resulting distributions.
                    ok;
                P1Node ->
                    file_test_utils:await_distribution(Providers, FileGuid, [{P1Node, FileSize}, {P2Node, 0}]);
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
                            file_test_utils:await_distribution(Providers, [FileGuid], [{P1Node, FileSize}, {P2Node, 0}])
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
%%% Misc tests
%%%===================================================================


sync_first_file_block_test(_Config) ->
    ParisNode = oct_background:get_random_provider_node(paris),

    FileBlocks = ?RAND_INT(2, 6),
    FileSize = FileBlocks * ?DEFAULT_READ_BLOCK_SIZE,
    #object{guid = FileGuid} = onenv_file_test_utils:create_and_sync_file_tree(
        user2, space_krk_par, #file_spec{content = ?RAND_CONTENT(FileSize)}, krakow
    ),
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),

    DownloadRestPath = <<"data/", FileObjectId/binary, "/content">>,
    HeadersWithAuth = [rest_test_utils:user_token_header(oct_background:get_user_access_token(user2))],

    test_utils:mock_assert_num_calls(ParisNode, rtransfer_config, fetch, 6, 0),

    % first download should synchronize all blocks
    ?assertMatch(
        {ok, 200, _, _},
        rest_test_utils:request(ParisNode, DownloadRestPath, get, HeadersWithAuth, [])
    ),
    test_utils:mock_assert_num_calls(ParisNode, rtransfer_config, fetch, 6, FileBlocks),

    % the next one will not do anything as it is already present
    ?assertMatch(
        {ok, 200, _, _},
        rest_test_utils:request(ParisNode, DownloadRestPath, get, HeadersWithAuth, [])
    ),
    test_utils:mock_assert_num_calls(ParisNode, rtransfer_config, fetch, 6, FileBlocks).


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
-spec download_file_using_download_code(api_test_memory:mem_ref(), node(), FileDownloadUrl :: binary()) ->
    {ok, Content :: binary()} | {error, term()}.
download_file_using_download_code(MemRef, Node, FileDownloadUrl) ->
    check_download_result(MemRef, http_client:request(get, FileDownloadUrl, #{}, <<>>, get_download_opts(Node))).


%% @private
-spec download_file_using_download_code_with_resumes(api_test_memory:mem_ref(), node(), FileDownloadUrl :: binary()) ->
    {ok, Content :: binary()} | {error, term()}.
download_file_using_download_code_with_resumes(MemRef, Node, FileDownloadUrl) ->
    InitDownloadFun = fun(Begin) ->
        {ok, _Ref} = http_client:request_return_stream(get, FileDownloadUrl, build_range_header(Begin), <<>>, get_download_opts(Node))
    end,
    Self = self(),
    spawn(fun() -> InitDownloadFun(0), failing_download_client(Self) end),
    check_download_result(MemRef, async_download(#{}, InitDownloadFun)).

%% @private
-spec get_download_opts(node()) -> http_client:opts().
get_download_opts(Node) ->
    CaCerts = opw_test_rpc:get_cert_chain_ders(Node),
    [{ssl_options, [{cacerts, CaCerts}]}, {recv_timeout, infinity}].


%% @private
-spec check_download_result(api_test_memory:mem_ref(), http_client:response()) ->
    {ok, http_client:body()} | {error, term()}.
check_download_result(MemRef, {ok, ?HTTP_200_OK, RespHeaders, RespBody}) ->
    assert_suitable_csp_header(MemRef, RespHeaders),
    {ok, RespBody};
check_download_result(MemRef, {ok, ?HTTP_206_PARTIAL_CONTENT, RespHeaders, RespBody}) ->
    assert_suitable_csp_header(MemRef, RespHeaders),
    {ok, RespBody};
check_download_result(_MemRef, {ok, _RespCode, _RespHeaders, RespBody}) ->
    errors:from_json(maps:get(<<"error">>, json_utils:decode(RespBody)));
check_download_result(_MemRef, {error, _} = Error) ->
    Error.


%% @private
%% @doc see page_file_content_download:add_headers_regulating_frame_ancestors/2
-spec assert_suitable_csp_header(api_test_memory:mem_ref(), http_client:headers()) -> term().
assert_suitable_csp_header(MemRef, RespHeaders) ->
    case api_test_memory:get(MemRef, scope) of
        public ->
            ?assertNot(maps:is_key(?HDR_CONTENT_SECURITY_POLICY, RespHeaders));
        _ ->
            ExpCspHeaderValue = <<"frame-ancestors https://", (ozw_test_rpc:get_domain())/binary>>,
            ?assertMatch(#{?HDR_CONTENT_SECURITY_POLICY := ExpCspHeaderValue}, RespHeaders)
    end.


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
            % In the async mode, hackney returns headers as proplists (like in the sync mode, but
            % we convert them to maps in the http_client wrapper).
            async_download(ResultMap#{headers => maps:from_list(Headers)}, DownloadFun);
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
    ?assertEqual({error, enoent}, file:list_dir(filename:join(CurrentPath, Filename))).


%% @private
-spec check_symlink(api_test_memory:mem_ref(), file_meta:path(), onenv_file_test_utils:object_spec(), public | private) -> ok.
check_symlink(MemRef, CurrentPath, Object, private) ->
    #object{name = Filename, symlink_value = SymlinkValue} = Object,
    % NOTE: custom_label in SymlinkValue is used only for internal symlinks
    case {api_test_memory:get(MemRef, follow_symlinks), SymlinkValue} of
        {true, {custom_label, Content}} ->
            ?assertEqual({ok, Content}, file:read_file(filename:join(CurrentPath, Filename)));
        {true, _} ->
            % symlink target file has its name as content
            ?assertEqual({ok, filename:basename(SymlinkValue)}, file:read_file(filename:join(CurrentPath, Filename)));
        {false, {custom_label, Content}} ->
            % internal symlinks are modified to target files in tarball even with follow_symlinks = false
            % check that symlink was not resolved
            ?assertMatch({ok, _}, file:read_link(filename:join(CurrentPath, Filename))),
            % symlink targets file that is also in the downloaded tarball, so it should be resolved when reading
            ?assertEqual({ok, Content}, file:read_file(filename:join(CurrentPath, Filename)));
        {false, _} ->
            ?assertEqual({ok, binary_to_list(SymlinkValue)}, file:read_link(filename:join(CurrentPath, Filename)))
    end;
check_symlink(_MemRef, CurrentPath, #object{name = Filename, symlink_value = SymlinkValue}, public) ->
    case SymlinkValue of
        {custom_label, Content} ->
            % check that symlink was not resolved
            ?assertMatch({ok, _}, file:read_link(filename:join(CurrentPath, Filename))),
            % symlink targets file that is also in the downloaded tarball, so it should be resolved when reading
            ?assertEqual({ok, Content}, file:read_file(filename:join(CurrentPath, Filename)));
        _ ->
            % link targets outside share scope - it should not be downloaded
            ?assertEqual({error, enoent}, file:read_file(filename:join(CurrentPath, Filename)))
    end.


%% @private
-spec check_content_disposition_header(api_test_memory:mem_ref(), http_client:headers()) ->
    ok.
check_content_disposition_header(MemRef, Headers) ->
    ExpectedDownloadedFileName = api_test_memory:get(MemRef, expected_downloaded_file_name),
    ?assert(maps:is_key(?HDR_CONTENT_DISPOSITION, Headers)),
    ExpHeader = <<"attachment; filename=\"", ExpectedDownloadedFileName/binary, "\"">>,
    ?assertEqual(ExpHeader, maps:get(?HDR_CONTENT_DISPOSITION, Headers)).


%% @private
-spec unpack_tarball(binary()) -> binary().
unpack_tarball(Bytes) ->
    % use os:cmd instead of erl_tar:extract, as the later does not allow for relative symlinks in tarball
    TmpDir = mochitemp:mkdtemp(),
    Name = ?RANDOM_FILE_NAME(),
    Path = <<(list_to_binary(TmpDir))/binary, "/", Name/binary>>,
    ok = file:write_file(Path, Bytes),
    Cmd = "cd " ++ TmpDir ++ " && tar -xvf " ++ binary_to_list(Name),
    Res = os:cmd(Cmd),
    ct:print("~ts", [Res]),
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
make_symlink_target(SpaceId, #object{name = Name}) ->
    make_symlink_target(SpaceId, <<"">>, Name).


%% @private
-spec make_symlink_target(od_space:id(), file_meta:path(), file_meta:name()) ->
    file_meta_symlinks:symlink().
make_symlink_target(SpaceId, ParentPath, Name) ->
    SpaceIdSymlinkPrefix = ?SYMLINK_SPACE_ID_ABS_PATH_PREFIX(SpaceId),
    filename:join([SpaceIdSymlinkPrefix, ParentPath, Name]).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    oct_background:init_per_suite([{?LOAD_MODULES, [dir_stats_test_utils]} | Config], #onenv_test_config{
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
            dir_stats_test_utils:disable_stats_counting(NewConfig),
            User3Id = oct_background:get_user_id(user3),
            lists:foreach(fun(SpaceId) ->
                ozw_test_rpc:space_set_user_privileges(SpaceId, User3Id, [
                    ?SPACE_MANAGE_SHARES | privileges:space_member()
                ])
            end, oct_background:get_provider_supported_spaces(krakow)),

            ProviderNodes = oct_background:get_all_providers_nodes(),
            lists:foreach(fun(OpNode) ->
                test_node_starter:load_modules([OpNode], [?MODULE]),
                ok = test_utils:mock_new(OpNode, file_content_download_utils),
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
                ok = test_utils:mock_expect(OpNode, file_content_download_utils, download_single_file,
                    fun(SessionId, FileAttrs, Callback, Req) ->
                        case ErrorFun(FileAttrs, Req) of
                            passthrough -> meck:passthrough([SessionId, FileAttrs, Callback, Req]);
                            Res -> Res
                        end
                    end),
                ok = test_utils:mock_expect(OpNode, file_content_download_utils, download_tarball,
                    fun(Id, SessionId, FileAttrs, TarballName, FollowSymlinks, Req) ->
                        case ErrorFun(FileAttrs, Req) of
                            passthrough ->
                                meck:passthrough([Id, SessionId, FileAttrs, TarballName, FollowSymlinks, Req]);
                            Res -> Res
                        end
                    end)
            end, ProviderNodes),
            NewConfig
        end
    }).


end_per_suite(Config) ->
    Nodes = oct_background:get_all_providers_nodes(),
    test_utils:mock_unload(Nodes),
    oct_background:end_per_suite(),
    dir_stats_test_utils:enable_stats_counting(Config).


init_per_group(_Group, Config) ->
    lfm_proxy:init(Config, false).


end_per_group(_Group, Config) ->
    lfm_proxy:teardown(Config).


init_per_testcase(gui_download_file_test = Case, Config) ->
    Providers = ?config(op_worker_nodes, Config),
    % TODO VFS-6828 - call needed to preload file_middleware module and add 'download_url' atom
    % to known/existing atoms. Otherwise gs_ws_handler may fail to decode this request (gri)
    % if it is the first request made.
    utils:rpc_multicall(Providers, file_middleware_get_handler, assert_operation_supported, [download_url, private]),
    init_per_testcase(?DEFAULT_CASE(Case), Config);
init_per_testcase(sync_first_file_block_test = Case, Config) ->
    ProviderNodes = oct_background:get_all_providers_nodes(),
    lists:foreach(fun(OpNode) ->
        ok = test_utils:mock_new(OpNode, rtransfer_config, [passthrough])
    end, ProviderNodes),
    init_per_testcase(?DEFAULT_CASE(Case), Config);
init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 40}),
    Config.

end_per_testcase(sync_first_file_block_test = Case, Config) ->
    ok = test_utils:mock_unload(oct_background:get_all_providers_nodes(), rtransfer_config),
    end_per_testcase(?DEFAULT_CASE(Case), Config);
end_per_testcase(_Case, _Config) ->
    ok.
