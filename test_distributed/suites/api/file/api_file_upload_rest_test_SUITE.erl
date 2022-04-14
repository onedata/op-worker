%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020-2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests concerning file upload via REST API.
%%% @end
%%%-------------------------------------------------------------------
-module(api_file_upload_rest_test_SUITE).
-author("Bartosz Walkowicz").

-include("global_definitions.hrl").
-include("api_file_test_utils.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("onenv_test_utils.hrl").
-include("proto/oneclient/common_messages.hrl").
-include_lib("ctool/include/privileges.hrl").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    create_file_test/1,
    create_file_at_path_test/1,
    update_file_content_test/1,
    create_file_at_path_with_create_parents_in_parallel_test/1
]).

%% @TODO VFS-8976 - test with update_existing=true option
all() -> [
    create_file_test,
    create_file_at_path_test,
    update_file_content_test,
    create_file_at_path_with_create_parents_in_parallel_test
].


-define(ATTEMPTS, 30).
-define(WRITE_SIZE_BYTES, 300).


%%%===================================================================
%%% Test functions
%%%===================================================================


create_file_test(_Config) ->
    Providers = lists:flatten([
        oct_background:get_provider_nodes(krakow),
        oct_background:get_provider_nodes(paris)
    ]),
    SpaceOwnerId = oct_background:get_user_id(user2),
    User3Id = oct_background:get_user_id(user3),

    #object{
        guid = DirGuid,
        shares = [DirShareId],
        children = [#object{
            guid = FileGuid,
            name = UsedFileName
        }]
    } = onenv_file_test_utils:create_and_sync_file_tree(user3, space_krk_par, #dir_spec{
        mode = 8#704,
        shares = [#share_spec{}],
        % create a child file with full perms instead of default ones so that call to
        % create child of this file will fail on type check (?ENOTDIR) instead of perms
        % check (which is performed first)
        children = [#file_spec{mode = 8#777}]
    }),

    {ok, DirObjectId} = file_id:guid_to_objectid(DirGuid),
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),

    Content = crypto:strong_rand_bytes(?WRITE_SIZE_BYTES),

    MemRef = api_test_memory:init(),
    api_test_memory:set(MemRef, files, [FileGuid]),

    ?assert(onenv_api_test_runner:run_tests([
        #scenario_spec{
            name = <<"Upload file using rest endpoint">>,
            type = rest,
            target_nodes = Providers,
            client_spec = #client_spec{
                correct = case rand:uniform(2) of
                    1 -> [user2];  % space owner
                    2 -> [user3]   % files owner
                end,
                unauthorized = [nobody],
                forbidden_not_in_space = [user1],
                forbidden_in_space = [{user4, ?ERROR_POSIX(?EACCES)}]  % forbidden by file perms
            },

            prepare_args_fun = build_create_file_prepare_args_fun(MemRef, DirObjectId),
            validate_result_fun = build_create_file_validate_call_fun(MemRef, SpaceOwnerId),
            verify_fun = build_create_file_verify_fun(MemRef, DirGuid, Providers),

            data_spec = api_test_utils:add_file_id_errors_for_operations_not_available_in_share_mode(
                DirGuid, DirShareId, #data_spec{
                    required = [<<"name">>],
                    optional = [<<"type">>, <<"mode">>, <<"offset">>, body, <<"update_existing">>],
                    correct_values = #{
                        <<"name">> => [name_placeholder],
                        <<"type">> => [<<"REG">>, <<"DIR">>],
                        <<"mode">> => [<<"0544">>, <<"0707">>],
                        <<"offset">> => [
                            0,
                            ?WRITE_SIZE_BYTES,
                            ?WRITE_SIZE_BYTES * 1000000000 % > SUPPORT_SIZE
                        ],
                        body => [Content],
                        <<"update_existing">> => [false]
                    },
                    bad_values = [
                        {bad_id, FileObjectId, {rest, {error_fun, fun(#api_test_ctx{
                            client = ?USER(UserId),
                            data = Data
                        }) ->
                            case {UserId, maps:get(<<"type">>, Data, <<"REG">>)} of
                                {User3Id, <<"DIR">>} ->
                                    % User3 gets ?EACCES because operation fails on permissions
                                    % checks (file has 8#777 mode but this doesn't give anyone
                                    % ?add_subcontainer perm) rather than file type check which
                                    % is performed later
                                    ?ERROR_POSIX(?EACCES);
                                _ ->
                                    ?ERROR_POSIX(?ENOTDIR)
                            end
                        end}}},

                        {<<"name">>, UsedFileName, ?ERROR_POSIX(?EEXIST)},

                        {<<"type">>, <<"file">>, ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"type">>, [
                            <<"REG">>, <<"DIR">>, <<"LNK">>, <<"SYMLNK">>
                        ])},

                        {<<"mode">>, true, ?ERROR_BAD_VALUE_INTEGER(<<"mode">>)},
                        {<<"mode">>, <<"integer">>, ?ERROR_BAD_VALUE_INTEGER(<<"mode">>)},
                        {<<"mode">>, <<"0888">>, ?ERROR_BAD_VALUE_INTEGER(<<"mode">>)},
                        {<<"mode">>, <<"888">>, ?ERROR_BAD_VALUE_INTEGER(<<"mode">>)},
                        {<<"mode">>, <<"77777">>, ?ERROR_BAD_VALUE_NOT_IN_RANGE(<<"mode">>, 0, 8#1777)},

                        {<<"offset">>, <<"unicorns">>, ?ERROR_BAD_VALUE_INTEGER(<<"offset">>)},
                        {<<"offset">>, <<"-123">>, ?ERROR_BAD_VALUE_TOO_LOW(<<"offset">>, 0)},
                        {<<"update_existing">>, <<"asd">>, ?ERROR_BAD_VALUE_BOOLEAN(<<"update_existing">>)}
                    ]
                }
            )
        }
    ])).


%% @private
-spec build_create_file_prepare_args_fun(api_test_memory:mem_ref(), file_id:objectid()) ->
    onenv_api_test_runner:prepare_args_fun().
build_create_file_prepare_args_fun(MemRef, ParentDirObjectId) ->
    fun(#api_test_ctx{data = Data0}) ->
        {ParentId, Data1} = api_test_utils:maybe_substitute_bad_id(ParentDirObjectId, Data0),

        Data2 = case maps:get(<<"name">>, Data1, undefined) of
            name_placeholder ->
                Name = str_utils:rand_hex(10),
                api_test_memory:set(MemRef, name, Name),
                Data1#{<<"name">> => Name};
            _ ->
                Data1
        end,
        {Body, Data3} = utils:ensure_defined(maps:take(body, Data2), error, {<<>>, Data2}),

        #rest_args{
            method = post,
            path = http_utils:append_url_parameters(<<"data/", ParentId/binary, "/children">>, Data3),
            body = Body
        }
    end.


%% @private
-spec build_create_file_validate_call_fun(api_test_memory:mem_ref(), od_user:id()) ->
    onenv_api_test_runner:validate_call_result_fun().
build_create_file_validate_call_fun(MemRef, SpaceOwnerId) ->
    fun(#api_test_ctx{
        node = TestNode,
        client = #auth{subject = #subject{id = UserId}},
        data = Data
    }, {ok, RespCode, RespHeaders, RespBody}) ->
        DataSent = maps:get(body, Data, <<>>),
        Offset = maps:get(<<"offset">>, Data, 0),
        ShouldResultInWrite = Offset > 0 orelse byte_size(DataSent) > 0,

        Type = maps:get(<<"type">>, Data, <<"REG">>),
        Mode = maps:get(<<"mode">>, Data, undefined),

        case {Type, Mode, ShouldResultInWrite, UserId == SpaceOwnerId} of
            {<<"REG">>, <<"0544">>, true, false} ->
                % It is possible to create file but setting perms forbidding write access
                % and uploading some data at the same time should result in error for any
                % user not being space owner
                ?assertEqual(?HTTP_400_BAD_REQUEST, RespCode),
                ?assertEqual(?REST_ERROR(?ERROR_POSIX(?EACCES)), RespBody),
                api_test_memory:set(MemRef, success, false);
            _ ->
                ?assertEqual(?HTTP_201_CREATED, RespCode),

                #{<<"fileId">> := FileObjectId} = ?assertMatch(#{<<"fileId">> := <<_/binary>>}, RespBody),

                ExpLocation = api_test_utils:build_rest_url(TestNode, [<<"data">>, FileObjectId]),
                ?assertEqual(ExpLocation, maps:get(?HDR_LOCATION, RespHeaders)),

                {ok, FileGuid} = file_id:objectid_to_guid(FileObjectId),

                api_test_memory:set(MemRef, file_guid, FileGuid),
                api_test_memory:set(MemRef, success, true)
        end
    end.


%% @private
-spec build_create_file_verify_fun(api_test_memory:mem_ref(), file_id:file_guid(), [node()]) ->
    boolean().
build_create_file_verify_fun(MemRef, DirGuid, Providers) ->
    fun
        (expected_failure, #api_test_ctx{node = TestNode}) ->
            ExpFilesInDir = api_test_memory:get(MemRef, files),
            ?assertEqual(ExpFilesInDir, ls(TestNode, DirGuid), ?ATTEMPTS);
        (expected_success, #api_test_ctx{node = TestNode, data = Data}) ->
            case api_test_memory:get(MemRef, success) of
                true ->
                    FileGuid = api_test_memory:get(MemRef, file_guid),
                    OtherFilesInDir = api_test_memory:get(MemRef, files),
                    AllFilesInDir = lists:sort([FileGuid | OtherFilesInDir]),

                    ?assertEqual(AllFilesInDir, ls(TestNode, DirGuid), ?ATTEMPTS),
                    api_test_memory:set(MemRef, files, AllFilesInDir),

                    ExpName = api_test_memory:get(MemRef, name),
                    {ExpType, DefaultMode} = case maps:get(<<"type">>, Data, <<"REG">>) of
                        <<"REG">> -> {?REGULAR_FILE_TYPE, ?DEFAULT_FILE_PERMS};
                        <<"DIR">> -> {?DIRECTORY_TYPE, ?DEFAULT_DIR_PERMS}
                    end,
                    ExpMode = case maps:get(<<"mode">>, Data, undefined) of
                        undefined -> DefaultMode;
                        ModeBin -> binary_to_integer(ModeBin, 8)
                    end,

                    lists:foreach(fun(Provider) ->
                        ?assertMatch(
                            {ok, #file_attr{name = ExpName, type = ExpType, mode = ExpMode}},
                            file_test_utils:get_attrs(Provider, FileGuid),
                            ?ATTEMPTS
                        )
                    end, Providers),

                    case ExpType of
                        ?REGULAR_FILE_TYPE ->
                            verify_file_content_update(
                                FileGuid, TestNode, TestNode, Providers, <<>>,
                                maps:get(<<"offset">>, Data, undefined), maps:get(body, Data, <<>>)
                            );
                        ?DIRECTORY_TYPE ->
                            true
                    end;
                false ->
                    ExpFilesInDir = api_test_memory:get(MemRef, files),
                    ?assertEqual(ExpFilesInDir, ls(TestNode, DirGuid))
            end
    end.


create_file_at_path_test(_Config) ->
    MemRef = api_test_memory:init(),
    Providers = lists:flatten([
        oct_background:get_provider_nodes(krakow),
        oct_background:get_provider_nodes(paris)
    ]),
    SpaceOwnerId = oct_background:get_user_id(user2),

    #object{
        guid = DirGuid,
        shares = [DirShareId],
        children = [
            #object{
                guid = ChildFileGuid,
                name = ChildFileName,
                type = ?REGULAR_FILE_TYPE
            },
            #object{
                name = ChildDirName,
                type = ?DIRECTORY_TYPE
            }]
    } = onenv_file_test_utils:create_and_sync_file_tree(user3, space_krk_par, #dir_spec{
        mode = 8#704,
        shares = [#share_spec{}],
        % create a child file with full perms instead of default ones so that call to
        % create child of this file will fail on type check (?ENOTDIR) instead of perms
        % check (which is performed first)
        children = [
            #file_spec{
                mode = 8#777
            },
            #dir_spec{
                mode = 8#704,
                shares = [#share_spec{}]
            }
        ]
    }),

    {ok, ChildFileObjectId} = file_id:guid_to_objectid(ChildFileGuid),

    api_test_memory:set(MemRef, child_dir_name, ChildDirName),

    Content = crypto:strong_rand_bytes(?WRITE_SIZE_BYTES),

    ?assert(onenv_api_test_runner:run_tests([
        #scenario_spec{
            name = <<"Upload file at path using rest endpoint">>,
            type = rest,
            target_nodes = Providers,
            client_spec = #client_spec{
                correct = case rand:uniform(2) of
                    1 -> [user2];  % space owner
                    2 -> [user3]   % files owner
                end,
                unauthorized = [nobody],
                forbidden_not_in_space = [user1],
                forbidden_in_space = [{user4, ?ERROR_POSIX(?EACCES)}]  % forbidden by file perms
            },

            prepare_args_fun = build_rest_create_file_at_path_prepare_args_fun(MemRef, DirGuid),
            validate_result_fun = build_create_file_validate_call_fun(MemRef, SpaceOwnerId),
            verify_fun = build_rest_create_file_at_path_verify_fun(MemRef, Providers),

            data_spec = api_test_utils:add_file_id_errors_for_operations_not_available_in_share_mode(
                DirGuid, DirShareId, #data_spec{
                    required = [<<"path">>],
                    optional = [<<"type">>, <<"mode">>, <<"offset">>, body, <<"update_existing">>],
                    correct_values = #{
                        <<"path">> => [
                            filename_only_without_create_parents_flag_placeholder,
                            filename_only_with_create_parents_flag_placeholder,
                            existent_path_without_create_parents_flag_placeholder,
                            existent_path_with_create_parents_flag_placeholder,
                            nonexistent_path_with_create_parents_flag_placeholder
                        ],
                        <<"type">> => [<<"REG">>, <<"DIR">>],
                        <<"mode">> => [<<"0544">>, <<"0707">>],
                        <<"offset">> => [
                            0,
                            ?WRITE_SIZE_BYTES,
                            ?WRITE_SIZE_BYTES * 1000000000 % > SUPPORT_SIZE
                        ],
                        body => [Content],
                        <<"update_existing">> => [false]
                    },

                    bad_values = [
                        {bad_id, ChildFileObjectId, ?ERROR_POSIX(?ENOTDIR)},
                        {<<"path">>, filepath_utils:join([ChildFileName, <<"dir1/file.txt">>]), ?ERROR_POSIX(?ENOTDIR)},
                        {<<"path">>, <<"/a/b/\0null\0/">>, ?ERROR_BAD_VALUE_FILE_PATH},
                        {<<"path">>, nonexistent_path_without_create_parents_flag_placeholder, ?ERROR_POSIX(?ENOENT)},

                        {<<"type">>, <<"file">>, ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"type">>, [
                            <<"REG">>, <<"DIR">>, <<"LNK">>, <<"SYMLNK">>
                        ])},

                        {<<"mode">>, true, ?ERROR_BAD_VALUE_INTEGER(<<"mode">>)},
                        {<<"mode">>, <<"integer">>, ?ERROR_BAD_VALUE_INTEGER(<<"mode">>)},
                        {<<"mode">>, <<"0888">>, ?ERROR_BAD_VALUE_INTEGER(<<"mode">>)},
                        {<<"mode">>, <<"888">>, ?ERROR_BAD_VALUE_INTEGER(<<"mode">>)},
                        {<<"mode">>, <<"77777">>, ?ERROR_BAD_VALUE_NOT_IN_RANGE(<<"mode">>, 0, 8#1777)},

                        {<<"offset">>, <<"unicorns">>, ?ERROR_BAD_VALUE_INTEGER(<<"offset">>)},
                        {<<"offset">>, <<"-123">>, ?ERROR_BAD_VALUE_TOO_LOW(<<"offset">>, 0)},
                        {<<"update_existing">>, <<"asd">>, ?ERROR_BAD_VALUE_BOOLEAN(<<"update_existing">>)}
                    ]
                }
            )
        }
    ])).


%% @private
-spec build_rest_create_file_at_path_prepare_args_fun(api_test_memory:mem_ref(), file_id:file_guid()) ->
    onenv_api_test_runner:prepare_args_fun().
build_rest_create_file_at_path_prepare_args_fun(MemRef, RelRootDirGuid) ->
    fun(#api_test_ctx{data = Data0, node = TestNode}) ->
        {ok, RelRootDirObjectId} = file_id:guid_to_objectid(RelRootDirGuid),
        {ParentId, Data1} = api_test_utils:maybe_substitute_bad_id(RelRootDirObjectId, Data0),
        ChildDirName = api_test_memory:get(MemRef, child_dir_name),
        Name = str_utils:rand_hex(10),
        api_test_memory:set(MemRef, name, Name),
        RandomDirPath = generate_random_dir_path(),
        DataWithoutPath = maps:remove(<<"path">>, Data1),

        {RelativePath, Data2} = case maps:get(<<"path">>, Data1, undefined) of
            filename_only_without_create_parents_flag_placeholder ->
                {Name, DataWithoutPath};
            filename_only_with_create_parents_flag_placeholder ->
                {Name, DataWithoutPath#{<<"create_parents">> => true}};
            existent_path_without_create_parents_flag_placeholder ->
                Path = filepath_utils:join([ChildDirName, Name]),
                {Path, DataWithoutPath};
            existent_path_with_create_parents_flag_placeholder ->
                Path = filepath_utils:join([ChildDirName, Name]),
                {Path, DataWithoutPath#{<<"create_parents">> => true}};
            nonexistent_path_without_create_parents_flag_placeholder ->
                Path = filepath_utils:join([ChildDirName, RandomDirPath, Name]),
                {Path, DataWithoutPath};
            nonexistent_path_with_create_parents_flag_placeholder ->
                Path = filepath_utils:join([ChildDirName, RandomDirPath, Name]),
                {Path, DataWithoutPath#{<<"create_parents">> => true}};
            undefined ->
                {<<"">>, DataWithoutPath};
            CustomPath ->
                Path = filepath_utils:join([CustomPath, Name]),
                {Path, DataWithoutPath}
        end,

        {Body, Data3} = utils:ensure_defined(maps:take(body, Data2), error, {<<>>, Data2}),
        RestPath = str_utils:join_as_binaries([<<"data">>, ParentId, <<"path">>, RelativePath], <<"/">>),
        {ok, RelRootPath} = lfm_proxy:get_file_path(TestNode, ?ROOT_SESS_ID, RelRootDirGuid),
        CanonicalFilePath = filename:join([RelRootPath, RelativePath]),
        api_test_memory:set(MemRef, file_path, CanonicalFilePath),
        #rest_args{
            method = put,
            path = http_utils:append_url_parameters(RestPath, Data3),
            body = Body
        }
    end.


%% @private
-spec build_rest_create_file_at_path_verify_fun(api_test_memory:mem_ref(), [node()]) ->
    boolean().
build_rest_create_file_at_path_verify_fun(MemRef, Providers) ->
    fun
        (expected_failure, #api_test_ctx{node = TestNode}) ->
            FilePath = api_test_memory:get(MemRef, file_path),
            ?assertMatch({error, _}, lfm_proxy:stat(TestNode, ?ROOT_SESS_ID, {path, FilePath}));
        (expected_success, #api_test_ctx{node = TestNode, data = Data}) ->
            case api_test_memory:get(MemRef, success) of
                true ->
                    FileGuid = api_test_memory:get(MemRef, file_guid),
                    ExpPath = api_test_memory:get(MemRef, file_path),
                    {ok, FilePath} = ?assertMatch({ok, _}, lfm_proxy:get_file_path(
                        TestNode, ?ROOT_SESS_ID, FileGuid), ?ATTEMPTS),

                    ?assertEqual(ExpPath, FilePath, ?ATTEMPTS),

                    ExpName = api_test_memory:get(MemRef, name),
                    {ExpType, DefaultMode} = case maps:get(<<"type">>, Data, <<"REG">>) of
                        <<"REG">> -> {?REGULAR_FILE_TYPE, ?DEFAULT_FILE_PERMS};
                        <<"DIR">> -> {?DIRECTORY_TYPE, ?DEFAULT_DIR_PERMS}
                    end,
                    ExpMode = case maps:get(<<"mode">>, Data, undefined) of
                        undefined -> DefaultMode;
                        ModeBin -> binary_to_integer(ModeBin, 8)
                    end,

                    lists:foreach(fun(Provider) ->
                        ?assertMatch(
                            {ok, #file_attr{name = ExpName, type = ExpType, mode = ExpMode}},
                            file_test_utils:get_attrs(Provider, FileGuid),
                            ?ATTEMPTS
                        )
                    end, Providers),

                    case ExpType of
                        ?REGULAR_FILE_TYPE ->
                            verify_file_content_update(
                                FileGuid, TestNode, TestNode, Providers, <<>>,
                                maps:get(<<"offset">>, Data, undefined), maps:get(body, Data, <<>>)
                            );
                        ?DIRECTORY_TYPE ->
                            true
                    end;

                false ->
                    FilePath = api_test_memory:get(MemRef, file_path),
                    ?assertMatch({error, _}, lfm_proxy:stat(TestNode, ?ROOT_SESS_ID, {path, FilePath}))
            end
    end.


%% @private
-spec ls(node(), file_id:file_guid()) -> [file_id:file_guid()] | {error, term()}.
ls(Node, DirGuid) ->
    case lfm_proxy:get_children(Node, ?ROOT_SESS_ID, ?FILE_REF(DirGuid), 0, 10000) of
        {ok, Children} ->
            lists:sort(lists:map(fun({Guid, _Name}) -> Guid end, Children));
        {error, _} = Error ->
            Error
    end.


create_file_at_path_with_create_parents_in_parallel_test(_Config) ->
    Filename = generator:gen_name(),
    RelativePath = filename:join([Filename, Filename, Filename]),
    SpaceId = oct_background:get_space_id(space_krk_par),
    {ok, SpaceObjectId} = file_id:guid_to_objectid(fslogic_uuid:spaceid_to_space_dir_guid(SpaceId)),
    RestPath = str_utils:join_as_binaries([<<"data">>, SpaceObjectId, <<"path">>, RelativePath], <<"/">>),
    PortStr = case opw_test_rpc:get_env(krakow, https_server_port) of
        443 -> "";
        Port -> ":" ++ integer_to_list(Port)
    end,
    Domain = opw_test_rpc:get_provider_domain(krakow),
    
    RestPath2 = http_utils:append_url_parameters(RestPath, #{<<"create_parents">> => true, <<"update_existing">> => true}),
    
    RestApiRoot = str_utils:format_bin("https://~s~s/api/v3/oneprovider/", [Domain, PortStr]),
    URL = str_utils:join_as_binaries([RestApiRoot, RestPath2], <<>>),
    HeadersWithAuth = #{?HDR_X_AUTH_TOKEN => oct_background:get_user_access_token(user2)},
    Opts = [
        {ssl_options, [
            {cacerts, opw_test_rpc:get_cert_chain_ders(krakow)}
        ]}
    ],
    
    lists_utils:pforeach(fun(_) ->
        ?assertMatch({ok, 201, _, _}, http_client:request(put, URL, HeadersWithAuth, <<>>, Opts))
    end, lists:seq(1, 100)).


update_file_content_test(_Config) ->
    Providers = lists:flatten([
        oct_background:get_provider_nodes(krakow),
        oct_background:get_provider_nodes(paris)
    ]),

    #object{guid = DirGuid, shares = [DirShareId]} = onenv_file_test_utils:create_and_sync_file_tree(
        user3, space_krk_par, #dir_spec{
            mode = 8#704,
            shares = [#share_spec{}],
            children = [#file_spec{}]
        }
    ),
    {ok, DirObjectId} = file_id:guid_to_objectid(DirGuid),

    OriginalFileSize = ?WRITE_SIZE_BYTES,
    OriginalFileContent = crypto:strong_rand_bytes(OriginalFileSize),

    UpdateSize = 100,
    UpdateData = crypto:strong_rand_bytes(UpdateSize),

    MemRef = api_test_memory:init(),

    ?assert(onenv_api_test_runner:run_tests([
        #scenario_spec{
            name = <<"Update file content using rest endpoint">>,
            type = rest,
            target_nodes = Providers,
            client_spec = #client_spec{
                correct = case rand:uniform(2) of
                    1 -> [user2];  % space owner - doesn't need any perms
                    2 -> [user3]   % files owner
                end,
                unauthorized = [nobody],
                forbidden_not_in_space = [user1],
                forbidden_in_space = [{user4, ?ERROR_POSIX(?EACCES)}]  % forbidden by file perms
            },

            setup_fun = build_update_file_content_setup_fun(MemRef, OriginalFileContent),
            prepare_args_fun = build_update_file_content_prepare_args_fun(MemRef),
            validate_result_fun = build_update_file_content_validate_call_fun(),
            verify_fun = build_update_file_content_verify_fun(MemRef, OriginalFileContent),

            data_spec = api_test_utils:add_file_id_errors_for_operations_not_available_in_share_mode(
                DirGuid, DirShareId, #data_spec{
                    optional = [body, <<"offset">>],
                    correct_values = #{
                        body => [UpdateData],
                        <<"offset">> => [
                            0,
                            OriginalFileSize div 2,
                            OriginalFileSize - UpdateSize div 2,
                            OriginalFileSize,
                            OriginalFileSize * 4,
                            OriginalFileSize * 1000000000 % > SUPPORT_SIZE
                        ]
                    },
                    bad_values = [
                        {bad_id, DirObjectId, {rest, ?ERROR_POSIX(?EISDIR)}},

                        {<<"offset">>, <<"unicorns">>, ?ERROR_BAD_VALUE_INTEGER(<<"offset">>)},
                        {<<"offset">>, <<"-123">>, ?ERROR_BAD_VALUE_TOO_LOW(<<"offset">>, 0)}
                    ]
                }
            )
        }
    ])).


%% @private
-spec build_update_file_content_setup_fun(
    api_test_memory:mem_ref(),
    FileContent :: binary()
) ->
    onenv_api_test_runner:setup_fun().
build_update_file_content_setup_fun(MemRef, Content) ->
    UserSessIdP1 = oct_background:get_user_session_id(user3, krakow),
    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    Providers = [P1Node, P2Node],

    FileSize = size(Content),

    fun() ->
        FilePath = filename:join(["/", ?SPACE_KRK_PAR, ?RANDOM_FILE_NAME()]),
        {ok, FileGuid} = lfm_test_utils:create_file(<<"file">>, P1Node, UserSessIdP1, FilePath, 8#704),

        lfm_test_utils:write_file(P1Node, UserSessIdP1, FileGuid, Content),
        file_test_utils:await_size(P2Node, FileGuid, FileSize),
        file_test_utils:await_distribution(Providers, FileGuid, [{P1Node, FileSize}]),

        api_test_memory:set(MemRef, file_guid, FileGuid)
    end.


%% @private
-spec build_update_file_content_prepare_args_fun(api_test_memory:mem_ref()) ->
    onenv_api_test_runner:prepare_args_fun().
build_update_file_content_prepare_args_fun(MemRef) ->
    fun(#api_test_ctx{data = Data0}) ->
        FileGuid = api_test_memory:get(MemRef, file_guid),
        {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),
        {Id, Data1} = api_test_utils:maybe_substitute_bad_id(FileObjectId, Data0),

        #rest_args{
            method = put,
            path = http_utils:append_url_parameters(
                <<"data/", Id/binary, "/content">>,
                maps:with([<<"offset">>], Data1)
            ),
            body = maps:get(body, Data1, <<>>)
        }
    end.


%% @private
-spec build_update_file_content_validate_call_fun() ->
    onenv_api_test_runner:validate_call_result_fun().
build_update_file_content_validate_call_fun() ->
    fun(#api_test_ctx{}, {ok, RespCode, _RespHeaders, _RespBody}) ->
        ?assertEqual(?HTTP_204_NO_CONTENT, RespCode)
    end.


%% @private
-spec build_update_file_content_verify_fun(
    api_test_memory:mem_ref(),
    OriginalFileContent :: binary()
) ->
    boolean().
build_update_file_content_verify_fun(MemRef, OriginalFileContent) ->
    [CreationNode = P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    AllProviders = [P1Node, P2Node],

    OriginalFileSize = byte_size(OriginalFileContent),

    fun
        (expected_failure, _) ->
            FileGuid = api_test_memory:get(MemRef, file_guid),
            file_test_utils:await_distribution(AllProviders, FileGuid, [{P1Node, OriginalFileSize}]);
        (expected_success, #api_test_ctx{node = UpdateNode, data = Data}) ->
            FileGuid = api_test_memory:get(MemRef, file_guid),
            Offset = maps:get(<<"offset">>, Data, undefined),
            DataSent = maps:get(body, Data, <<>>),

            verify_file_content_update(
                FileGuid, CreationNode, UpdateNode, AllProviders,
                OriginalFileContent, Offset, DataSent
            )
    end.


%% @private
-spec verify_file_content_update(
    file_id:file_guid(),
    CreationNode :: node(),
    UpdateNode :: node(),
    AllProviders :: [node()],
    OriginalContent :: binary(),
    Offset :: non_neg_integer(),
    DataSent :: binary()
) ->
    true.
verify_file_content_update(
    FileGuid, CreationNode, UpdateNode, AllProviders,
    OriginalContent, Offset, DataSent
) ->
    OriginalFileSize = byte_size(OriginalContent),
    DataSentSize = byte_size(DataSent),

    case Offset of
        undefined ->
            % File was truncated and new data written at offset 0
            file_test_utils:await_size(AllProviders, FileGuid, DataSentSize),

            ExpDist = case UpdateNode == CreationNode of
                true ->
                    [{UpdateNode, DataSentSize}];
                false ->
                    [{CreationNode, 0}, {UpdateNode, DataSentSize}]
            end,
            file_test_utils:await_distribution(AllProviders, FileGuid, ExpDist),

            file_test_utils:await_content(AllProviders, FileGuid, DataSent);
        Offset ->
            ExpSize = max(OriginalFileSize, Offset + DataSentSize),
            file_test_utils:await_size(AllProviders, FileGuid, ExpSize),

            ExpDist = case UpdateNode == CreationNode of
                true ->
                    case Offset =< OriginalFileSize of
                        true ->
                            [{CreationNode, max(OriginalFileSize, Offset + DataSentSize)}];
                        false ->
                            [{CreationNode, fslogic_blocks:consolidate([
                                #file_block{offset = 0, size = OriginalFileSize},
                                #file_block{offset = Offset, size = DataSentSize}
                            ])}]
                    end;
                false ->
                    case Offset + DataSentSize < OriginalFileSize of
                        true ->
                            [
                                {CreationNode, fslogic_blocks:consolidate([
                                    #file_block{offset = 0, size = Offset},
                                    #file_block{
                                        offset = Offset + DataSentSize,
                                        size = OriginalFileSize - Offset - DataSentSize
                                    }
                                ])},
                                {UpdateNode, [#file_block{offset = Offset, size = DataSentSize}]}
                            ];
                        false ->
                            [
                                {CreationNode, [#file_block{offset = 0, size = min(Offset, OriginalFileSize)}]},
                                {UpdateNode, [#file_block{offset = Offset, size = DataSentSize}]}
                            ]
                    end
            end,
            file_test_utils:await_distribution(AllProviders, FileGuid, ExpDist),

            case Offset > 1024 * 1024 * 1024 of  % 1 GB
                true ->
                    % In case of too big files to verify entire content (it will not fit into memory
                    % and reading it chunk by chunk will take too much time as we are speaking about
                    % PB of data) assert only the last fragment
                    ExpContent = str_utils:join_binary([
                        <<<<"\0">> || _ <- lists:seq(1, 50)>>,
                        DataSent
                    ]),
                    file_test_utils:await_content(AllProviders, FileGuid, ExpContent, Offset - 50);
                false ->
                    ExpContent = case Offset =< OriginalFileSize of
                        true ->
                            str_utils:join_binary([
                                slice_binary(OriginalContent, 0, Offset),
                                DataSent,
                                slice_binary(OriginalContent, Offset + DataSentSize)
                            ]);
                        false ->
                            str_utils:join_binary([
                                OriginalContent,
                                <<<<"\0">> || _ <- lists:seq(OriginalFileSize, Offset - 1)>>,
                                DataSent
                            ])
                    end,
                    file_test_utils:await_content(AllProviders, FileGuid, ExpContent)
            end
    end,
    true.


%% @private
-spec slice_binary(binary(), Offset :: non_neg_integer()) -> binary().
slice_binary(Bin, Offset) ->
    slice_binary(Bin, Offset, byte_size(Bin) - Offset).


%% @private
-spec slice_binary(binary(), Offset :: non_neg_integer(), Len :: non_neg_integer()) -> binary().
slice_binary(Bin, Offset, _Len) when Offset >= byte_size(Bin) ->
    <<>>;
slice_binary(Bin, Offset, Len) ->
    binary:part(Bin, Offset, min(Len, byte_size(Bin) - Offset)).


%% @private
-spec generate_random_dir_path() -> file_meta:path().
generate_random_dir_path() ->
    PathLength = rand:uniform(10),
    DirNameLength = 10,
    Tokens = lists:map(fun(_Item) ->
        str_utils:rand_hex(DirNameLength)
    end, lists:seq(1, PathLength)),
    filepath_utils:join(Tokens).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    oct_background:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "api_tests",
        envs = [{op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}],
        posthook = fun(NewConfig) ->
            User3Id = oct_background:get_user_id(user3),
            SpaceId = oct_background:get_space_id(space_krk_par),
            ozw_test_rpc:space_set_user_privileges(SpaceId, User3Id, [
                ?SPACE_MANAGE_SHARES | privileges:space_member()
            ]),
            NewConfig
        end
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 20}),
    lfm_proxy:init(Config).


end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).
