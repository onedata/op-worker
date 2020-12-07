%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests concerning file upload API (REST + gs).
%%% @end
%%%-------------------------------------------------------------------
-module(api_file_upload_test_SUITE).
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
    rest_create_file_test/1,
    rest_update_file_content_test/1,

    gui_registering_upload_for_directory_should_fail/1,
    gui_registering_upload_for_non_empty_file_should_fail/1,
    gui_registering_upload_for_not_owned_file_should_fail/1,
    gui_not_registered_upload_should_fail/1,
    gui_upload_test/1,
    gui_stale_upload_file_should_be_deleted/1,
    gui_finished_upload_file_should_be_left_intact/1,
    gui_upload_with_time_warps_test/1
]).

all() -> [
    rest_create_file_test,
    rest_update_file_content_test,

    gui_registering_upload_for_directory_should_fail,
    gui_registering_upload_for_non_empty_file_should_fail,
    gui_registering_upload_for_not_owned_file_should_fail,
    gui_not_registered_upload_should_fail,
    gui_upload_test,
    gui_stale_upload_file_should_be_deleted,
    gui_finished_upload_file_should_be_left_intact,
    gui_upload_with_time_warps_test
].


-define(SPACE_ID, <<"space2">>).
-define(FILE_PATH, <<"/", ?SPACE_ID/binary, "/", (str_utils:rand_hex(12))/binary>>).

-define(ATTEMPTS, 30).


%%%===================================================================
%%% REST File upload test functions
%%%===================================================================


rest_create_file_test(Config) ->
    [P1Node] = api_test_env:get_provider_nodes(p1, Config),
    [P2Node] = api_test_env:get_provider_nodes(p2, Config),
    Providers = [P1Node, P2Node],

    SpaceOwnerId = api_test_env:get_user_id(user2, Config),
    UserSessIdP1 = api_test_env:get_user_session_id(user3, p1, Config),

    {_, DirPath, DirGuid, DirShareId} = api_test_utils:create_and_sync_shared_file_in_space2(
        <<"dir">>, 8#704, Config
    ),
    {ok, DirObjectId} = file_id:guid_to_objectid(DirGuid),

    UsedFileName = ?RANDOM_FILE_NAME(),
    FilePath = filename:join([DirPath, UsedFileName]),
    {ok, FileGuid} = api_test_utils:create_file(<<"file">>, P1Node, UserSessIdP1, FilePath, 8#777),
    file_test_utils:wait_for_sync(P2Node, FileGuid),

    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),

    WriteSize = 300,
    Content = crypto:strong_rand_bytes(WriteSize),

    MemRef = api_test_memory:init(),
    api_test_memory:set(MemRef, files, [FileGuid]),

    ?assert(onenv_api_test_runner:run_tests(Config, [
        #scenario_spec{
            name = <<"Upload file using rest endpoint">>,
            type = rest,
            target_nodes = Providers,
            client_spec = #client_spec{
                correct = lists_utils:random_sublist([
                    user2,  % space owner - doesn't need any perms
                    user3  % files owner (see fun create_shared_file/1)
                ], 1, 1),
                unauthorized = [nobody],
                forbidden_not_in_space = [user1],
                forbidden_in_space = [{user4, ?ERROR_POSIX(?EACCES)}]  % forbidden by file perms
            },

            prepare_args_fun = build_rest_create_file_prepare_args_fun(MemRef, DirObjectId),
            validate_result_fun = build_rest_create_file_validate_call_fun(MemRef, SpaceOwnerId),
            verify_fun = build_rest_create_file_verify_fun(MemRef, DirGuid, Providers),

            data_spec = api_test_utils:add_file_id_errors_for_operations_not_available_in_share_mode(
                DirGuid, DirShareId, #data_spec{
                    required = [<<"name">>],
                    optional = [<<"type">>, <<"mode">>, <<"offset">>, body],
                    correct_values = #{
                        <<"name">> => [name_placeholder],
                        <<"type">> => [<<"reg">>, <<"dir">>],
                        <<"mode">> => [<<"0544">>, <<"0707">>],
                        <<"offset">> => [
                            0,
                            WriteSize,
                            WriteSize * 1000000000 % > SUPPORT_SIZE
                        ],
                        body => [Content]
                    },
                    bad_values = [
                        {bad_id, FileObjectId, {rest, ?ERROR_POSIX(?ENOTDIR)}},

                        {<<"name">>, UsedFileName, ?ERROR_POSIX(?EEXIST)},

                        {<<"type">>, <<"file">>, ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"type">>, [<<"reg">>, <<"dir">>])},

                        {<<"mode">>, true, ?ERROR_BAD_VALUE_INTEGER(<<"mode">>)},
                        {<<"mode">>, <<"integer">>, ?ERROR_BAD_VALUE_INTEGER(<<"mode">>)},
                        {<<"mode">>, <<"0888">>, ?ERROR_BAD_VALUE_INTEGER(<<"mode">>)},
                        {<<"mode">>, <<"888">>, ?ERROR_BAD_VALUE_INTEGER(<<"mode">>)},
                        {<<"mode">>, <<"77777">>, ?ERROR_BAD_VALUE_NOT_IN_RANGE(<<"mode">>, 0, 8#1777)},

                        {<<"offset">>, <<"unicorns">>, ?ERROR_BAD_VALUE_INTEGER(<<"offset">>)},
                        {<<"offset">>, <<"-123">>, ?ERROR_BAD_VALUE_TOO_LOW(<<"offset">>, 0)}
                    ]
                }
            )
        }
    ])).


%% @private
-spec build_rest_create_file_prepare_args_fun(api_test_memory:mem_ref(), file_id:objectid()) ->
    onenv_api_test_runner:prepare_args_fun().
build_rest_create_file_prepare_args_fun(MemRef, ParentDirObjectId) ->
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
-spec build_rest_create_file_validate_call_fun(api_test_memory:mem_ref(), od_user:id()) ->
    onenv_api_test_runner:validate_call_result_fun().
build_rest_create_file_validate_call_fun(MemRef, SpaceOwnerId) ->
    fun(#api_test_ctx{
        node = TestNode,
        client = #auth{subject = #subject{id = UserId}},
        data = Data
    }, {ok, RespCode, RespHeaders, RespBody}) ->
        DataSent = maps:get(body, Data, <<>>),
        Offset = maps:get(<<"offset">>, Data, 0),
        ShouldResultInWrite = Offset > 0 orelse byte_size(DataSent) > 0,

        Type = maps:get(<<"type">>, Data, <<"reg">>),
        Mode = maps:get(<<"mode">>, Data, undefined),

        case {Type, Mode, ShouldResultInWrite, UserId == SpaceOwnerId} of
            {<<"reg">>, <<"0544">>, true, false} ->
                % It is possible to create file but setting perms forbidding write access
                % and uploading some data at the same time should result in error
                ?assertEqual(?HTTP_400_BAD_REQUEST, RespCode),
                ?assertEqual(?REST_ERROR(?ERROR_POSIX(?EACCES)), RespBody),
                api_test_memory:set(MemRef, success, false);
            _ ->
                ?assertEqual(?HTTP_201_CREATED, RespCode),

                #{<<"fileId">> := FileObjectId} = ?assertMatch(#{<<"fileId">> := <<_/binary>>}, RespBody),

                ExpLocation = api_test_utils:build_rest_url(TestNode, [<<"data">>, FileObjectId]),
                ?assertEqual(ExpLocation, maps:get(<<"Location">>, RespHeaders)),

                {ok, FileGuid} = file_id:objectid_to_guid(FileObjectId),

                api_test_memory:set(MemRef, file_guid, FileGuid),
                api_test_memory:set(MemRef, success, true)
        end
    end.


%% @private
-spec build_rest_create_file_verify_fun(api_test_memory:mem_ref(), file_id:file_guid(), [node()]) ->
    boolean().
build_rest_create_file_verify_fun(MemRef, DirGuid, Providers) ->
    fun
        (expected_failure, #api_test_ctx{node = TestNode}) ->
            ExpFilesInDir = api_test_memory:get(MemRef, files),
            ?assertEqual(ExpFilesInDir, ls(TestNode, DirGuid)),
            true;
        (expected_success, #api_test_ctx{node = TestNode, data = Data}) ->
            case api_test_memory:get(MemRef, success) of
                true ->
                    FileGuid = api_test_memory:get(MemRef, file_guid),
                    OtherFilesInDir = api_test_memory:get(MemRef, files),
                    AllFilesInDir = lists:sort([FileGuid | OtherFilesInDir]),

                    ?assertEqual(AllFilesInDir, ls(TestNode, DirGuid)),
                    api_test_memory:set(MemRef, files, AllFilesInDir),

                    ExpName = api_test_memory:get(MemRef, name),
                    {ExpType, DefaultMode} = case maps:get(<<"type">>, Data, <<"reg">>) of
                        <<"reg">> -> {?REGULAR_FILE_TYPE, <<"0664">>};
                        <<"dir">> -> {?DIRECTORY_TYPE, <<"0775">>}
                    end,
                    ExpMode = binary_to_integer(maps:get(<<"mode">>, Data, DefaultMode), 8),

                    lists:foreach(fun(Provider) ->
                        ?assertMatch(
                            {ok, #file_attr{name = ExpName, type = ExpType, mode = ExpMode}},
                            file_test_utils:get_attrs(Provider, FileGuid),
                            ?ATTEMPTS
                        )
                    end, Providers),

                    case ExpType of
                        ?REGULAR_FILE_TYPE ->
                            verify_rest_file_content_update(
                                FileGuid, TestNode, TestNode, Providers, <<>>,
                                maps:get(<<"offset">>, Data, undefined), maps:get(body, Data, <<>>)
                            );
                        ?DIRECTORY_TYPE ->
                            true
                    end;
                false ->
                    ExpFilesInDir = api_test_memory:get(MemRef, files),
                    ?assertEqual(ExpFilesInDir, ls(TestNode, DirGuid)),
                    true
            end
    end.


%% @private
-spec ls(node(), file_id:file_guid()) -> [file_id:file_guid()].
ls(Node, DirGuid) ->
    {ok, Children} = ?assertMatch(
        {ok, _}, lfm_proxy:get_children(Node, ?ROOT_SESS_ID, {guid, DirGuid}, 0, 10000), ?ATTEMPTS
    ),
    lists:sort(lists:map(fun({Guid, _Name}) -> Guid end, Children)).


rest_update_file_content_test(Config) ->
    Providers = ?config(op_worker_nodes, Config),

    {_, _DirPath, DirGuid, DirShareId} = api_test_utils:create_and_sync_shared_file_in_space2(
        <<"dir">>, 8#704, Config
    ),
    {ok, DirObjectId} = file_id:guid_to_objectid(DirGuid),

    OriginalFileSize = 300,
    OriginalFileContent = crypto:strong_rand_bytes(OriginalFileSize),

    UpdateSize = 100,
    UpdateData = crypto:strong_rand_bytes(UpdateSize),

    MemRef = api_test_memory:init(),

    ?assert(onenv_api_test_runner:run_tests(Config, [
        #scenario_spec{
            name = <<"Update file content using rest endpoint">>,
            type = rest,
            target_nodes = Providers,
            client_spec = #client_spec{
                correct = lists_utils:random_sublist([
                    user2, % space owner - doesn't need any perms
                    user3  % files owner (see fun create_shared_file/1)
                ], 1, 1),
                unauthorized = [nobody],
                forbidden_not_in_space = [user1],
                forbidden_in_space = [{user4, ?ERROR_POSIX(?EACCES)}]  % forbidden by file perms
            },

            setup_fun = build_rest_update_file_content_setup_fun(MemRef, OriginalFileContent, Config),
            prepare_args_fun = build_rest_update_file_content_prepare_args_fun(MemRef),
            validate_result_fun = build_rest_update_file_content_validate_call_fun(),
            verify_fun = build_rest_update_file_content_verify_fun(MemRef, OriginalFileContent, Config),

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
-spec build_rest_update_file_content_setup_fun(
    api_test_memory:mem_ref(),
    FileContent :: binary(),
    onenv_api_test_runner:ct_config()
) ->
    onenv_api_test_runner:setup_fun().
build_rest_update_file_content_setup_fun(MemRef, Content, Config) ->
    UserSessIdP1 = api_test_env:get_user_session_id(user3, p1, Config),
    [P1Node] = api_test_env:get_provider_nodes(p1, Config),
    [P2Node] = api_test_env:get_provider_nodes(p2, Config),
    Providers = [P1Node, P2Node],

    FileSize = size(Content),

    fun() ->
        FilePath = filename:join(["/", ?SPACE_2, ?RANDOM_FILE_NAME()]),
        {ok, FileGuid} = api_test_utils:create_file(<<"file">>, P1Node, UserSessIdP1, FilePath, 8#704),

        api_test_utils:write_file(P1Node, UserSessIdP1, FileGuid, 0, Content),
        file_test_utils:assert_size(P2Node, FileGuid, FileSize),
        api_test_utils:assert_distribution(Providers, FileGuid, [{P1Node, FileSize}]),

        api_test_memory:set(MemRef, file_guid, FileGuid)
    end.


%% @private
-spec build_rest_update_file_content_prepare_args_fun(api_test_memory:mem_ref()) ->
    onenv_api_test_runner:prepare_args_fun().
build_rest_update_file_content_prepare_args_fun(MemRef) ->
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
-spec build_rest_update_file_content_validate_call_fun() ->
    onenv_api_test_runner:validate_call_result_fun().
build_rest_update_file_content_validate_call_fun() ->
    fun(#api_test_ctx{}, {ok, RespCode, _RespHeaders, _RespBody}) ->
        ?assertEqual(?HTTP_204_NO_CONTENT, RespCode)
    end.


%% @private
-spec build_rest_update_file_content_verify_fun(
    api_test_memory:mem_ref(),
    OriginalFileContent :: binary(),
    onenv_api_test_runner:ct_config()
) ->
    boolean().
build_rest_update_file_content_verify_fun(MemRef, OriginalFileContent, Config) ->
    [CreationNode = P1Node] = api_test_env:get_provider_nodes(p1, Config),
    [P2Node] = api_test_env:get_provider_nodes(p2, Config),
    AllProviders = [P1Node, P2Node],

    OriginalFileSize = byte_size(OriginalFileContent),

    fun
        (expected_failure, _) ->
            FileGuid = api_test_memory:get(MemRef, file_guid),
            api_test_utils:assert_distribution(AllProviders, FileGuid, [{P1Node, OriginalFileSize}]);
        (expected_success, #api_test_ctx{node = UpdateNode, data = Data}) ->
            FileGuid = api_test_memory:get(MemRef, file_guid),
            Offset = maps:get(<<"offset">>, Data, undefined),
            DataSent = maps:get(body, Data, <<>>),

            verify_rest_file_content_update(
                FileGuid, CreationNode, UpdateNode, AllProviders,
                OriginalFileContent, Offset, DataSent
            )
    end.


%% @private
-spec verify_rest_file_content_update(
    file_id:file_guid(),
    CreationNode :: node(),
    UpdateNode :: node(),
    AllProviders :: [node()],
    OriginalContent :: binary(),
    Offset :: non_neg_integer(),
    DataSent :: binary()
) ->
    true.
verify_rest_file_content_update(
    FileGuid, CreationNode, UpdateNode, AllProviders,
    OriginalContent, Offset, DataSent
) ->
    OriginalFileSize = byte_size(OriginalContent),
    DataSentSize = byte_size(DataSent),

    case Offset of
        undefined ->
            % File was truncated and new data written at offset 0
            file_test_utils:assert_size(AllProviders, FileGuid, DataSentSize),

            ExpDist = case UpdateNode == CreationNode of
                true ->
                    [{UpdateNode, DataSentSize}];
                false ->
                    [{CreationNode, 0}, {UpdateNode, DataSentSize}]
            end,
            api_test_utils:assert_distribution(AllProviders, FileGuid, ExpDist),

            file_test_utils:assert_content(AllProviders, FileGuid, DataSent);
        Offset ->
            ExpSize = max(OriginalFileSize, Offset + DataSentSize),
            file_test_utils:assert_size(AllProviders, FileGuid, ExpSize),

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
            api_test_utils:assert_distribution(AllProviders, FileGuid, ExpDist),

            case Offset > 1024*1024*1024 of  % 1 GB
                true ->
                    % In case of too big files to verify entire content (it will not fit into memory
                    % and reading it chunk by chunk will take too much time as we are speaking about
                    % PB of data) assert only the last fragment
                    ExpContent = str_utils:join_binary([
                        << <<"\0">> || _ <- lists:seq(1, 50) >>,
                        DataSent
                    ]),
                    file_test_utils:assert_content(AllProviders, FileGuid, ExpContent, Offset - 50);
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
                                << <<"\0">> || _ <- lists:seq(OriginalFileSize, Offset - 1) >>,
                                DataSent
                            ])
                    end,
                    file_test_utils:assert_content(AllProviders, FileGuid, ExpContent)
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


%%%===================================================================
%%% GUI File upload test functions
%%%===================================================================


gui_registering_upload_for_directory_should_fail(Config) ->
    UserId = api_test_env:get_user_id(user3, Config),
    UserSessId = api_test_env:get_user_session_id(user3, p1, Config),
    [Worker] = api_test_env:get_provider_nodes(p1, Config),

    {ok, DirGuid} = lfm_proxy:mkdir(Worker, UserSessId, ?FILE_PATH),

    ?assertMatch(
        ?ERROR_BAD_DATA(<<"guid">>, <<"not a regular file">>),
        initialize_gui_upload(UserId, UserSessId, DirGuid, Worker)
    ).


gui_registering_upload_for_non_empty_file_should_fail(Config) ->
    UserId = api_test_env:get_user_id(user3, Config),
    UserSessId = api_test_env:get_user_session_id(user3, p1, Config),
    [Worker] = api_test_env:get_provider_nodes(p1, Config),

    {ok, FileGuid} = lfm_proxy:create(Worker, UserSessId, ?FILE_PATH, ?DEFAULT_FILE_MODE),
    {ok, FileHandle} = lfm_proxy:open(Worker, UserSessId, {guid, FileGuid}, write),
    ?assertMatch({ok, _}, lfm_proxy:write(Worker, FileHandle, 0, crypto:strong_rand_bytes(5))),
    lfm_proxy:fsync(Worker, FileHandle),
    lfm_proxy:close(Worker, FileHandle),

    ?assertMatch(
        ?ERROR_BAD_DATA(<<"guid">>, <<"file is not empty">>),
        initialize_gui_upload(UserId, UserSessId, FileGuid, Worker)
    ).


gui_registering_upload_for_not_owned_file_should_fail(Config) ->
    User1Id = api_test_env:get_user_id(user3, Config),
    User1SessId = api_test_env:get_user_session_id(user3, p1, Config),
    [Worker] = api_test_env:get_provider_nodes(p1, Config),

    User2SessId = api_test_env:get_user_session_id(user4, p1, Config),
    {ok, FileGuid} = lfm_proxy:create(Worker, User2SessId, ?FILE_PATH, ?DEFAULT_FILE_MODE),

    ?assertMatch(
        ?ERROR_BAD_DATA(<<"guid">>, <<"file is not owned by user">>),
        initialize_gui_upload(User1Id, User1SessId, FileGuid, Worker)
    ).


gui_not_registered_upload_should_fail(Config) ->
    UserId = api_test_env:get_user_id(user3, Config),
    UserSessId = api_test_env:get_user_session_id(user3, p1, Config),
    [Worker] = api_test_env:get_provider_nodes(p1, Config),

    {ok, FileGuid} = lfm_proxy:create(Worker, UserSessId, ?FILE_PATH, ?DEFAULT_FILE_MODE),

    ?assertMatch(
        upload_not_registered,
        rpc:call(Worker, page_file_upload, handle_multipart_req, [
            #{size => 20, left => 1},
            ?USER(UserId, UserSessId),
            #{
                <<"guid">> => FileGuid,
                <<"resumableChunkNumber">> => 1,
                <<"resumableChunkSize">> => 20
            }
        ])
    ).


gui_upload_test(Config) ->
    UserId = api_test_env:get_user_id(user3, Config),
    UserSessId = api_test_env:get_user_session_id(user3, p1, Config),
    [Worker] = api_test_env:get_provider_nodes(p1, Config),

    {ok, FileGuid} = lfm_proxy:create(Worker, UserSessId, ?FILE_PATH, ?DEFAULT_FILE_MODE),
    ?assertMatch({ok, _}, lfm_proxy:stat(Worker, UserSessId, {guid, FileGuid})),

    ?assertMatch({ok, _}, initialize_gui_upload(UserId, UserSessId, FileGuid, Worker)),
    ?assertMatch(true, is_gui_upload_registered(UserId, FileGuid, Worker)),

    do_multipart(Worker, ?USER(UserId, UserSessId), 5, 10, 5, FileGuid),

    ?assertMatch({ok, _}, finalize_gui_upload(UserId, UserSessId, FileGuid, Worker)),
    ?assertMatch(false, is_gui_upload_registered(UserId, FileGuid, Worker), ?ATTEMPTS),

    ?assertMatch(
        {ok, #file_attr{size = 250}},
        lfm_proxy:stat(Worker, UserSessId, {guid, FileGuid}),
        ?ATTEMPTS
    ),
    {ok, FileHandle} = lfm_proxy:open(Worker, UserSessId, {guid, FileGuid}, read),
    {ok, Data} = ?assertMatch({ok, _}, lfm_proxy:read(Worker, FileHandle, 0, 250)),
    ?assert(lists:all(fun(X) -> X == true end, [$a == Char || <<Char>> <= Data])),
    lfm_proxy:close(Worker, FileHandle).


gui_stale_upload_file_should_be_deleted(Config) ->
    UserId = api_test_env:get_user_id(user3, Config),
    UserSessId = api_test_env:get_user_session_id(user3, p1, Config),
    [Worker] = api_test_env:get_provider_nodes(p1, Config),

    {ok, FileGuid} = lfm_proxy:create(Worker, UserSessId, ?FILE_PATH, ?DEFAULT_FILE_MODE),
    ?assertMatch({ok, _}, lfm_proxy:stat(Worker, UserSessId, {guid, FileGuid})),

    ?assertMatch({ok, _}, initialize_gui_upload(UserId, UserSessId, FileGuid, Worker)),
    ?assertMatch(true, is_gui_upload_registered(UserId, FileGuid, Worker)),

    % file being uploaded shouldn't be deleted after only 30s of inactivity
    timer:sleep(timer:seconds(30)),
    ?assertMatch({ok, _}, lfm_proxy:stat(Worker, UserSessId, {guid, FileGuid})),
    ?assertMatch(true, is_gui_upload_registered(UserId, FileGuid, Worker)),

    % but if upload is not resumed or finished before INACTIVITY_PERIOD then file should be deleted
    timer:sleep(timer:seconds(100)),
    ?assertMatch(false, is_gui_upload_registered(UserId, FileGuid, Worker), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(Worker, UserSessId, {guid, FileGuid}), ?ATTEMPTS).


gui_finished_upload_file_should_be_left_intact(Config) ->
    UserId = api_test_env:get_user_id(user3, Config),
    UserSessId = api_test_env:get_user_session_id(user3, p1, Config),
    [Worker] = api_test_env:get_provider_nodes(p1, Config),

    {ok, FileGuid} = lfm_proxy:create(Worker, UserSessId, ?FILE_PATH, ?DEFAULT_FILE_MODE),
    ?assertMatch({ok, _}, lfm_proxy:stat(Worker, UserSessId, {guid, FileGuid})),

    ?assertMatch({ok, _}, initialize_gui_upload(UserId, UserSessId, FileGuid, Worker)),
    ?assertMatch(true, is_gui_upload_registered(UserId, FileGuid, Worker)),

    % file being uploaded shouldn't be deleted after only 30s of inactivity
    timer:sleep(timer:seconds(30)),
    ?assertMatch(true, is_gui_upload_registered(UserId, FileGuid, Worker)),
    ?assertMatch({ok, _}, lfm_proxy:stat(Worker, UserSessId, {guid, FileGuid})),

    % uploaded files shouldn't be deleted automatically after INACTIVITY_PERIOD
    ?assertMatch({ok, _}, finalize_gui_upload(UserId, UserSessId, FileGuid, Worker)),
    ?assertMatch(false, is_gui_upload_registered(UserId, FileGuid, Worker), ?ATTEMPTS),

    timer:sleep(timer:seconds(100)),

    ?assertMatch(false, is_gui_upload_registered(UserId, FileGuid, Worker), ?ATTEMPTS),
    ?assertMatch({ok, _}, lfm_proxy:stat(Worker, UserSessId, {guid, FileGuid})).


gui_upload_with_time_warps_test(Config) ->
    UserId = api_test_env:get_user_id(user3, Config),
    UserSessId = api_test_env:get_user_session_id(user3, p1, Config),
    [Worker] = api_test_env:get_provider_nodes(p1, Config),

    {ok, FileGuid} = lfm_proxy:create(Worker, UserSessId, ?FILE_PATH, ?DEFAULT_FILE_MODE),

    FileKey = {guid, FileGuid},
    CurrTime = time_test_utils:get_frozen_time_seconds(),
    ct:pal("~p", [CurrTime]),
    ?assertMatch({ok, #file_attr{mtime = CurrTime}}, lfm_proxy:stat(Worker, UserSessId, FileKey)),
    ?assertMatch({ok, _}, initialize_gui_upload(UserId, UserSessId, FileGuid, Worker)),

    AssertUploadStatusAfterStaleUploadsRemoval = fun(ExpMTime, ExpStatus) ->
        ?assertMatch(
            {ok, #file_attr{mtime = ExpMTime}},
            lfm_proxy:stat(Worker, UserSessId, FileKey),
            ?ATTEMPTS
        ),
        force_stale_gui_uploads_removal(Worker),
        case ExpStatus of
            ongoing ->
                ?assertMatch(true, is_gui_upload_registered(UserId, FileGuid, Worker)),
                ?assertMatch({ok, _}, lfm_proxy:stat(Worker, UserSessId, FileKey));
            removed ->
                ?assertMatch(false, is_gui_upload_registered(UserId, FileGuid, Worker)),
                ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(Worker, UserSessId, FileKey))
        end
    end,
    AssertUploadStatusAfterStaleUploadsRemoval(CurrTime, ongoing),

    % upload should not be canceled if time warps backward (whether write occurred or not)
    PastTime = time_test_utils:simulate_seconds_passing(-1000),

    AssertUploadStatusAfterStaleUploadsRemoval(CurrTime, ongoing),
    do_multipart(Worker, ?USER(UserId, UserSessId), 5, 10, 1, FileGuid),
    AssertUploadStatusAfterStaleUploadsRemoval(PastTime, ongoing),

    % in case of forward time warp if next chunk was written to file (this updates file mtime)
    % it should be left. Otherwise it will be deleted as obsolete upload.
    FutureTime = time_test_utils:simulate_seconds_passing(3000),

    do_multipart(Worker, ?USER(UserId, UserSessId), 5, 10, 1, FileGuid),
    AssertUploadStatusAfterStaleUploadsRemoval(FutureTime, ongoing),

    time_test_utils:simulate_seconds_passing(2000),
    AssertUploadStatusAfterStaleUploadsRemoval(FutureTime, removed).


%% @private
-spec initialize_gui_upload(od_user:id(), session:id(), file_id:file_guid(), node()) ->
    {ok, term()} | errors:error().
initialize_gui_upload(UserId, SessionId, FileGuid, Worker) ->
    rpc:call(Worker, gs_rpc, handle, [
        ?USER(UserId, SessionId), <<"initializeFileUpload">>, #{<<"guid">> => FileGuid}
    ]).


%% @private
-spec finalize_gui_upload(od_user:id(), session:id(), file_id:file_guid(), node()) ->
    {ok, term()} | errors:error().
finalize_gui_upload(UserId, SessionId, FileGuid, Worker) ->
    rpc:call(Worker, gs_rpc, handle, [
        ?USER(UserId, SessionId), <<"finalizeFileUpload">>, #{<<"guid">> => FileGuid}
    ]).


%% @private
-spec is_gui_upload_registered(od_user:id(), file_id:file_guid(), node()) -> boolean().
is_gui_upload_registered(UserId, FileGuid, Worker) ->
    rpc:call(Worker, file_upload_manager, is_upload_registered, [UserId, FileGuid]).


%% @private
-spec force_stale_gui_uploads_removal(node()) -> ok.
force_stale_gui_uploads_removal(Worker) ->
    {file_upload_manager, Worker} ! check_uploads,
    ok.


%% @private
-spec do_multipart(node(), aai:auth(), integer(), integer(), integer(), file_id:file_guid()) ->
    ok.
do_multipart(Worker, Auth, PartsNumber, PartSize, ChunksNumber, FileGuid) ->
    Params = #{
        <<"guid">> => FileGuid,
        <<"resumableChunkSize">> => integer_to_binary(PartsNumber*PartSize)
    },
    lists_utils:pforeach(fun(Chunk) ->
        rpc:call(Worker, page_file_upload, handle_multipart_req, [
            #{size => PartSize, left => PartsNumber},
            Auth,
            Params#{<<"resumableChunkNumber">> => integer_to_binary(Chunk)}
        ])
    end, lists:seq(1, ChunksNumber)).


%% @private
-spec mock_cowboy_multipart(test_config:config()) -> ok.
mock_cowboy_multipart(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_new(Workers, cow_multipart),
    ok = test_utils:mock_new(Workers, cowboy_req),
    ok = test_utils:mock_expect(Workers, cow_multipart, form_data,
        fun(_) -> {file, ok, ok, ok} end
    ),
    ok = test_utils:mock_expect(Workers, cowboy_req, read_part,
        fun
            (#{done := true} = Req) ->
                {done, Req};
            (Req) ->
                {ok, [], Req}
        end
    ),
    ok = test_utils:mock_expect(Workers, cowboy_req, read_part_body,
        fun
            (#{left := 1, size := Size} = Req, _) ->
                {ok, << <<$a>> || _ <- lists:seq(1, Size) >>, Req#{done => true}};
            (#{left := Left, size := Size} = Req, _) ->
                {more, << <<$a>> || _ <- lists:seq(1, Size) >>, Req#{left => Left-1}}
        end
    ).


%% @private
-spec unmock_cowboy_multipart(test_config:config()) -> ok.
unmock_cowboy_multipart(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, cowboy_req),
    test_utils:mock_unload(Workers, cow_multipart).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    ssl:start(),
    hackney:start(),
    api_test_env:init_per_suite(Config, #onenv_test_config{envs = [
        {op_worker, op_worker, [
            {fuse_session_grace_period_seconds, 24 * 60 * 60}
        ]}
    ]}).


end_per_suite(_Config) ->
    hackney:stop(),
    ssl:stop().


init_per_testcase(Case, Config) when
    Case =:= gui_not_registered_upload_should_fail;
    Case =:= gui_upload_test
->
    mock_cowboy_multipart(Config),
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(gui_upload_with_time_warps_test = Case, Config) ->
    mock_cowboy_multipart(Config),
    time_test_utils:freeze_time(Config),
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 20}),
    lfm_proxy:init(Config).

end_per_testcase(Case, Config) when
    Case =:= gui_not_registered_upload_should_fail;
    Case =:= gui_upload_test
->
    unmock_cowboy_multipart(Config),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(gui_upload_with_time_warps_test = Case, Config) ->
    unmock_cowboy_multipart(Config),
    time_test_utils:unfreeze_time(Config),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).
