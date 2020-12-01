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
    create_file_test/1,
    update_file_content_test/1
]).

all() -> [
    create_file_test,
    update_file_content_test
].


-define(ATTEMPTS, 30).


%%%===================================================================
%%% File download test functions
%%%===================================================================


create_file_test(Config) ->
    [P1Node] = api_test_env:get_provider_nodes(p1, Config),
    [P2Node] = api_test_env:get_provider_nodes(p2, Config),
    Providers = [P1Node, P2Node],

    SpaceOwnerId = api_test_env:get_user_id(user2, Config),
    UserSessIdP1 = api_test_env:get_user_session_id(user3, p1, Config),
    UserSessIdP2 = api_test_env:get_user_session_id(user3, p2, Config),

    {_, DirPath, DirGuid, DirShareId} = api_test_utils:create_and_sync_shared_file_in_space2(
        <<"dir">>, 8#704, Config
    ),
    {ok, DirObjectId} = file_id:guid_to_objectid(DirGuid),

    UsedFileName = ?RANDOM_FILE_NAME(),
    FilePath = filename:join([DirPath, UsedFileName]),
    {ok, FileGuid} = api_test_utils:create_file(<<"file">>, P1Node, UserSessIdP1, FilePath, 8#777),
    api_test_utils:wait_for_file_sync(P2Node, UserSessIdP2, FileGuid),

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

            prepare_args_fun = build_create_file_prepare_args_fun(MemRef, DirObjectId),
            validate_result_fun = build_create_file_validate_call_fun(MemRef, SpaceOwnerId),
            verify_fun = build_create_file_verify_fun(MemRef, DirGuid, Providers),

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
-spec build_create_file_verify_fun(api_test_memory:mem_ref(), file_id:file_guid(), [node()]) ->
    boolean().
build_create_file_verify_fun(MemRef, DirGuid, Providers) ->
    fun
        (expected_failure, #api_test_ctx{node = TestNode}) ->
            ExpFilesInDir = api_test_memory:get(MemRef, files),
            ?assertEqual(ExpFilesInDir, ls(TestNode, DirGuid), ?ATTEMPTS),
            true;
        (expected_success, #api_test_ctx{node = TestNode, data = Data}) ->
            case api_test_memory:get(MemRef, success) of
                true ->
                    FileGuid = api_test_memory:get(MemRef, file_guid),
                    OtherFilesInDir = api_test_memory:get(MemRef, files),
                    AllFilesInDir = lists:sort([FileGuid | OtherFilesInDir]),

                    ?assertEqual(AllFilesInDir, ls(TestNode, DirGuid), ?ATTEMPTS),
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
                            api_test_utils:get_file_attrs(Provider, FileGuid),
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
                    ?assertEqual(ExpFilesInDir, ls(TestNode, DirGuid)),
                    true
            end
    end.


%% @private
-spec ls(node(), file_id:file_guid()) -> [file_id:file_guid()] | {error, term()}.
ls(Node, DirGuid) ->
    case lfm_proxy:get_children(Node, ?ROOT_SESS_ID, {guid, DirGuid}, 0, 10000) of
        {ok, Children} ->
            lists:sort(lists:map(fun({Guid, _Name}) -> Guid end, Children));
        {error, _} = Error ->
            Error
    end.


update_file_content_test(Config) ->
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

            setup_fun = build_update_file_content_setup_fun(MemRef, OriginalFileContent, Config),
            prepare_args_fun = build_update_file_content_prepare_args_fun(MemRef),
            validate_result_fun = build_update_file_content_validate_call_fun(),
            verify_fun = build_update_file_content_verify_fun(MemRef, OriginalFileContent, Config),

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
    FileContent :: binary(),
    onenv_api_test_runner:ct_config()
) ->
    onenv_api_test_runner:setup_fun().
build_update_file_content_setup_fun(MemRef, Content, Config) ->
    UserSessIdP1 = api_test_env:get_user_session_id(user3, p1, Config),
    [P1Node] = api_test_env:get_provider_nodes(p1, Config),
    [P2Node] = api_test_env:get_provider_nodes(p2, Config),
    Providers = [P1Node, P2Node],

    FileSize = size(Content),

    fun() ->
        FilePath = filename:join(["/", ?SPACE_2, ?RANDOM_FILE_NAME()]),
        {ok, FileGuid} = api_test_utils:create_file(<<"file">>, P1Node, UserSessIdP1, FilePath, 8#704),

        api_test_utils:write_file(P1Node, UserSessIdP1, FileGuid, 0, Content),
        ?assertMatch({ok, #file_attr{size = FileSize}}, api_test_utils:get_file_attrs(P2Node, FileGuid), ?ATTEMPTS),
        api_test_utils:assert_distribution(Providers, FileGuid, [{P1Node, FileSize}]),

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
    OriginalFileContent :: binary(),
    onenv_api_test_runner:ct_config()
) ->
    boolean().
build_update_file_content_verify_fun(MemRef, OriginalFileContent, Config) ->
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
            assert_file_size(AllProviders, FileGuid, DataSentSize),

            ExpDist = case UpdateNode == CreationNode of
                true ->
                    [{UpdateNode, DataSentSize}];
                false ->
                    [{CreationNode, 0}, {UpdateNode, DataSentSize}]
            end,
            api_test_utils:assert_distribution(AllProviders, FileGuid, ExpDist),

            assert_file_content(AllProviders, FileGuid, DataSent);
        Offset ->
            assert_file_size(AllProviders, FileGuid, max(OriginalFileSize, Offset + DataSentSize)),

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
                    assert_file_content(AllProviders, FileGuid, Offset - 50, ExpContent);
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
                    assert_file_content(AllProviders, FileGuid, ExpContent)
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
-spec assert_file_size([node()], file_id:file_guid(), non_neg_integer()) -> ok.
assert_file_size(AllProviders, FileGuid, ExpFileSize) ->
    lists:foreach(fun(Provider) ->
        ?assertMatch(
            {ok, #file_attr{size = ExpFileSize}},
            api_test_utils:get_file_attrs(Provider, FileGuid),
            ?ATTEMPTS
        )
    end, AllProviders).


%% @private
-spec assert_file_content([node()], file_id:file_guid(), ExpContent :: binary()) -> ok.
assert_file_content(Nodes, FileGuid, ExpContent) ->
    assert_file_content(Nodes, FileGuid, 0, ExpContent).


%% @private
-spec assert_file_content(
    [node()],
    file_id:file_guid(),
    Offset :: non_neg_integer(),
    ExpContent :: binary()
) ->
    ok.
assert_file_content(Nodes, FileGuid, Offset, ExpContent) ->
    lists:foreach(fun(Node) ->
        ?assertEqual({ok, ExpContent}, get_file_content(Node, FileGuid, Offset), ?ATTEMPTS)
    end, Nodes).


%% @private
-spec get_file_content(node(), file_id:file_guid(), Offset :: non_neg_integer()) -> binary().
get_file_content(Node, FileGuid, Offset) ->
    FileKey = {guid, FileGuid},

    {ok, #file_attr{size = FileSize}} = lfm_proxy:stat(Node, ?ROOT_SESS_ID, FileKey),
    {ok, FileHandle} = lfm_proxy:open(Node, ?ROOT_SESS_ID, FileKey, read),
    lfm_proxy:read(Node, FileHandle, Offset, FileSize).


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


init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 20}),
    lfm_proxy:init(Config).


end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).
