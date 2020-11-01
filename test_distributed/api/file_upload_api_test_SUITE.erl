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
-module(file_upload_api_test_SUITE).
-author("Bartosz Walkowicz").

-include("file_api_test_utils.hrl").
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
    update_file_content_test/1
]).

all() -> [
    update_file_content_test
].


-define(ATTEMPTS, 30).


-type test_mode() :: normal_mode | share_mode.


%%%===================================================================
%%% File download test functions
%%%===================================================================


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
            name = <<"Download file using rest endpoint">>,
            type = rest,
            target_nodes = Providers,
            client_spec = #client_spec{
                correct = [
                    % TODO unblock after making space owner work on posix storage
%%                    user2, % space owner - doesn't need any perms
                    user3  % files owner (see fun create_shared_file/1)
                ],
                unauthorized = [nobody],
                forbidden_not_in_space = [user1],
                forbidden_in_space = [{user4, ?ERROR_POSIX(?EACCES)}]  % forbidden by file perms
            },

            setup_fun = build_download_file_setup_fun(MemRef, OriginalFileContent, Config),
            prepare_args_fun = build_update_file_content_prepare_args_fun(MemRef, normal_mode),
            validate_result_fun = build_update_file_content_validate_call_fun(),
            verify_fun = build_upload_file_content_verify_fun(MemRef, OriginalFileContent, Config),

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
                    bad_values = [{bad_id, DirObjectId, {rest, ?ERROR_POSIX(?EISDIR)}}]
                }
            )
        }
    ])).


%% @private
-spec build_update_file_content_prepare_args_fun(api_test_memory:mem_ref(), test_mode()) ->
    onenv_api_test_runner:prepare_args_fun().
build_update_file_content_prepare_args_fun(MemRef, TestMode) ->
    fun(#api_test_ctx{data = Data0}) ->
        FileGuid = get_file_guid(MemRef, TestMode),
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
build_upload_file_content_verify_fun(MemRef, OriginalFileContent, Config) ->
    [CreationNode = P1Node] = api_test_env:get_provider_nodes(p1, Config),
    [P2Node] = api_test_env:get_provider_nodes(p2, Config),
    AllProviders = [P1Node, P2Node],

    OriginalFileSize = byte_size(OriginalFileContent),

    fun
        (expected_failure, _) ->
            FileGuid = api_test_memory:get(MemRef, file_guid),
            api_test_utils:assert_distribution(AllProviders, [FileGuid], [{P1Node, OriginalFileSize}]);
        (expected_success, #api_test_ctx{node = UpdateNode, data = Data}) ->
            FileGuid = api_test_memory:get(MemRef, file_guid),
            Offset = maps:get(<<"offset">>, Data, undefined),

            case {Offset, maps:get(body, Data, <<>>)} of
                {undefined, _} ->
                    % TODO FIX TRUNCATE
                    true;
                {_, <<>>} ->
                    % TODO EMPTY BODY
                    true;
                {_, DataSent} ->
                    verify_file_content_update(
                        FileGuid, CreationNode, UpdateNode, AllProviders,
                        OriginalFileContent, Offset, DataSent
                    )
            end
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
            % File was overwritten completely
            api_test_utils:assert_distribution(AllProviders, [FileGuid], [{UpdateNode, DataSentSize}]),
            assert_file_content(AllProviders, FileGuid, DataSent);
        Offset ->
            ExpDist = case UpdateNode == CreationNode of
                true ->
                    case Offset =< OriginalFileSize of
                        true ->
                            [{CreationNode, max(OriginalFileSize, Offset + DataSentSize)}];
                        false ->
                            [{CreationNode, [
                                #file_block{offset = 0, size = OriginalFileSize},
                                #file_block{offset = Offset, size = DataSentSize}
                            ]}]
                    end;
                false ->
                    case Offset + DataSentSize < OriginalFileSize of
                        true ->
                            [
                                {CreationNode, [
                                    #file_block{offset = 0, size = Offset},
                                    #file_block{
                                        offset = Offset + DataSentSize,
                                        size = OriginalFileSize - Offset - DataSentSize
                                    }
                                ]},
                                {UpdateNode, [#file_block{offset = Offset, size = DataSentSize}]}
                            ];
                        false ->
                            [
                                {CreationNode, [#file_block{offset = 0, size = min(Offset, OriginalFileSize)}]},
                                {UpdateNode, [#file_block{offset = Offset, size = DataSentSize}]}
                            ]
                    end
            end,
            api_test_utils:assert_distribution(AllProviders, [FileGuid], ExpDist),

            case Offset > 1000 * OriginalFileSize of
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
%%% Internal functions
%%%===================================================================


%% TODO rm redundancy with similar function in file_stream_api_test_SUITE
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
        api_test_utils:assert_distribution(Providers, [FileGuid], [{P1Node, FileSize}]),

        api_test_memory:set(MemRef, file_guid, FileGuid),
        api_test_memory:set(MemRef, share_id, ShareId)
    end.


%% @private
-spec get_file_guid(api_test_memory:mem_ref(), test_mode()) -> file_id:file_guid().
get_file_guid(MemRef, TestMode) ->
    BareGuid = api_test_memory:get(MemRef, file_guid),
    case TestMode of
        normal_mode ->
            BareGuid;
        share_mode ->
            ShareId = api_test_memory:get(MemRef, share_id),
            file_id:guid_to_share_guid(BareGuid, ShareId)
    end.


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
