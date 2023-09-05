%%%-------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------
%%% @doc
%%% CDMI tests
%%% @end
%%%-------------------------------------
-module(cdmi_test_SUITE).
-author("Tomasz Lichon").

-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").
-include("onenv_test_utils.hrl").
-include("cdmi_test.hrl").

%% API
-export([
    groups/0, all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_group/2, end_per_group/2,
    init_per_testcase/2, end_per_testcase/1
]).

-export([
    list_dir/1,
    objectid/1,
    errors/1,
    download_file_in_blocks/1,

    get_file/1,
    metadata/1,
    delete_file/1,
    delete_dir/1,
    create_file/1,
    create_raw_file_with_cdmi_version_header_should_succeed/1,
    create_cdmi_file_without_cdmi_version_header_should_fail/1,
    create_dir/1,
    create_raw_dir_with_cdmi_version_header_should_succeed/1,
    create_cdmi_dir_without_cdmi_version_header_should_fail/1,
    update_file/1,
    capabilities/1,
    use_supported_cdmi_version/1,
    use_unsupported_cdmi_version/1,
    copy/1,
    move/1,
    moved_permanently/1,
    move_copy_conflict/1,
    request_format_check/1,
    mimetype_and_encoding/1,
    out_of_range/1,
    partial_upload/1,
    acl/1,
    accept_header/1,
    download_empty_file/1
]).


groups() -> [
    {sequential_tests, [sequential], [
        %% list_dir needs to start first as it lists the main directory
        list_dir,
        objectid,
        errors,
        download_file_in_blocks
    ]},
    {parallel_tests, [parallel], [
        get_file,
        metadata,
        delete_file,
        delete_dir,
        create_file,
        create_raw_file_with_cdmi_version_header_should_succeed,
        create_cdmi_file_without_cdmi_version_header_should_fail,
        create_dir,
        create_raw_dir_with_cdmi_version_header_should_succeed,
        create_cdmi_dir_without_cdmi_version_header_should_fail,update_file,
        update_file,
        capabilities,
        use_supported_cdmi_version,
        use_unsupported_cdmi_version,
        copy,
        move,
        moved_permanently,
        move_copy_conflict,
        request_format_check,
        mimetype_and_encoding,
        out_of_range,
        partial_upload,
        acl,
        accept_header,
        download_empty_file
    ]}

].

all() -> [
    {group, sequential_tests}
%%    {group, parallel_tests}
].

-record(chunk, {
    offset :: non_neg_integer(),
    size :: non_neg_integer()
}).

-define(DEFAULT_STORAGE_BLOCK_SIZE, 100).

-define(RUN_TEST(__TEST_BASE_MODULE),
    try
        __TEST_BASE_MODULE:?FUNCTION_NAME(#cdmi_test_config{
            p1_selector = krakow,
            p2_selector = krakow,
            space_selector = space_krk})
    catch __TYPE:__REASON:__STACKTRACE ->
        ct:pal("Test failed due to ~p:~p.~nStacktrace: ~p", [__TYPE, __REASON, __STACKTRACE]),
        error(test_failed)
    end
).

-define(RUN_BASE_TEST(), ?RUN_TEST(cdmi_test_base)).
%%-define(RUN_CREATE_TEST(), ?RUN_TEST(cdmi_create_tests_base)).


%%%===================================================================
%%% Test functions
%%%===================================================================

%%%===================================================================
%%% Sequential tests
%%%===================================================================

list_dir(_Config) ->
    ?RUN_BASE_TEST().

objectid(_Config) ->
    ?RUN_BASE_TEST().

errors(_Config) ->
    ?RUN_BASE_TEST().

download_file_in_blocks(_Config) ->
    [_WorkerP1, WorkerP2] = Workers = oct_background:get_provider_nodes(krakow),
    SpaceName = binary_to_list(oct_background:get_space_name(space_krk)),
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = SpaceName ++ "/" ++ RootName,

    AuthHeaders = [rest_test_utils:user_token_header(oct_background:get_user_access_token(user2))],

    % Create file
    FilePath = filename:join([RootPath, <<"upload_file_in_blocks">>]),
    Data = crypto:strong_rand_bytes(200),

    onenv_file_test_utils:create_and_sync_file_tree(
        user2,
        node_cache:get(root_dir_guid),
        #file_spec{
            name = <<"upload_file_in_blocks">>,
            content = Data
        },
        krakow
    ),

    % Reading file with ?DEFAULT_STORAGE_BLOCK_SIZE should result in 2 full reads
    {ok, _, _, Response1} = ?assertMatch(
        {ok, 200, _Headers, _Response},
        cdmi_test_utils:do_request(WorkerP2, FilePath, get, AuthHeaders, <<>>)
    ),
    ?assertEqual(Data, Response1),
    ?assertEqual(
        [
            #chunk{offset = 0, size = 100},
            #chunk{offset = 100, size = 100}
        ],
        get_read_chunks()
    ),

    % When streaming starting from offset not being multiple of block size
    % only bytes up to next smallest multiple of block size should be read
    % in first block. Next blocks read should be of equal size to storage
    % block size. The exception to this is last block as it only returns
    % remaining bytes.
    set_storage_block_size(Workers, 50),
    DataPart = binary:part(Data, {33, 100}),
    RangeHeader = {?HDR_RANGE, <<"bytes=33-132">>},    % 33-132 inclusive
    {ok, _, _, Response2} = ?assertMatch(
        {ok, 206, _Headers, _Response},
        cdmi_test_utils:do_request(WorkerP2, FilePath, get, [RangeHeader | AuthHeaders], <<>>)
    ),
    ?assertEqual(DataPart, Response2),

    ?assertEqual(
        [
            #chunk{offset = 33, size = 17},
            #chunk{offset = 50, size = 50},
            #chunk{offset = 100, size = 33}
        ],
        get_read_chunks()
    ).

%%%===================================================================
%%% Parallel tests
%%%===================================================================

get_file(_Config) ->
    ?RUN_BASE_TEST().

metadata(_Config) ->
    ?RUN_BASE_TEST().

delete_file(_Config) ->
    ?RUN_BASE_TEST().

delete_dir(_Config) ->
    ?RUN_BASE_TEST().

create_file(_Config) ->
    ?RUN_BASE_TEST().

create_raw_file_with_cdmi_version_header_should_succeed(_Config) ->
    ?RUN_BASE_TEST().

create_cdmi_file_without_cdmi_version_header_should_fail(_Config) ->
    ?RUN_BASE_TEST().

create_dir(_Config) ->
    ?RUN_BASE_TEST().

create_raw_dir_with_cdmi_version_header_should_succeed(_Config) ->
    ?RUN_BASE_TEST().

create_cdmi_dir_without_cdmi_version_header_should_fail(_Config) ->
    ?RUN_BASE_TEST().

update_file(_Config) ->
    ?RUN_BASE_TEST().

capabilities(_Config) ->
    ?RUN_BASE_TEST().

use_supported_cdmi_version(_Config) ->
    ?RUN_BASE_TEST().

use_unsupported_cdmi_version(_Config) ->
    ?RUN_BASE_TEST().

copy(_Config) ->
    ?RUN_BASE_TEST().

move(_Config) ->
    ?RUN_BASE_TEST().

moved_permanently(_Config) ->
    ?RUN_BASE_TEST().

move_copy_conflict(_Config) ->
    ?RUN_BASE_TEST().

request_format_check(_Config) ->
    ?RUN_BASE_TEST().

mimetype_and_encoding(_Config) ->
    ?RUN_BASE_TEST().

out_of_range(_Config) ->
    ?RUN_BASE_TEST().

partial_upload(_Config) ->
    ?RUN_BASE_TEST().

acl(_Config) ->
    ?RUN_BASE_TEST().

accept_header(_Config) ->
    ?RUN_BASE_TEST().

download_empty_file(_Config) ->
    [_WorkerP1, WorkerP2] = oct_background:get_provider_nodes(krakow),
    SpaceName = binary_to_list(oct_background:get_space_name(space_krk)),
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = SpaceName ++ "/" ++ RootName,

    AuthHeaders = [rest_test_utils:user_token_header(oct_background:get_user_access_token(user2))],
    SessionId = oct_background:get_user_session_id(user2, krakow),
    UserId2 = oct_background:get_user_id(user2),

    % Create file
    FileName = <<"download_empty_file">>,
    FilePath = filename:join([RootPath, FileName]),
    {ok, FileGuid} = lfm_proxy:create(WorkerP2, SessionId, FilePath),
    {ok, ObjectId} = file_id:guid_to_objectid(FileGuid),

    ?assertMatch(ok, lfm_proxy:truncate(WorkerP2, SessionId, ?FILE_REF(FileGuid), 0)),

    {ok, _, _, Response} = ?assertMatch(
        {ok, 200, _Headers, _Response},
        cdmi_test_utils:do_request(
            WorkerP2, FilePath, get, [?CDMI_VERSION_HEADER | AuthHeaders], <<>>
        )
    ),
    ?assertMatch(
        #{
            <<"completionStatus">> := <<"Complete">>,
            <<"metadata">> := #{
                <<"cdmi_owner">> := UserId2,
                <<"cdmi_size">> := <<"0">>
            },
            <<"objectID">> := ObjectId,
            <<"objectName">> := FileName,
            <<"objectType">> := <<"application/cdmi-object">>,
            <<"value">> := <<>>,
            <<"valuerange">> := <<"0--1">>,
            <<"valuetransferencoding">> := <<"base64">>
        },
        json_utils:decode(Response)
    ).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    {{YY, MM, DD}, {Hour, Min, Sec}} = time:seconds_to_datetime(global_clock:timestamp_seconds()),
    DateString = str_utils:format_bin(
        "~4..0w-~2..0w-~2..0w_~2..0w~2..0w~2..0w",
        [YY, MM, DD, Hour, Min, Sec]
    ),
    oct_background:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "1op-2nodes",
        envs = [{op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}],
        posthook = fun(NewConfig) ->
            #object{guid = DirGuid} = onenv_file_test_utils:create_and_sync_file_tree(user2, space_krk,
                #dir_spec{
                    name = DateString
                }, krakow
            ),
            node_cache:put(root_dir_guid, DirGuid),
            node_cache:put(root_dir_name, binary:bin_to_list(DateString)),
            NewConfig
        end
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_group(_Group, Config) ->
    lfm_proxy:init(Config, false).


end_per_group(_Group, Config) ->
    lfm_proxy:teardown(Config).


init_per_testcase(download_file_in_blocks = Case, Config) ->
    Self = self(),
    Workers = oct_background:get_provider_nodes(krakow),

    test_utils:mock_new(Workers, [lfm], [passthrough]),
    test_utils:mock_expect(Workers, lfm, check_size_and_read, fun(FileHandle, Offset, ToRead) ->
        {ok, _, Data} = Res = meck:passthrough([FileHandle, Offset, ToRead]),
        Self ! {read, #chunk{offset = Offset, size = byte_size(Data)}},
        Res
    end),
    mock_storage_get_block_size(Workers),
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(_Case, Config) ->
    Config.


end_per_testcase(download_file_in_blocks = Case) ->
    Workers = oct_background:get_provider_nodes(krakow),
    unmock_storage_get_block_size(Workers),
    ok = test_utils:mock_unload(Workers, [lfm]),
    end_per_testcase(?DEFAULT_CASE(Case));

end_per_testcase(_Case) ->
    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================


get_read_chunks() ->
    get_read_chunks([]).


get_read_chunks(Chunks) ->
    receive
        {read, Chunk} ->
            get_read_chunks([Chunk | Chunks])
    after 10 ->
        lists:reverse(Chunks)
    end.


mock_storage_get_block_size(Workers) ->
    test_utils:mock_new(Workers, [storage], [passthrough]),
    test_utils:mock_expect(Workers, storage, get_block_size, fun(_) ->
        node_cache:get(storage_block_size, ?DEFAULT_STORAGE_BLOCK_SIZE)
    end).


unmock_storage_get_block_size(Workers) ->
    ok = test_utils:mock_unload(Workers, [storage]).


set_storage_block_size(Workers, BlockSize) ->
    ?assertMatch(
        {_, []},
        utils:rpc_multicall(Workers, node_cache, put, [storage_block_size, BlockSize])
    ).
