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
-include_lib("ctool/include/http/headers.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

%% API
-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    list_dir_test/1,
    get_file_test/1,
    metadata_test/1,
    delete_file_test/1,
    delete_dir_test/1,
    create_file_test/1,
    update_file_test/1,
    create_dir_test/1,
    capabilities_test/1,
    use_supported_cdmi_version_test/1,
    use_unsupported_cdmi_version_test/1,
    moved_permanently_test/1,
    objectid_test/1,
    request_format_check_test/1,
    mimetype_and_encoding_test/1,
    out_of_range_test/1,
    partial_upload_test/1,
    acl_test/1,
    errors_test/1,
    accept_header_test/1,
    move_copy_conflict_test/1,
    move_test/1,
    copy_test/1,
    create_raw_file_with_cdmi_version_header_should_succeed_test/1,
    create_raw_dir_with_cdmi_version_header_should_succeed_test/1,
    create_cdmi_file_without_cdmi_version_header_should_fail_test/1,
    create_cdmi_dir_without_cdmi_version_header_should_fail_test/1,
    download_empty_file/1,
    download_file_in_blocks/1
]).

all() -> [
        list_dir_test,
        get_file_test,
        metadata_test,
        delete_file_test,
        delete_dir_test,
        create_file_test,
        create_dir_test,
        update_file_test,
        capabilities_test,
        use_supported_cdmi_version_test,
        use_unsupported_cdmi_version_test,
        moved_permanently_test,
        objectid_test,
        request_format_check_test,
        mimetype_and_encoding_test,
        out_of_range_test,
        move_copy_conflict_test,
        move_test,
        copy_test,
        partial_upload_test,
        acl_test,
        errors_test,
        accept_header_test,
        create_raw_file_with_cdmi_version_header_should_succeed_test,
        create_raw_dir_with_cdmi_version_header_should_succeed_test,
        create_cdmi_file_without_cdmi_version_header_should_fail_test,
        create_cdmi_dir_without_cdmi_version_header_should_fail_test,
        download_empty_file,
        download_file_in_blocks
].


-record(chunk, {
    offset :: non_neg_integer(),
    size :: non_neg_integer()
}).

-define(DEFAULT_STORAGE_BLOCK_SIZE, 100).
-define(CDMI_VERSION_HEADER, {<<"X-CDMI-Specification-Version">>, <<"1.1.1">>}).


%%%===================================================================
%%% Test functions
%%%===================================================================


list_dir_test(Config) ->
    cdmi_test_base:list_dir(Config, krakow, krakow, space_krk).


get_file_test(Config) ->
    cdmi_test_base:get_file(Config, krakow, krakow, space_krk).


metadata_test(Config) ->
    cdmi_test_base:metadata(Config, krakow, krakow, space_krk).


delete_file_test(Config) ->
    cdmi_test_base:delete_file(Config, krakow, krakow, space_krk).


delete_dir_test(Config) ->
    cdmi_test_base:delete_dir(Config, krakow, krakow, space_krk).


create_file_test(Config) ->
    cdmi_test_base:create_file(Config, krakow, krakow, space_krk).


create_dir_test(Config) ->
    cdmi_test_base:create_dir(Config, krakow, krakow, space_krk).


update_file_test(Config) ->
    cdmi_test_base:update_file(Config, krakow, krakow, space_krk).


capabilities_test(Config) ->
    cdmi_test_base:capabilities(Config, krakow, krakow).


use_supported_cdmi_version_test(Config) ->
    cdmi_test_base:use_supported_cdmi_version(Config, krakow, krakow).


use_unsupported_cdmi_version_test(Config) ->
    cdmi_test_base:use_unsupported_cdmi_version(Config, krakow, krakow).


moved_permanently_test(Config) ->
    cdmi_test_base:moved_permanently(Config, krakow, krakow, space_krk).


objectid_test(Config) ->
    cdmi_test_base:objectid(Config, krakow, krakow, space_krk).


request_format_check_test(Config) ->
    cdmi_test_base:request_format_check(Config, krakow, krakow, space_krk).


mimetype_and_encoding_test(Config) ->
    cdmi_test_base:mimetype_and_encoding(Config, krakow, krakow, space_krk).


out_of_range_test(Config) ->
    cdmi_test_base:out_of_range(Config, krakow, krakow, space_krk).


move_copy_conflict_test(Config) ->
    cdmi_test_base:move_copy_conflict(Config, krakow, krakow, space_krk).


move_test(Config) ->
    cdmi_test_base:move(Config, krakow, krakow, space_krk).


copy_test(Config) ->
    cdmi_test_base:copy(Config, krakow, krakow, space_krk).


partial_upload_test(Config) ->
    cdmi_test_base:partial_upload(Config, krakow, krakow, space_krk).


acl_test(Config) ->
    cdmi_test_base:acl(Config, krakow, krakow, space_krk).


errors_test(Config) ->
    cdmi_test_base:errors(Config, krakow, krakow, space_krk).


accept_header_test(Config) ->
    cdmi_test_base:accept_header(Config, krakow, krakow).


create_raw_file_with_cdmi_version_header_should_succeed_test(Config) ->
    cdmi_test_base:create_raw_file_with_cdmi_version_header_should_succeed(Config, krakow, krakow, space_krk).


create_raw_dir_with_cdmi_version_header_should_succeed_test(Config) ->
    cdmi_test_base:create_raw_dir_with_cdmi_version_header_should_succeed(Config, krakow, krakow, space_krk).


create_cdmi_file_without_cdmi_version_header_should_fail_test(Config) ->
    cdmi_test_base:create_cdmi_file_without_cdmi_version_header_should_fail(Config, krakow, krakow, space_krk).


create_cdmi_dir_without_cdmi_version_header_should_fail_test(Config) ->
    cdmi_test_base:create_cdmi_dir_without_cdmi_version_header_should_fail(Config, krakow, krakow, space_krk).


download_empty_file(_Config) ->
    [_WorkerP1, WorkerP2] = oct_background:get_provider_nodes(krakow),
    SpaceName = oct_background:get_space_name(space_krk),

    AuthHeaders = [rest_test_utils:user_token_header(oct_background:get_user_access_token(user2))],
    SessionId = oct_background:get_user_session_id(user2, krakow),
    UserId2 = oct_background:get_user_id(user2),

    % Create file
    FileName = <<"download_empty_file">>,
    FilePath = filename:join([SpaceName, FileName]),
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


download_file_in_blocks(_Config) ->
    [_WorkerP1, WorkerP2] = Workers = oct_background:get_provider_nodes(krakow),
    SpaceName = oct_background:get_space_name(space_krk),

    AuthHeaders = [rest_test_utils:user_token_header(oct_background:get_user_access_token(user2))],
    SessionId = oct_background:get_user_session_id(user2, krakow),

    % Create file
    FilePath = filename:join([SpaceName, <<"upload_file_in_blocks">>]),
    Data = crypto:strong_rand_bytes(200),{ok, Guid} = lfm_proxy:create(WorkerP2, SessionId, FilePath),
    {ok, Handle} = lfm_proxy:open(WorkerP2, SessionId, ?FILE_REF(Guid), write),
    {ok, _} = lfm_proxy:write(WorkerP2, Handle, 0, Data),
    ok = lfm_proxy:close(WorkerP2, Handle),

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
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    oct_background:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "1op-2nodes"
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


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
    lfm_proxy:init(Config),
    Config.


end_per_testcase(download_file_in_blocks = Case, Config) ->
    Workers = oct_background:get_provider_nodes(krakow),
    unmock_storage_get_block_size(Workers),
    ok = test_utils:mock_unload(Workers, [lfm]),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config),
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
