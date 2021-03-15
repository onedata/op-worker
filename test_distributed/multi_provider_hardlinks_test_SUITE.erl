%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of hardlinks in multi provider environment
%%% @end
%%%-------------------------------------------------------------------
-module(multi_provider_hardlinks_test_SUITE).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

-export([
    hardlinks_test/1,
    first_reading_and_writing_via_hardlink_test/1,
    create_hardlink_to_hardlink_test/1
]).

-define(TEST_CASES, [
    hardlinks_test,
    first_reading_and_writing_via_hardlink_test,
    create_hardlink_to_hardlink_test
]).

all() ->
    ?ALL(?TEST_CASES).

-define(match(Expect, Expr, Attempts),
    case Attempts of
        0 ->
            ?assertMatch(Expect, Expr);
        _ ->
            ?assertMatch(Expect, Expr, Attempts)
    end
).
-define(GET_TIMES(Worker, Uuid), rpc:call(Worker, times, get, [Uuid])).
-define(GET_LOCATION(Worker, Uuid), rpc:call(Worker, file_location, get_local, [Uuid])).

%%%===================================================================
%%% Test functions
%%%===================================================================

hardlinks_test(Config0) ->
    Attempts = 60,
    Config = multi_provider_file_ops_test_base:extend_config(Config0, <<"user1">>, {4, 0, 0, 2}, Attempts),
    SessId = ?config(session, Config),
    SpaceName = ?config(space_name, Config),
    Worker1 = ?config(worker1, Config),
    [Worker2 | _] = ?config(workers2, Config),

    % Setup environment
    Dir = <<"/", SpaceName/binary, "/",  (generator:gen_name())/binary>>,
    File = <<Dir/binary, "/", (generator:gen_name())/binary>>,
    ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker1, SessId(Worker1), Dir)),
    FileContent = <<"1234567890abcd">>,
    FileSize = byte_size(FileContent),
    create_file_to_be_linked(Worker1, SessId, File, FileContent),

    % Verify environment
    multi_provider_file_ops_test_base:verify_stats(Config, Dir, true),
    multi_provider_file_ops_test_base:verify(Config, fun(W) ->
        ?assertMatch({ok, #file_attr{type = ?REGULAR_FILE_TYPE, size = FileSize}},
            lfm_proxy:stat(W, SessId(W), {path, File}), Attempts)
    end),
    {ok, #file_attr{guid = SpaceGuid}} = ?assertMatch({ok, _}, lfm_proxy:stat(Worker1, SessId(Worker1), {path, <<"/", SpaceName/binary>>})),
    {ok, #file_attr{guid = FileGuid} = FileAttr} = ?assertMatch({ok, _}, lfm_proxy:stat(Worker1, SessId(Worker1), {path, File})),
    FileUuid = file_id:guid_to_uuid(FileGuid),
    ct:print("File created and verified"),

    % Create hardlink and verify its stats
    HardlinkName = generator:gen_name(),
    Hardlink = <<"/", SpaceName/binary, "/",  HardlinkName/binary>>,
    {ok, HardlinkAttr} = ?assertMatch({ok, _},
        lfm_proxy:make_link(Worker1, SessId(Worker1), Hardlink, FileGuid), Attempts),
    verify_hardlink_attrs(HardlinkName, HardlinkAttr, FileAttr, SpaceGuid),
    % stat ignores fully_replicated field so use attrs without it in asserts
    HardlinkAttrWithoutReplicationStatus = HardlinkAttr#file_attr{fully_replicated = undefined},
    ?assertEqual({ok, HardlinkAttrWithoutReplicationStatus},
        lfm_proxy:stat(Worker1, SessId(Worker1), {path, Hardlink})),
    ?assertEqual({ok, HardlinkAttrWithoutReplicationStatus},
        lfm_proxy:stat(Worker2, SessId(Worker2), {path, Hardlink}), Attempts),

    % Verify reading through hardlink
    verify_hardlink_read(Worker1, SessId, Hardlink, FileContent),
    verify_hardlink_read(Worker2, SessId, Hardlink, FileContent),

    % Delete hardlink - check that file is not deleted
    ?assertEqual(ok, lfm_proxy:unlink(Worker1, SessId(Worker1), {path, Hardlink})),
    ?assertEqual({error, enoent}, lfm_proxy:stat(Worker1, SessId(Worker1), {path, Hardlink})),
    ?assertMatch({ok, _}, lfm_proxy:stat(Worker1, SessId(Worker1), {path, File})),
    ?assertEqual({error, enoent}, lfm_proxy:stat(Worker2, SessId(Worker2), {path, Hardlink}), Attempts),
    ?assertMatch({ok, _}, lfm_proxy:stat(Worker2, SessId(Worker2), {path, File})),

    % Create second hardlink on other provider
    HardlinkName2 = generator:gen_name(),
    Hardlink2 = <<"/", SpaceName/binary, "/",  HardlinkName2/binary>>,
    ?assertMatch({ok, _}, lfm_proxy:make_link(Worker2, SessId(Worker2), Hardlink2, FileGuid), Attempts),
    {ok, #file_attr{guid = HardlinkGuid2}} =  ?assertMatch({ok, _},
        lfm_proxy:stat(Worker1, SessId(Worker1), {path, Hardlink2}), Attempts),
    HardlinkUuid2 = file_id:guid_to_uuid(HardlinkGuid2),

    % Change file through hardlink
    NewFileContent = write_hardlink(Worker2, SessId, Hardlink2, FileContent),

    % Check file content using hardlink
    verify_hardlink_read(Worker2, SessId, Hardlink2, NewFileContent),
    % Check file content
    verify_hardlink_read(Worker2, SessId, File, NewFileContent),

    % Delete file - verify that hardlink is not deleted and can be read
    ?assertEqual(ok, lfm_proxy:unlink(Worker2, SessId(Worker2), {path, File})),
    ?assertEqual({error, enoent}, lfm_proxy:stat(Worker2, SessId(Worker2), {path, File})),
    verify_hardlink_read(Worker2, SessId, Hardlink2, NewFileContent),

    % Verify hardlink and file on first provider
    ?assertEqual({error, enoent}, lfm_proxy:stat(Worker1, SessId(Worker1), {path, File}), Attempts),
    verify_hardlink_read(Worker1, SessId, Hardlink2, NewFileContent, Attempts),

    % Check if times and file_location documents have not been deleted
    verify_hardlink_times_and_location_documents(Worker1, Worker2, FileUuid, HardlinkUuid2),

    % Delete second hardlink
    ?assertEqual(ok, lfm_proxy:unlink(Worker2, SessId(Worker2), {path, Hardlink2})),
    ?assertEqual({error, enoent}, lfm_proxy:stat(Worker2, SessId(Worker2), {path, Hardlink2})),
    ?assertEqual({error, enoent}, lfm_proxy:stat(Worker1, SessId(Worker1), {path, Hardlink2}), Attempts),

    % Check if times and file_location documents have been deleted
    ?assertEqual({error, not_found}, ?GET_TIMES(Worker2, FileUuid)),
    ?assertEqual({error, not_found}, ?GET_LOCATION(Worker2, FileUuid)),
    ?assertEqual({error, not_found}, ?GET_TIMES(Worker1, FileUuid), Attempts),
    ?assertEqual({error, not_found}, ?GET_LOCATION(Worker1, FileUuid), Attempts).

first_reading_and_writing_via_hardlink_test(Config0) ->
    Attempts = 60,
    Config = multi_provider_file_ops_test_base:extend_config(Config0, <<"user1">>, {4, 0, 0, 2}, Attempts),
    SessId = ?config(session, Config),
    SpaceName = ?config(space_name, Config),
    Worker1 = ?config(worker1, Config),
    [Worker2 | _] = ?config(workers2, Config),

    % Setup environment
    Dir = <<"/", SpaceName/binary, "/",  (generator:gen_name())/binary>>,
    File = <<Dir/binary, "/", (generator:gen_name())/binary>>,
    File2 = <<Dir/binary, "/", (generator:gen_name())/binary>>,
    ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker1, SessId(Worker1), Dir)),
    FileContent = <<"1234567890abcd">>,
    create_file_to_be_linked(Worker1, SessId, File, FileContent),
    create_file_to_be_linked(Worker1, SessId, File2, FileContent),

    {ok, #file_attr{guid = SpaceGuid}} = ?assertMatch({ok, _}, lfm_proxy:stat(Worker1, SessId(Worker1), {path, <<"/", SpaceName/binary>>})),
    {ok, #file_attr{guid = FileGuid} = FileAttr} = ?assertMatch({ok, _}, lfm_proxy:stat(Worker1, SessId(Worker1), {path, File})),
    {ok, #file_attr{guid = FileGuid2} = FileAttr2} = ?assertMatch({ok, _}, lfm_proxy:stat(Worker1, SessId(Worker1), {path, File2})),
    FileUuid = file_id:guid_to_uuid(FileGuid),
    FileUuid2 = file_id:guid_to_uuid(FileGuid2),
    ct:print("Files created and verified"),

    % Create hardlinks and verify its stats
    HardlinkName = generator:gen_name(),
    Hardlink = <<"/", SpaceName/binary, "/",  HardlinkName/binary>>,
    {ok, #file_attr{guid = HardlinkGuid} = HardlinkAttr} = ?assertMatch({ok, _},
        lfm_proxy:make_link(Worker1, SessId(Worker1), Hardlink, FileGuid), Attempts),
    verify_hardlink_attrs(HardlinkName, HardlinkAttr, FileAttr, SpaceGuid),
    HardlinkUuid = file_id:guid_to_uuid(HardlinkGuid),
    HardlinkName2 = generator:gen_name(),
    Hardlink2 = <<"/", SpaceName/binary, "/",  HardlinkName2/binary>>,
    {ok, #file_attr{guid = HardlinkGuid2} = HardlinkAttr2} = ?assertMatch({ok, _},
        lfm_proxy:make_link(Worker1, SessId(Worker1), Hardlink2, FileGuid2), Attempts),
    HardlinkUuid2 = file_id:guid_to_uuid(HardlinkGuid2),
    verify_hardlink_attrs(HardlinkName2, HardlinkAttr2, FileAttr2, SpaceGuid),

    % Read/write hardlink on second provider without reading/writing file
    % (file_locations are not created for these files)
    verify_hardlink_read(Worker2, SessId, Hardlink, FileContent, Attempts),
    write_hardlink(Worker2, SessId, Hardlink2, FileContent, Attempts),

    verify_hardlink_times_and_location_documents(Worker1, Worker2, FileUuid, HardlinkUuid),
    verify_hardlink_times_and_location_documents(Worker1, Worker2, FileUuid2, HardlinkUuid2).

create_hardlink_to_hardlink_test(Config0) ->
    Attempts = 60,
    Config = multi_provider_file_ops_test_base:extend_config(Config0, <<"user1">>, {4, 0, 0, 2}, Attempts),
    SessId = ?config(session, Config),
    SpaceName = ?config(space_name, Config),
    Worker1 = ?config(worker1, Config),
    [Worker2 | _] = ?config(workers2, Config),

    % Setup environment
    Dir = <<"/", SpaceName/binary, "/",  (generator:gen_name())/binary>>,
    File = <<Dir/binary, "/", (generator:gen_name())/binary>>,
    ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker1, SessId(Worker1), Dir)),
    FileContent = <<"1234567890abcd">>,
    FileSize = byte_size(FileContent),
    create_file_to_be_linked(Worker1, SessId, File, FileContent),

    % Verify environment
    multi_provider_file_ops_test_base:verify_stats(Config, Dir, true),
    multi_provider_file_ops_test_base:verify(Config, fun(W) ->
        ?assertMatch({ok, #file_attr{type = ?REGULAR_FILE_TYPE, size = FileSize}},
            lfm_proxy:stat(W, SessId(W), {path, File}), Attempts)
    end),
    {ok, #file_attr{guid = SpaceGuid}} = ?assertMatch({ok, _}, lfm_proxy:stat(Worker1, SessId(Worker1), {path, <<"/", SpaceName/binary>>})),
    {ok, #file_attr{guid = FileGuid} = FileAttr} = ?assertMatch({ok, _}, lfm_proxy:stat(Worker1, SessId(Worker1), {path, File})),
    ct:print("File created and verified"),

    % Create hardlink and verify its stats
    HardlinkName = generator:gen_name(),
    Hardlink = <<"/", SpaceName/binary, "/",  HardlinkName/binary>>,
    {ok, HardlinkAttr} = ?assertMatch({ok, _},
        lfm_proxy:make_link(Worker1, SessId(Worker1), Hardlink, FileGuid), Attempts),
    verify_hardlink_attrs(HardlinkName, HardlinkAttr, FileAttr, SpaceGuid),

    % Create hardlink to hardlink and verify its stats
    HardlinkName2 = generator:gen_name(),
    Hardlink2 = <<"/", SpaceName/binary, "/",  HardlinkName2/binary>>,
    {ok, HardlinkAttr2} = ?assertMatch({ok, _},
        lfm_proxy:make_link(Worker1, SessId(Worker1), Hardlink2, HardlinkAttr#file_attr.guid), Attempts),
    verify_hardlink_attrs(HardlinkName2, HardlinkAttr2, FileAttr, SpaceGuid),

    % Verify reading through second hardlink
    verify_hardlink_read(Worker1, SessId, Hardlink2, FileContent),
    verify_hardlink_read(Worker2, SessId, Hardlink2, FileContent, Attempts),

    % Delete hardlinks and file and verify
    ?assertEqual(ok, lfm_proxy:unlink(Worker1, SessId(Worker1), {path, Hardlink})),
    ?assertEqual(ok, lfm_proxy:unlink(Worker1, SessId(Worker1), {path, Hardlink2})),
    ?assertEqual(ok, lfm_proxy:unlink(Worker1, SessId(Worker1), {path, File})),

    ?assertEqual({error, enoent}, lfm_proxy:stat(Worker1, SessId(Worker1), {path, Hardlink})),
    ?assertEqual({error, enoent}, lfm_proxy:stat(Worker1, SessId(Worker1), {path, Hardlink2})),
    ?assertEqual({error, enoent}, lfm_proxy:stat(Worker1, SessId(Worker1), {path, File})),
    ?assertEqual({error, enoent}, lfm_proxy:stat(Worker2, SessId(Worker2), {path, Hardlink}), Attempts),
    ?assertEqual({error, enoent}, lfm_proxy:stat(Worker2, SessId(Worker2), {path, Hardlink2}), Attempts),
    ?assertEqual({error, enoent}, lfm_proxy:stat(Worker2, SessId(Worker2), {path, File}), Attempts).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) -> multi_provider_file_ops_test_base:init_env(NewConfig) end,
    [{?LOAD_MODULES, [initializer, multi_provider_file_ops_test_base]}, {?ENV_UP_POSTHOOK, Posthook} | Config].

end_per_suite(Config) ->
    multi_provider_file_ops_test_base:teardown_env(Config).

init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 30}),
    lfm_proxy:init(Config).

end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).


%%%===================================================================
%%% Internal functions
%%%===================================================================

verify_hardlink_attrs(HardlinkName, HardlinkAttr, FileAttr, SpaceGuid) ->
    ?assertNotEqual(FileAttr#file_attr.guid, HardlinkAttr#file_attr.guid),
    ?assert(fslogic_uuid:is_hardlink_uuid(file_id:guid_to_uuid(HardlinkAttr#file_attr.guid))),
    ?assertNot(fslogic_uuid:is_hardlink_uuid(file_id:guid_to_uuid(FileAttr#file_attr.guid))),
    ?assertEqual(HardlinkName, HardlinkAttr#file_attr.name),
    ?assertEqual(SpaceGuid, HardlinkAttr#file_attr.parent_guid),

    ?assertEqual(FileAttr#file_attr.type, HardlinkAttr#file_attr.type),
    ?assertEqual(FileAttr#file_attr.mode, HardlinkAttr#file_attr.mode),
    ?assertEqual(FileAttr#file_attr.uid, HardlinkAttr#file_attr.uid),
    ?assertEqual(FileAttr#file_attr.gid, HardlinkAttr#file_attr.gid),
    ?assertEqual(FileAttr#file_attr.atime, HardlinkAttr#file_attr.atime),
    ?assertEqual(FileAttr#file_attr.mtime, HardlinkAttr#file_attr.mtime),
    ?assertEqual(FileAttr#file_attr.ctime, HardlinkAttr#file_attr.ctime),
    ?assertEqual(FileAttr#file_attr.size, HardlinkAttr#file_attr.size),
    ?assertEqual(FileAttr#file_attr.size, HardlinkAttr#file_attr.size),
    ?assertEqual(FileAttr#file_attr.shares, HardlinkAttr#file_attr.shares),
    ?assertEqual(FileAttr#file_attr.provider_id, HardlinkAttr#file_attr.provider_id),
    ?assertEqual(FileAttr#file_attr.owner_id, HardlinkAttr#file_attr.owner_id),
    ?assertEqual(true, HardlinkAttr#file_attr.fully_replicated).

verify_hardlink_read(Worker, SessId, HardlinkOrFile, FileContent) ->
    verify_hardlink_read(Worker, SessId, HardlinkOrFile, FileContent, 0).

verify_hardlink_read(Worker, SessId, HardlinkOrFile, FileContent, Attempts) ->
    FileSize = byte_size(FileContent),
    ?match({ok, #file_attr{size = FileSize}}, lfm_proxy:stat(Worker, SessId(Worker), {path, HardlinkOrFile}), Attempts),
    {ok, LinkHandle112} = ?assertMatch({ok, _}, lfm_proxy:open(Worker, SessId(Worker), {path, HardlinkOrFile}, rdwr)),
    ?assertEqual({ok, FileContent}, lfm_proxy:read(Worker, LinkHandle112, 0, 100)),
    ?assertEqual(ok, lfm_proxy:close(Worker, LinkHandle112)).

verify_hardlink_times_and_location_documents(Worker1, Worker2, FileUuid, HardlinkUuid) ->
    % Check if times and file_location documents exists
    ?assertMatch({ok, _}, ?GET_TIMES(Worker1, FileUuid)),
    {ok, #document{key = FileLocation1Uuid}} = ?assertMatch({ok, _}, ?GET_LOCATION(Worker1, FileUuid)),
    ?assertMatch({ok, _}, ?GET_TIMES(Worker2, FileUuid)),
    {ok, #document{key = FileLocation2Uuid}} = ?assertMatch({ok, _}, ?GET_LOCATION(Worker2, FileUuid)),

    % Check if times and file_location documents are returned for hardlink
    ?assertMatch({ok, #document{key = FileUuid}}, ?GET_TIMES(Worker1, HardlinkUuid)),
    ?assertMatch({ok, #document{key = FileLocation1Uuid}}, ?GET_LOCATION(Worker1, HardlinkUuid)),
    ?assertMatch({ok, #document{key = FileUuid}}, ?GET_TIMES(Worker2, HardlinkUuid)),
    ?assertMatch({ok, #document{key = FileLocation2Uuid}}, ?GET_LOCATION(Worker2, HardlinkUuid)),

    % Check if times and file_location documents have not been created for hardlink uuid,
    % Use datastore api to skip usage of effective key inside modules
    TimesCtx = #{model => times},
    FileLocationCtx = #{model => file_location},
    ?assertEqual({error, not_found}, rpc:call(Worker1, datastore_model, get, [TimesCtx, HardlinkUuid])),
    ?assertEqual({error, not_found}, rpc:call(Worker1, datastore_model, get, [FileLocationCtx, HardlinkUuid])),
    ?assertEqual({error, not_found}, rpc:call(Worker2, datastore_model, get, [TimesCtx, HardlinkUuid])),
    ?assertEqual({error, not_found}, rpc:call(Worker2, datastore_model, get, [FileLocationCtx, HardlinkUuid])).

create_file_to_be_linked(Worker, SessId, File, FileContent) ->
    ?assertMatch({ok, _}, lfm_proxy:create(Worker, SessId(Worker), File, 8#755)),
    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(Worker, SessId(Worker), {path, File}, rdwr)),
    FileSize = byte_size(FileContent),
    ?assertEqual({ok, FileSize}, lfm_proxy:write(Worker, Handle, 0, FileContent)),
    ?assertEqual(ok, lfm_proxy:close(Worker, Handle)).

write_hardlink(Worker, SessId, Hardlink, FileContent) ->
    write_hardlink(Worker, SessId, Hardlink, FileContent, 0).

write_hardlink(Worker, SessId, Hardlink, FileContent, Attempts) ->
    {ok, Ans} = ?match({ok, _}, begin
        {ok, LinkHandle11} = ?assertMatch({ok, _}, lfm_proxy:open(Worker, SessId(Worker), {path, Hardlink}, rdwr)),
        NewFileContent = <<FileContent/binary, "xyz">>,
        FileSize = byte_size(FileContent),
        ?assertEqual({ok, 3}, lfm_proxy:write(Worker, LinkHandle11, FileSize, <<"xyz">>)),
        ?assertEqual(ok, lfm_proxy:close(Worker, LinkHandle11)),
        {ok, NewFileContent}
    end, Attempts),
    Ans.