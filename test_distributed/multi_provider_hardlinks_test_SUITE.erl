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
    basic_test/1,
    first_access_performed_via_link_test/1,
    create_link_to_link_test/1
]).

-define(TEST_CASES, [
    basic_test,
    first_access_performed_via_link_test,
    create_link_to_link_test
]).

all() ->
    ?ALL(?TEST_CASES).

-define(GET_TIMES(Worker, Uuid), rpc:call(Worker, times, get, [Uuid])).
-define(GET_LOCATION(Worker, Uuid), rpc:call(Worker, file_location, get_local, [Uuid])).

%%%===================================================================
%%% Test functions
%%%===================================================================

% Test scenario is as follows:
% - creation of file and link via provider 1
% - verification of link access via both providers
% - deletion of link
% - verification that link is deleted and file is still accessible
% - creation of second link via provider 2
% - changing of file via link
% - deletion of file
% - verification that data still can be accessed via link
% - deletion of second link
basic_test(Config0) ->
    Attempts = 60,
    Config = multi_provider_file_ops_test_base:extend_config(Config0, <<"user1">>, {4, 0, 0, 2}, Attempts),
    SessId = ?config(session, Config),
    SpaceName = ?config(space_name, Config),
    Worker1 = ?config(worker1, Config),
    [Worker2 | _] = ?config(workers2, Config),

    % Setup test dir and file
    Dir = filename:join(["/", SpaceName, generator:gen_name()]),
    FileName = generator:gen_name(),
    File = filename:join(Dir, FileName),
    {ok, DirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker1, SessId(Worker1), Dir)),
    FileContent = <<"1234567890abcd">>,
    FileSize = byte_size(FileContent),
    FileGuid = file_ops_test_utils:create_file(Worker1, SessId(Worker1), DirGuid, FileName, FileContent),

    % Verify test dir and file
    multi_provider_file_ops_test_base:verify_stats(Config, Dir, true),
    multi_provider_file_ops_test_base:verify(Config, fun(W) ->
        ?assertMatch({ok, #file_attr{type = ?REGULAR_FILE_TYPE, size = FileSize}},
            lfm_proxy:stat(W, SessId(W), {path, File}), Attempts)
    end),
    {ok, #file_attr{guid = SpaceGuid}} = ?assertMatch({ok, _},
        lfm_proxy:stat(Worker1, SessId(Worker1), {path, filename:join("/", SpaceName)})),
    {ok, FileAttr} = ?assertMatch({ok, _}, lfm_proxy:stat(Worker1, SessId(Worker1), {path, File})),
    FileUuid = file_id:guid_to_uuid(FileGuid),

    % Create link and verify its stats
    {Link, LinkAttr} = make_and_verify_link(Config, FileGuid, SpaceGuid, FileAttr),
    % stat ignores fully_replicated field so use attrs without it in asserts
    LinkAttrWithoutReplicationStatus = LinkAttr#file_attr{fully_replicated = undefined},
    ?assertEqual({ok, LinkAttrWithoutReplicationStatus},
        lfm_proxy:stat(Worker1, SessId(Worker1), {path, Link})),
    ?assertEqual({ok, LinkAttrWithoutReplicationStatus},
        lfm_proxy:stat(Worker2, SessId(Worker2), {path, Link}), Attempts),

    % Verify reading through link
    assert_size_and_content_identical(Worker1, SessId, Link, FileContent),
    assert_size_and_content_identical(Worker2, SessId, Link, FileContent),

    % Delete link - check that file is not deleted
    ?assertEqual(ok, lfm_proxy:unlink(Worker1, SessId(Worker1), {path, Link})),
    ?assertEqual({error, enoent}, lfm_proxy:stat(Worker1, SessId(Worker1), {path, Link})),
    ?assertMatch({ok, _}, lfm_proxy:stat(Worker1, SessId(Worker1), {path, File})),
    ?assertEqual({error, enoent}, lfm_proxy:stat(Worker2, SessId(Worker2), {path, Link}), Attempts),
    ?assertMatch({ok, _}, lfm_proxy:stat(Worker2, SessId(Worker2), {path, File})),

    % Create second link on other provider
    LinkName2 = generator:gen_name(),
    Link2 = filename:join(["/", SpaceName, LinkName2]),
    {ok, #file_attr{guid = LinkGuid2}} = ?assertMatch({ok, _},
        lfm_proxy:make_link(Worker2, SessId(Worker2), Link2, FileGuid), Attempts),
    ?assertMatch({ok, _}, lfm_proxy:stat(Worker1, SessId(Worker1), {path, Link2}), Attempts),
    LinkUuid2 = file_id:guid_to_uuid(LinkGuid2),

    % Change file through link
    NewFileContent = write(Worker2, SessId, Link2, FileContent),

    % Check file content using link
    assert_size_and_content_identical(Worker2, SessId, Link2, NewFileContent),
    % Check file content
    assert_size_and_content_identical(Worker2, SessId, File, NewFileContent),

    % Delete file - verify that link is not deleted and can be read
    ?assertEqual(ok, lfm_proxy:unlink(Worker2, SessId(Worker2), {path, File})),
    ?assertEqual({error, enoent}, lfm_proxy:stat(Worker2, SessId(Worker2), {path, File})),
    assert_size_and_content_identical(Worker2, SessId, Link2, NewFileContent),

    % Verify link and file on first provider
    ?assertEqual({error, enoent}, lfm_proxy:stat(Worker1, SessId(Worker1), {path, File}), Attempts),
    assert_size_and_content_identical(Worker1, SessId, Link2, NewFileContent, Attempts),

    % Check if times and file_location documents have not been deleted
    verify_link_times_and_location_documents(Worker1, Worker2, FileUuid, LinkUuid2),

    % Delete second link
    ?assertEqual(ok, lfm_proxy:unlink(Worker2, SessId(Worker2), {path, Link2})),
    ?assertEqual({error, enoent}, lfm_proxy:stat(Worker2, SessId(Worker2), {path, Link2})),
    ?assertEqual({error, enoent}, lfm_proxy:stat(Worker1, SessId(Worker1), {path, Link2}), Attempts),

    % Check if times and file_location documents have been deleted
    ?assertEqual({error, not_found}, ?GET_TIMES(Worker2, FileUuid)),
    ?assertMatch({error, not_found}, ?GET_LOCATION(Worker2, FileUuid)),
    ?assertEqual({error, not_found}, ?GET_TIMES(Worker1, FileUuid), Attempts),
    ?assertMatch({error, not_found}, ?GET_LOCATION(Worker1, FileUuid), Attempts).

% Test scenario is as follows:
% - creation of file two files and two links via provider 1
% - reading link 1 via provider 2 (note that file 1 has never been accessed via provider 2)
% - writing link 2 via provider 2 (note that file 2 has never been accessed via provider 2)
first_access_performed_via_link_test(Config0) ->
    Attempts = 60,
    Config = multi_provider_file_ops_test_base:extend_config(Config0, <<"user1">>, {4, 0, 0, 2}, Attempts),
    SessId = ?config(session, Config),
    SpaceName = ?config(space_name, Config),
    Worker1 = ?config(worker1, Config),
    [Worker2 | _] = ?config(workers2, Config),

    % Setup environment
    Dir = filename:join(["/", SpaceName, generator:gen_name()]),
    FileName = generator:gen_name(),
    File = filename:join(Dir, FileName),
    FileName2 = generator:gen_name(),
    File2 = filename:join(Dir, FileName),
    {ok, DirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker1, SessId(Worker1), Dir)),
    FileContent = <<"1234567890abcd">>,
    FileGuid = file_ops_test_utils:create_file(Worker1, SessId(Worker1), DirGuid, FileName, FileContent),
    FileGuid2 = file_ops_test_utils:create_file(Worker1, SessId(Worker1), DirGuid, FileName2, FileContent),

    {ok, #file_attr{guid = SpaceGuid}} = ?assertMatch({ok, _},
        lfm_proxy:stat(Worker1, SessId(Worker1), {path, filename:join("/", SpaceName)})),
    {ok, FileAttr} = ?assertMatch({ok, _}, lfm_proxy:stat(Worker1, SessId(Worker1), {path, File})),
    {ok, FileAttr2} = ?assertMatch({ok, _}, lfm_proxy:stat(Worker1, SessId(Worker1), {path, File2})),
    FileUuid = file_id:guid_to_uuid(FileGuid),
    FileUuid2 = file_id:guid_to_uuid(FileGuid2),

    % Create links and verify its stats
    {Link, #file_attr{guid = LinkGuid}} = make_and_verify_link(Config, FileGuid, SpaceGuid, FileAttr),
    LinkUuid = file_id:guid_to_uuid(LinkGuid),
    {Link2, #file_attr{guid = LinkGuid2}} = make_and_verify_link(Config, FileGuid2, SpaceGuid, FileAttr2),
    LinkUuid2 = file_id:guid_to_uuid(LinkGuid2),

    % Read/write link on second provider without reading/writing file
    % (file_locations are not created for these files)
    assert_size_and_content_identical(Worker2, SessId, Link, FileContent, Attempts),
    write(Worker2, SessId, Link2, FileContent, Attempts),

    verify_link_times_and_location_documents(Worker1, Worker2, FileUuid, LinkUuid),
    verify_link_times_and_location_documents(Worker1, Worker2, FileUuid2, LinkUuid2).

create_link_to_link_test(Config0) ->
    Attempts = 60,
    Config = multi_provider_file_ops_test_base:extend_config(Config0, <<"user1">>, {4, 0, 0, 2}, Attempts),
    SessId = ?config(session, Config),
    SpaceName = ?config(space_name, Config),
    Worker1 = ?config(worker1, Config),
    [Worker2 | _] = ?config(workers2, Config),

    % Setup test dir and file
    Dir = filename:join(["/", SpaceName, generator:gen_name()]),
    FileName = generator:gen_name(),
    File = filename:join(Dir, FileName),
    {ok, DirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker1, SessId(Worker1), Dir)),
    FileContent = <<"1234567890abcd">>,
    FileSize = byte_size(FileContent),
    FileGuid = file_ops_test_utils:create_file(Worker1, SessId(Worker1), DirGuid, FileName, FileContent),

    % Verify test dir and file
    multi_provider_file_ops_test_base:verify_stats(Config, Dir, true),
    multi_provider_file_ops_test_base:verify(Config, fun(W) ->
        ?assertMatch({ok, #file_attr{type = ?REGULAR_FILE_TYPE, size = FileSize}},
            lfm_proxy:stat(W, SessId(W), {path, File}), Attempts)
    end),
    {ok, #file_attr{guid = SpaceGuid}} = ?assertMatch({ok, _},
        lfm_proxy:stat(Worker1, SessId(Worker1), {path, filename:join("/", SpaceName)})),
    {ok, FileAttr} = ?assertMatch({ok, _}, lfm_proxy:stat(Worker1, SessId(Worker1), {path, File})),

    % Create link and verify its stats
    {Link, #file_attr{guid = LinkGuid}} = make_and_verify_link(Config, FileGuid, SpaceGuid, FileAttr),

    % Create link to link and verify its stats
    {Link2, _LinkAttr2} = make_and_verify_link(Config, LinkGuid, SpaceGuid, FileAttr),

    % Verify reading through second link
    assert_size_and_content_identical(Worker1, SessId, Link2, FileContent),
    assert_size_and_content_identical(Worker2, SessId, Link2, FileContent, Attempts),

    % Delete links and file and verify
    ?assertEqual(ok, lfm_proxy:unlink(Worker1, SessId(Worker1), {path, Link})),
    ?assertEqual(ok, lfm_proxy:unlink(Worker1, SessId(Worker1), {path, Link2})),
    ?assertEqual(ok, lfm_proxy:unlink(Worker1, SessId(Worker1), {path, File})),

    ?assertEqual({error, enoent}, lfm_proxy:stat(Worker1, SessId(Worker1), {path, Link})),
    ?assertEqual({error, enoent}, lfm_proxy:stat(Worker1, SessId(Worker1), {path, Link2})),
    ?assertEqual({error, enoent}, lfm_proxy:stat(Worker1, SessId(Worker1), {path, File})),
    ?assertEqual({error, enoent}, lfm_proxy:stat(Worker2, SessId(Worker2), {path, Link}), Attempts),
    ?assertEqual({error, enoent}, lfm_proxy:stat(Worker2, SessId(Worker2), {path, Link2}), Attempts),
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

make_and_verify_link(Config, FileGuid, SpaceGuid, AttrToVerify) ->
    SessId = ?config(session, Config),
    SpaceName = ?config(space_name, Config),
    Attempts = ?config(attempts, Config),
    Worker1 = ?config(worker1, Config),
    LinkName = generator:gen_name(),
    Link = filename:join(["/", SpaceName, LinkName]),
    {ok, LinkAttr} = ?assertMatch({ok, _},
        lfm_proxy:make_link(Worker1, SessId(Worker1), Link, FileGuid), Attempts),
    verify_link_attrs(LinkName, LinkAttr, AttrToVerify, SpaceGuid),
    {Link, LinkAttr}.

verify_link_attrs(LinkName, LinkAttr, FileAttr, SpaceGuid) ->
    ?assertNotEqual(FileAttr#file_attr.guid, LinkAttr#file_attr.guid),
    ?assert(fslogic_file_id:is_link_uuid(file_id:guid_to_uuid(LinkAttr#file_attr.guid))),
    ?assertNot(fslogic_file_id:is_link_uuid(file_id:guid_to_uuid(FileAttr#file_attr.guid))),
    ?assertEqual(LinkName, LinkAttr#file_attr.name),
    ?assertEqual(SpaceGuid, LinkAttr#file_attr.parent_guid),

    ?assertEqual(FileAttr#file_attr.type, LinkAttr#file_attr.type),
    ?assertEqual(FileAttr#file_attr.mode, LinkAttr#file_attr.mode),
    ?assertEqual(FileAttr#file_attr.uid, LinkAttr#file_attr.uid),
    ?assertEqual(FileAttr#file_attr.gid, LinkAttr#file_attr.gid),
    ?assertEqual(FileAttr#file_attr.size, LinkAttr#file_attr.size),
    ?assertEqual(FileAttr#file_attr.size, LinkAttr#file_attr.size),
    ?assertEqual(FileAttr#file_attr.shares, LinkAttr#file_attr.shares),
    ?assertEqual(FileAttr#file_attr.provider_id, LinkAttr#file_attr.provider_id),
    ?assertEqual(FileAttr#file_attr.owner_id, LinkAttr#file_attr.owner_id),
    ?assertEqual(true, LinkAttr#file_attr.fully_replicated),

    % Time can be changed by event after file creation
    ?assert(FileAttr#file_attr.atime =< LinkAttr#file_attr.atime),
    ?assert(FileAttr#file_attr.mtime =< LinkAttr#file_attr.mtime),
    ?assert(FileAttr#file_attr.ctime =< LinkAttr#file_attr.ctime).

assert_size_and_content_identical(Worker, SessId, LinkOrFile, FileContent) ->
    assert_size_and_content_identical(Worker, SessId, LinkOrFile, FileContent, 1).

assert_size_and_content_identical(Worker, SessId, LinkOrFile, FileContent, Attempts) ->
    FileSize = byte_size(FileContent),
    ?assertMatch({ok, #file_attr{size = FileSize}}, lfm_proxy:stat(Worker, SessId(Worker), {path, LinkOrFile}), Attempts),
    {ok, LinkHandle112} = ?assertMatch({ok, _}, lfm_proxy:open(Worker, SessId(Worker), {path, LinkOrFile}, rdwr)),
    ?assertEqual({ok, FileContent}, lfm_proxy:read(Worker, LinkHandle112, 0, 100)),
    ?assertEqual(ok, lfm_proxy:close(Worker, LinkHandle112)).

verify_link_times_and_location_documents(Worker1, Worker2, FileUuid, LinkUuid) ->
    % Check if times and file_location documents exists
    ?assertMatch({ok, _}, ?GET_TIMES(Worker1, FileUuid)),
    {ok, #document{key = FileLocation1Uuid}} = ?assertMatch({ok, _}, ?GET_LOCATION(Worker1, FileUuid)),
    ?assertMatch({ok, _}, ?GET_TIMES(Worker2, FileUuid)),
    {ok, #document{key = FileLocation2Uuid}} = ?assertMatch({ok, _}, ?GET_LOCATION(Worker2, FileUuid)),

    % Check if times and file_location documents are returned for link
    ?assertMatch({ok, #document{key = FileUuid}}, ?GET_TIMES(Worker1, LinkUuid)),
    ?assertMatch({ok, #document{key = FileLocation1Uuid}}, ?GET_LOCATION(Worker1, LinkUuid)),
    ?assertMatch({ok, #document{key = FileUuid}}, ?GET_TIMES(Worker2, LinkUuid)),
    ?assertMatch({ok, #document{key = FileLocation2Uuid}}, ?GET_LOCATION(Worker2, LinkUuid)),

    % Check if times and file_location documents have not been created for link uuid,
    % Use datastore api to skip usage of referenced key inside modules
    TimesCtx = #{model => times},
    FileLocationCtx = #{model => file_location},
    ?assertEqual({error, not_found}, rpc:call(Worker1, datastore_model, get, [TimesCtx, LinkUuid])),
    ?assertEqual({error, not_found}, rpc:call(Worker1, datastore_model, get, [FileLocationCtx, LinkUuid])),
    ?assertEqual({error, not_found}, rpc:call(Worker2, datastore_model, get, [TimesCtx, LinkUuid])),
    ?assertEqual({error, not_found}, rpc:call(Worker2, datastore_model, get, [FileLocationCtx, LinkUuid])).

write(Worker, SessId, Link, FileContent) ->
    write(Worker, SessId, Link, FileContent, 1).

write(Worker, SessId, Link, FileContent, Attempts) ->
    {ok, Ans} = ?assertMatch({ok, _},
        try
            {ok, LinkHandle11} = lfm_proxy:open(Worker, SessId(Worker), {path, Link}, rdwr),
            NewFileContent = <<FileContent/binary, "xyz">>,
            FileSize = byte_size(FileContent),
            {ok, 3} = lfm_proxy:write(Worker, LinkHandle11, FileSize, <<"xyz">>),
            ok = lfm_proxy:close(Worker, LinkHandle11),
            {ok, NewFileContent}
        catch
            E1:E2 ->
                {error, {E1, E2}}
        end, Attempts),
    Ans.