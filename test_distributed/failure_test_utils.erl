%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utils used by ct_onenv tests that emulate node failures and other errors.
%%% @end
%%%-------------------------------------------------------------------
-module(failure_test_utils).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

%% API
-export([kill_nodes/2, restart_nodes/2]).
-export([create_files_and_dirs/5, verify_files_and_dirs/4]).

-export([init_per_suite/2]).

-define(FILE_DATA, <<"1234567890abcd">>).

%%%===================================================================
%%% API
%%%===================================================================

kill_nodes(_Config, []) ->
    ok;
kill_nodes(Config, [Node | Nodes]) ->
    kill_nodes(Config, Node),
    kill_nodes(Config, Nodes);
kill_nodes(Config, Node) ->
    ok = onenv_test_utils:kill_node(Config, Node),
    ?assertEqual({badrpc, nodedown}, rpc:call(Node, oneprovider, get_id, []), 10).

restart_nodes(Config, Nodes) when is_list(Nodes) ->
    lists:foreach(fun(Node) ->
        ok = onenv_test_utils:start_node(Config, Node)
    end, Nodes),

    lists:foreach(fun(Node) ->
        ?assertMatch({ok, _}, rpc:call(Node, provider_auth, get_provider_id, []), 60),
        {ok, _} = rpc:call(Node, mock_manager, start, []),
        ?assertEqual(true, rpc:call(Node, gs_channel_service, is_connected, []), 30)
    end, Nodes),

    UpdatedConfig = provider_onenv_test_utils:setup_sessions(proplists:delete(sess_id, Config)),
    lfm_proxy:init(UpdatedConfig, true, Nodes);
restart_nodes(Config, Node) ->
    restart_nodes(Config, [Node]).

create_files_and_dirs(Worker, SessId, ParentUuid, DirsNum, FilesNum) ->
    Dirs = lists:map(fun(_) ->
        Dir = generator:gen_name(),
        {ok, DirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker, SessId, ParentUuid, Dir, 8#755)),
        DirGuid
    end, lists:seq(1, DirsNum)),

    FileDataSize = size(?FILE_DATA),
    Files = lists:map(fun(_) ->
        File = generator:gen_name(),
        {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(Worker, SessId, ParentUuid, File, 8#755)),
        {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(Worker, SessId, {guid, FileGuid}, rdwr)),
        ?assertMatch({ok, FileDataSize}, lfm_proxy:write(Worker, Handle, 0, ?FILE_DATA)),
        ?assertEqual(ok, lfm_proxy:close(Worker, Handle)),
        FileGuid
    end, lists:seq(1, FilesNum)), % TODO VFS-6873 - create `FilesNum` files when rtransfer problems are fixed

    {Dirs, Files}.

verify_files_and_dirs(Worker, SessId, {Dirs, Files}, Attempts) ->
    lists:foreach(fun(Dir) ->
        ?assertMatch({ok, #file_attr{type = ?DIRECTORY_TYPE}},
            lfm_proxy:stat(Worker, SessId, {guid, Dir}), Attempts)
    end, Dirs),

    FileDataSize = size(?FILE_DATA),
    lists:foreach(fun(File) ->
        ?assertMatch({ok, #file_attr{type = ?REGULAR_FILE_TYPE, size = FileDataSize}},
            lfm_proxy:stat(Worker, SessId, {guid, File}), Attempts)
    end, Files),

    lists:foreach(fun(File) ->
        ?assertEqual(FileDataSize,
            begin
                {ok, Handle} = lfm_proxy:open(Worker, SessId, {guid, File}, rdwr),
                try
                    {ok, _, ReadData} = lfm_proxy:check_size_and_read(Worker, Handle, 0, 1000), % use check_size_and_read because of null helper usage
                    size(ReadData) % compare size because of null helper usage
                catch
                    E1:E2 -> {E1, E2}
                after
                    lfm_proxy:close(Worker, Handle)
                end
            end, Attempts)
    end, Files).

%%%===================================================================
%%% SetUp and TearDown helpers
%%%===================================================================

init_per_suite(Config, Scenario) ->
    Posthook = fun(NewConfig) ->
        provider_onenv_test_utils:initialize(NewConfig)
    end,
    test_config:set_many(Config, [
        {set_onenv_scenario, [Scenario]}, % name of yaml file in test_distributed/onenv_scenarios
        {set_posthook, Posthook}
    ]).