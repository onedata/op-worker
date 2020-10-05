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
-export([create_files_and_dirs/5, verify_files_and_dirs/4]).
-export([init_per_suite/2]).

-define(FILE_DATA, <<"1234567890abcd">>).

%%%===================================================================
%%% API
%%%===================================================================

create_files_and_dirs(Worker, SessId, ParentUuid, DirsNum, FilesNum) ->
    Dirs = lists:map(fun(_) ->
        Dir = generator:gen_name(),
        {ok, DirGuid} = ?assertMatch({ok, _}, rpc:call(Worker, lfm, mkdir, [SessId, ParentUuid, Dir, 8#755], 5000)),
        DirGuid
    end, lists:seq(1, DirsNum)),

    Files = lists:map(fun(_) ->
        File = generator:gen_name(),
        {ok, FileGuid} = ?assertMatch({ok, _}, rpc:call(Worker, lfm, create, [SessId, ParentUuid, File, 8#755], 5000)),
        {ok, Handle} = ?assertMatch({ok, _}, rpc:call(Worker, lfm, open, [SessId, {guid, FileGuid}, rdwr], 5000)),
        {ok, NewHandle, _} = ?assertMatch({ok, _, _}, rpc:call(Worker, lfm, write,  [Handle, 0, ?FILE_DATA], 5000)),
        ?assertEqual(ok, rpc:call(Worker, lfm, fsync, [NewHandle], 35000)),
        ?assertEqual(ok, rpc:call(Worker, lfm, release, [NewHandle], 5000)),
        FileGuid
    end, lists:seq(1, FilesNum)),

    {Dirs, Files}.

verify_files_and_dirs(Worker, SessId, {Dirs, Files}, Attempts) ->
    lists:foreach(fun(Dir) ->
        ?assertMatch({ok, #file_attr{type = ?DIRECTORY_TYPE}},
            rpc:call(Worker, lfm, stat, [SessId, {guid, Dir}], 1000), Attempts)
    end, Dirs),

    FileDataSize = size(?FILE_DATA),
    lists:foreach(fun(File) ->
        ?assertMatch({ok, #file_attr{type = ?REGULAR_FILE_TYPE, size = FileDataSize}},
            rpc:call(Worker, lfm, stat, [SessId, {guid, File}], 1000), Attempts)
    end, Files),

    lists:foreach(fun(File) ->
        ?assertEqual(FileDataSize,
            begin
                {ok, Handle} = rpc:call(Worker, lfm, open, [SessId, {guid, File}, rdwr]),
                try
                    {ok, _, ReadData} = rpc:call(Worker, lfm, check_size_and_read, [Handle, 0, 1000]), % use check_size_and_read because of null helper usage
                    size(ReadData) % compare size because of null helper usage
                catch
                    E1:E2 -> {E1, E2}
                after
                    rpc:call(Worker, lfm, release, [Handle])
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