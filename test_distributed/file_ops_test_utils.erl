%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utils used to create files and dirs and verify their state
%%% e.g. on other provider or after node restart.
%%% @end
%%%-------------------------------------------------------------------
-module(file_ops_test_utils).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

%% API
-export([create_files_and_dirs/5, verify_files_and_dirs/4, test_read_operations_on_error/4]).

-define(FILE_DATA, <<"1234567890abcd">>).

-record(test_data, {
    dir_guids,
    file_guids
}).

%%%===================================================================
%%% API
%%%===================================================================

create_files_and_dirs(Worker, SessId, ParentUuid, DirsNum, _FilesNum) ->
    DirGuids = lists:map(fun(_) ->
        Dir = generator:gen_name(),
        {ok, DirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker, SessId, ParentUuid, Dir, 8#755)),
        DirGuid
    end, lists:seq(1, DirsNum)),

    FileDataSize = size(?FILE_DATA),
    FileGuids = lists:map(fun(_) ->
        File = generator:gen_name(),
        {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(Worker, SessId, ParentUuid, File, 8#755)),
        {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(Worker, SessId, {guid, FileGuid}, rdwr)),
        ?assertMatch({ok, FileDataSize}, lfm_proxy:write(Worker, Handle, 0, ?FILE_DATA)),
        ?assertEqual(ok, lfm_proxy:close(Worker, Handle)),
        FileGuid
    end, lists:seq(1, 0)), % TODO VFS-6873 - create `FilesNum` files when rtransfer problems are fixed

    #test_data{dir_guids = DirGuids, file_guids = FileGuids}.

verify_files_and_dirs(Worker, SessId, #test_data{dir_guids = DirGuids, file_guids = FileGuids}, Attempts) ->
    lists:foreach(fun(Dir) ->
        ?assertMatch({ok, #file_attr{type = ?DIRECTORY_TYPE}},
            lfm_proxy:stat(Worker, SessId, {guid, Dir}), Attempts)
    end, DirGuids),

    FileDataSize = size(?FILE_DATA),
    lists:foreach(fun(File) ->
        ?assertMatch({ok, #file_attr{type = ?REGULAR_FILE_TYPE, size = FileDataSize}},
            lfm_proxy:stat(Worker, SessId, {guid, File}), Attempts)
    end, FileGuids),

    lists:foreach(fun(File) ->
        ?assertEqual(FileDataSize,
            begin
                {ok, Handle} = lfm_proxy:open(Worker, SessId, {guid, File}, rdwr),
                try
                    {ok, ReadData} = lfm_proxy:check_size_and_read(Worker, Handle, 0, 1000), % use check_size_and_read because of null helper usage
                    size(ReadData) % compare size because of null helper usage
                catch
                    E1:E2 -> {E1, E2}
                after
                    lfm_proxy:close(Worker, Handle)
                end
            end, Attempts)
    end, FileGuids).

test_read_operations_on_error(Worker, SessId, #test_data{dir_guids = DirGuids, file_guids = FileGuids}, ErrorType) ->
    lists:foreach(fun(Dir) ->
        ?assertMatch({error, ErrorType}, lfm_proxy:stat(Worker, SessId, {guid, Dir})),
        ?assertMatch({error, ErrorType}, lfm_proxy:get_children_attrs(Worker, SessId, {guid, Dir}, 0, 100))
    end, DirGuids),

    lists:foreach(fun(File) ->
        ?assertMatch({error, ErrorType}, lfm_proxy:stat(Worker, SessId, {guid, File})),
        ?assertMatch({error, ErrorType}, lfm_proxy:open(Worker, SessId, {guid, File}, rdwr))
    end, FileGuids).