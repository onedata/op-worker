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
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

%% API
-export([create_dir/4, create_file/4, create_file/5]).
-export([create_files_and_dirs/5, verify_files_and_dirs/4, test_read_operations_on_error/4]).
-export([write_byte_to_file/4, empty_write_to_file/4]).
-export([get_sparse_file_content/2]).

-define(FILE_DATA, <<"1234567890abcd">>).

-record(test_data, {
    dir_guids,
    file_guids
}).

%%%===================================================================
%%% API
%%%===================================================================

create_files_and_dirs(Worker, SessId, ParentGuid, DirsNum, FilesNum) ->
    DirGuids = lists:map(fun(_) ->
        create_dir(Worker, SessId, ParentGuid, generator:gen_name())
    end, lists:seq(1, DirsNum)),

    FileGuids = lists:map(fun(_) ->
        create_file(Worker, SessId, ParentGuid, generator:gen_name())
    end, lists:seq(1, FilesNum)),

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


create_dir(Worker, SessId, ParentGuid, DirName) ->
    {ok, DirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(Worker, SessId, ParentGuid, DirName, ?DEFAULT_DIR_PERMS)),
    DirGuid.

create_file(Worker, SessId, ParentGuid, FileName) ->
    create_file(Worker, SessId, ParentGuid, FileName, ?FILE_DATA).

create_file(Worker, SessId, ParentGuid, FileName, FileContentSize) when is_integer(FileContentSize) ->
    create_file(Worker, SessId, ParentGuid, FileName, crypto:strong_rand_bytes(FileContentSize));
create_file(Worker, SessId, ParentGuid, FileName, FileContent) when is_binary(FileContent) ->
    {ok, {FileGuid, Handle}} =
        ?assertMatch({ok, _}, lfm_proxy:create_and_open(Worker, SessId, ParentGuid, FileName, ?DEFAULT_FILE_PERMS)),
    FileContentSize = size(FileContent),
    ?assertMatch({ok, FileContentSize}, lfm_proxy:write(Worker, Handle, 0, FileContent)),
    ?assertEqual(ok, lfm_proxy:close(Worker, Handle)),
    FileGuid.

write_byte_to_file(W, SessId, FileGuid, Offset) ->
    {ok, Handle} = lfm_proxy:open(W, SessId, {guid, FileGuid}, rdwr),
    ?assertEqual({ok, 1}, lfm_proxy:write(W, Handle, Offset, <<"t">>)),
    ?assertEqual(ok, lfm_proxy:close(W, Handle)).

empty_write_to_file(W, SessId, FileGuid, Offset) ->
    {ok, Handle} = lfm_proxy:open(W, SessId, {guid, FileGuid}, rdwr),
    ?assertEqual({ok, 0}, lfm_proxy:write(W, Handle, Offset, <<>>)),
    ?assertEqual(ok, lfm_proxy:close(W, Handle)).

get_sparse_file_content([{_, _} | _] = ExpectedBlocks, FileSize) ->
    Blocks = lists:foldl(fun({_ProviderId, ProviderBlocks}, Acc) -> Acc ++ ProviderBlocks end, [], ExpectedBlocks),
    get_sparse_file_content(lists:usort(Blocks), FileSize);
get_sparse_file_content(Blocks, FileSize) ->
    get_sparse_file_content(Blocks, FileSize, 0).

get_sparse_file_content([], FileSize, CurrentPos) ->
    binary:copy(<<"\0">>, FileSize - CurrentPos);
get_sparse_file_content([[Offset, Size] | Blocks], FileSize, Offset) ->
    <<(binary:copy(<<"t">>, Size))/binary, (get_sparse_file_content(Blocks, FileSize, Offset + Size))/binary>>;
get_sparse_file_content(Blocks, FileSize, CurrentPos) ->
    <<"\0", (get_sparse_file_content(Blocks, FileSize, CurrentPos + 1))/binary>>.