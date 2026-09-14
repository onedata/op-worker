%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Assertions on the state of files kept on a provider's storage, for tests that
%%% verify how logical file operations are reflected on the storage backend.
%%%
%%% Files are addressed by a path relative to the space directory on the storage,
%%% never by an absolute one - resolving it is left to the assertions, so that the
%%% way the storage is accessed stays an implementation detail of this header.
%%%
%%% NOTE: the storage is inspected through the file system of the node it is mounted
%%% on (see storage_test_utils), hence the assertions only work for POSIX compatible
%%% storages.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(STORAGE_TEST_HRL).
-define(STORAGE_TEST_HRL, 1).

-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("kernel/include/file.hrl").

-define(DEFAULT_STORAGE_ASSERT_ATTEMPTS, 60).

% Full #file_info.mode of a storage file/directory with the given permissions
% (permission bits extended with the file type bits)
-define(FILE_MODE(Perms), Perms bor 8#100000).
-define(DIR_MODE(Perms), Perms bor 8#40000).


-define(assertStorageFileContent(Node, SpaceId, RelPath, ExpContent),
    ?assertStorageFileContent(Node, SpaceId, RelPath, ExpContent, ?DEFAULT_STORAGE_ASSERT_ATTEMPTS)
).
-define(assertStorageFileContent(Node, SpaceId, RelPath, ExpContent, Attempts), (fun() ->
    __Path = storage_test_utils:file_path(Node, SpaceId, RelPath),
    ?assertEqual({ok, ExpContent}, storage_test_utils:read_file(Node, __Path), Attempts),
    ?assertEqual({ok, regular, byte_size(ExpContent)}, case storage_test_utils:read_file_info(Node, __Path) of
        {ok, #file_info{type = __Type, size = __Size}} -> {ok, __Type, __Size};
        __Error -> __Error
    end, Attempts),
    ok
end)()).


-define(assertNoStorageFile(Node, SpaceId, RelPath),
    ?assertNoStorageFile(Node, SpaceId, RelPath, ?DEFAULT_STORAGE_ASSERT_ATTEMPTS)
).
-define(assertNoStorageFile(Node, SpaceId, RelPath, Attempts), (fun() ->
    __Path = storage_test_utils:file_path(Node, SpaceId, RelPath),
    ?assertEqual({error, ?ENOENT}, storage_test_utils:read_file(Node, __Path), Attempts),
    ?assertEqual({error, ?ENOENT}, storage_test_utils:read_file_info(Node, __Path), Attempts),
    ok
end)()).


-define(assertStorageDirChildren(Node, SpaceId, RelPath, ExpChildNames),
    ?assertStorageDirChildren(Node, SpaceId, RelPath, ExpChildNames, ?DEFAULT_STORAGE_ASSERT_ATTEMPTS)
).
-define(assertStorageDirChildren(Node, SpaceId, RelPath, ExpChildNames, Attempts), (fun() ->
    __Path = storage_test_utils:file_path(Node, SpaceId, RelPath),
    __ExpChildren = lists:sort([binary_to_list(__Name) || __Name <- ExpChildNames]),
    ?assertEqual({ok, directory}, case storage_test_utils:read_file_info(Node, __Path) of
        {ok, #file_info{type = __Type}} -> {ok, __Type};
        __Error -> __Error
    end, Attempts),
    ?assertEqual({ok, __ExpChildren}, case storage_test_utils:list_dir(Node, __Path) of
        {ok, __Children} -> {ok, lists:sort(__Children)};
        __Error -> __Error
    end, Attempts),
    ok
end)()).


-define(assertNoStorageDir(Node, SpaceId, RelPath),
    ?assertNoStorageDir(Node, SpaceId, RelPath, ?DEFAULT_STORAGE_ASSERT_ATTEMPTS)
).
-define(assertNoStorageDir(Node, SpaceId, RelPath, Attempts), (fun() ->
    __Path = storage_test_utils:file_path(Node, SpaceId, RelPath),
    ?assertEqual({error, ?ENOENT}, storage_test_utils:list_dir(Node, __Path), Attempts),
    ?assertEqual({error, ?ENOENT}, storage_test_utils:read_file_info(Node, __Path), Attempts),
    ok
end)()).

-endif.
