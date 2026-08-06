%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of listing files through lfm. Two APIs are covered:
%%%  * the children of a single directory (get_children, get_children_attrs),
%%%    addressed either by offset and limit, by a pagination token or by a start
%%%    index;
%%%  * a whole subtree at once (get_files_recursively), optionally narrowed to a
%%%    path prefix.
%%%
%%% The bodies are shared by file_lfm_posix_test_SUITE and file_lfm_s3_test_SUITE.
%%% Each test works within its own, randomly named directory in the space, which
%%% the suites empty between test cases.
%%% @end
%%%-------------------------------------------------------------------
-module(file_lfm_listing_tests).
-author("Bartosz Walkowicz").

% This module indirectly includes eunit.hrl, whose parse transform would
% otherwise auto-export every arity 0 function named *_test - clashing with
% the export list below.
-define(EUNIT_NOAUTO, 1).

-include("file/file_lfm_test.hrl").
-include("global_definitions.hrl").
-include("modules/fslogic/file_attr.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

%% tests
-export([
    get_children_attrs_of_empty_dir_test/0,
    get_children_attrs_with_zero_limit_test/0,
    get_children_attrs_with_zero_offset_test/0,
    get_children_attrs_with_non_zero_offset_test/0,
    get_children_attrs_with_limit_greater_than_dir_size_test/0,
    get_children_attrs_with_offset_beyond_dir_size_test/0,

    get_children_with_pagination_token_test/0,
    get_children_with_pagination_token_and_not_full_last_batch_test/0,
    get_children_attrs_with_pagination_token_test/0,
    get_children_attrs_with_pagination_token_and_not_full_last_batch_test/0,

    get_children_with_start_index_test/0,
    get_children_attrs_with_start_index_test/0,

    get_children_attrs_with_xattrs_test/0,

    get_recursive_file_list_test/0,
    get_recursive_file_list_with_prefix_test/0,
    get_recursive_file_list_with_inaccessible_paths_test/0,
    get_recursive_file_list_with_xattrs_test/0,
    get_recursive_file_list_spanning_multiple_internal_batches_test/0
]).

%% performance tests (the parameter specs live here so that both suites share them)
-export([
    ls_test_performance_spec/0, ls_test_base/1,
    ls_with_stats_test_performance_spec/0, ls_with_stats_test_base/1
]).

%% suite setup helpers
-export([ensure_default_fold_cache_timeout/0]).

-define(REPEATS, 3).
-define(SUCCESS_RATE, 100).

-define(PAGINATION_TOKEN_BATCH_SIZE, 3).

% Listing with a pagination token is resumed after the fold cache backing it has
% been dropped - this is how long it takes for the expired entries to be flushed.
-define(FOLD_CACHE_FLUSH_TIME_SECONDS, 5).

% The value the provider is deployed with (see fold_cache_timeout in rel/files/app.config).
% Used only to undo a zeroed timeout left behind by an interrupted run - see
% ensure_default_fold_cache_timeout/0.
-define(DEPLOYED_FOLD_CACHE_TIMEOUT_MILLIS, 30000).


%%%===================================================================
%%% Tests of listing dir children by offset and limit
%%%===================================================================


get_children_attrs_of_empty_dir_test() ->
    assert_children_attrs_listed(0, #{offset => 0, limit => 10}, 0).


get_children_attrs_with_zero_limit_test() ->
    assert_children_attrs_listed(10, #{offset => 0, limit => 0}, 0).


get_children_attrs_with_zero_offset_test() ->
    assert_children_attrs_listed(5, #{offset => 0, limit => 5}, 5).


get_children_attrs_with_non_zero_offset_test() ->
    assert_children_attrs_listed(5, #{offset => 2, limit => 3}, 3).


get_children_attrs_with_limit_greater_than_dir_size_test() ->
    assert_children_attrs_listed(5, #{offset => 0, limit => 10}, 5).


get_children_attrs_with_offset_beyond_dir_size_test() ->
    assert_children_attrs_listed(5, #{offset => 5, limit => 10}, 0).


%% @private
-spec assert_children_attrs_listed(
    non_neg_integer(),
    #{offset := file_listing:offset(), limit := file_listing:limit()},
    non_neg_integer()
) ->
    ok.
assert_children_attrs_listed(DirSize, ListingOpts, ExpectedCount) ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    {DirPath, Files} = create_dir_with_files(Node, SessId, DirSize),

    {ok, Listed, _} = ?assertMatch({ok, _, _}, lfm_proxy:get_children_attrs(
        Node, SessId, {path, DirPath},
        ListingOpts#{tune_for_large_continuous_listing => false}
    )),
    ?assertEqual(ExpectedCount, length(Listed)),
    ExpectedNames = lists:sublist(Files, maps:get(offset, ListingOpts) + 1, ExpectedCount),
    ?assertEqual(ExpectedNames, get_listed_names(Listed)),

    ok.


%%%===================================================================
%%% Tests of listing dir children with a pagination token
%%%===================================================================


get_children_with_pagination_token_test() ->
    assert_listed_in_batches_with_pagination_token(children, 12).


get_children_with_pagination_token_and_not_full_last_batch_test() ->
    assert_listed_in_batches_with_pagination_token(children, 10).


get_children_attrs_with_pagination_token_test() ->
    assert_listed_in_batches_with_pagination_token(children_attrs, 12).


get_children_attrs_with_pagination_token_and_not_full_last_batch_test() ->
    assert_listed_in_batches_with_pagination_token(children_attrs, 10).


%% @private
-spec assert_listed_in_batches_with_pagination_token(children | children_attrs, pos_integer()) ->
    ok.
assert_listed_in_batches_with_pagination_token(ListingType, DirSize) ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    {DirPath, Files} = create_dir_with_files(Node, SessId, DirSize),

    BatchSize = ?PAGINATION_TOKEN_BATCH_SIZE,
    ListBatch = fun(Offset, IsLast, PaginationToken) ->
        assert_batch_listed(
            ListingType, Node, SessId, DirPath, Files,
            Offset, max(0, min(DirSize - Offset, BatchSize)), IsLast, PaginationToken
        )
    end,

    PaginationToken1 = ListBatch(0, false, undefined),

    % The listing must survive the expiration of the datastore fold cache it
    % started with - the pagination token alone is enough to resume it.
    DefaultFoldCacheTimeout = get_fold_cache_timeout(Node),
    set_fold_cache_timeout(Node, 0),
    PaginationToken2 = ListBatch(BatchSize, false, PaginationToken1),
    timer:sleep(timer:seconds(?FOLD_CACHE_FLUSH_TIME_SECONDS)),
    % restore the default timeout so that the tokens of the remaining batches do not expire
    set_fold_cache_timeout(Node, DefaultFoldCacheTimeout),

    PaginationToken3 = ListBatch(2 * BatchSize, false, PaginationToken2),
    ListBatch(3 * BatchSize, true, PaginationToken3),

    ok.


%% @private
-spec assert_batch_listed(
    children | children_attrs, node(), session:id(), file_meta:path(), [file_meta:name()],
    non_neg_integer(), non_neg_integer(), boolean(), file_listing:pagination_token() | undefined
) ->
    file_listing:pagination_token().
assert_batch_listed(
    ListingType, Node, SessId, DirPath, AllFiles,
    Offset, ExpectedCount, IsLast, PaginationToken
) ->
    BaseListingOpts = case PaginationToken of
        undefined -> #{tune_for_large_continuous_listing => true};
        _ -> #{pagination_token => PaginationToken}
    end,
    ListingOpts = BaseListingOpts#{limit => ?PAGINATION_TOKEN_BATCH_SIZE},

    {ok, Listed, NextPaginationToken} = ?assertMatch({ok, _, _}, case ListingType of
        children -> lfm_proxy:get_children(Node, SessId, {path, DirPath}, ListingOpts);
        children_attrs -> lfm_proxy:get_children_attrs(Node, SessId, {path, DirPath}, ListingOpts)
    end),
    ?assertEqual(lists:sublist(AllFiles, Offset + 1, ExpectedCount), get_listed_names(Listed)),
    ?assertEqual(IsLast, file_listing:is_finished(NextPaginationToken)),

    NextPaginationToken.


%%%===================================================================
%%% Tests of listing dir children from a start index
%%%===================================================================


get_children_with_start_index_test() ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    {DirPath, Files} = create_dir_with_files(Node, SessId, 10),

    ListFrom = fun(StartIndex, Offset, Limit, FilesOffset, ExpectedCount) ->
        {ok, Listed, _} = ?assertMatch({ok, _, _}, lfm_proxy:get_children(
            Node, SessId, {path, DirPath},
            #{
                offset => Offset,
                limit => Limit,
                index => file_listing:build_index(StartIndex),
                tune_for_large_continuous_listing => false
            }
        )),
        ?assertEqual(lists:sublist(Files, FilesOffset + 1, ExpectedCount), get_listed_names(Listed)),
        case Listed of
            [_ | _] -> lists:last(get_listed_names(Listed));
            [] -> undefined
        end
    end,

    % list all files in chunks (use 0 offset for each chunk)
    StartIndex1 = ListFrom(undefined, 0, 4, 0, 4),
    StartIndex2 = ListFrom(StartIndex1, 0, 4, 3, 4),
    StartIndex3 = ListFrom(StartIndex2, 0, 3, 6, 3),
    StartIndex4 = ListFrom(StartIndex3, 0, 3, 8, 2),
    ?assertEqual(lists:last(Files), StartIndex4),

    % list with a start index and a positive offset
    StartIndex5 = ListFrom(undefined, 4, 2, 4, 2),
    StartIndex6 = ListFrom(StartIndex5, 2, 4, 7, 3),
    ?assertEqual(lists:last(Files), StartIndex6),

    % list with a start index and an offset beyond the number of files
    ListFrom(StartIndex5, 20, 4, 0, 0),

    % list with a start index and a negative offset
    StartIndex7 = ListFrom(StartIndex5, -2, 4, 3, 4),
    ListFrom(StartIndex7, -10, 6, 0, 6),

    ok.


get_children_attrs_with_start_index_test() ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    {DirPath, Files} = create_dir_with_files(Node, SessId, 10),

    ListFrom = fun(StartIndex, Offset, Limit, FilesOffset, ExpectedCount) ->
        {ok, Listed, _} = ?assertMatch({ok, _, _}, lfm_proxy:get_children_attrs(
            Node, SessId, {path, DirPath},
            #{
                offset => Offset,
                limit => Limit,
                index => StartIndex,
                inclusive => true,
                tune_for_large_continuous_listing => false
            },
            [index | ?ONECLIENT_FILE_ATTRS]
        )),
        ?assertEqual(lists:sublist(Files, FilesOffset + 1, ExpectedCount), get_listed_names(Listed)),
        case Listed of
            [_ | _] -> (lists:last(Listed))#file_attr.index;
            [] -> undefined
        end
    end,

    LastFileIndex = file_listing:build_index(
        lists:last(Files), oct_background:get_provider_id(?PROVIDER_SELECTOR)
    ),

    % list all files in chunks (use 0 offset for each chunk)
    StartIndex1 = ListFrom(undefined, 0, 4, 0, 4),
    StartIndex2 = ListFrom(StartIndex1, 0, 4, 3, 4),
    StartIndex3 = ListFrom(StartIndex2, 0, 3, 6, 3),
    StartIndex4 = ListFrom(StartIndex3, 0, 3, 8, 2),
    ?assertEqual(LastFileIndex, StartIndex4),

    % list with a start index and a positive offset
    StartIndex5 = ListFrom(undefined, 4, 2, 4, 2),
    StartIndex6 = ListFrom(StartIndex5, 2, 4, 7, 3),
    ?assertEqual(LastFileIndex, StartIndex6),

    % list with a start index and an offset beyond the number of files
    ListFrom(StartIndex5, 20, 4, 0, 0),

    % list with a start index and a negative offset
    StartIndex7 = ListFrom(StartIndex5, -2, 4, 3, 4),
    ListFrom(StartIndex7, -10, 6, 0, 6),

    ok.


%%%===================================================================
%%% Tests of listing with xattrs
%%%===================================================================


get_children_attrs_with_xattrs_test() ->
    assert_xattrs_listed(fun(Node, SessId, DirPath, XattrNames) ->
        {ok, Listed, _} = ?assertMatch({ok, _, _}, lfm_proxy:get_children_attrs(
            Node, SessId, {path, DirPath},
            #{offset => 0, limit => 10, tune_for_large_continuous_listing => false},
            [{xattrs, XattrNames}]
        )),
        Listed
    end).


get_recursive_file_list_with_xattrs_test() ->
    assert_xattrs_listed(fun(Node, SessId, DirPath, XattrNames) ->
        {ok, Listed, _, _} = ?assertMatch({ok, _, _, _}, lfm_proxy:get_files_recursively(
            Node, SessId, {path, DirPath},
            #{offset => 0, limit => 10, tune_for_large_continuous_listing => false},
            [{xattrs, XattrNames}]
        )),
        Listed
    end).


%% @private
-spec assert_xattrs_listed(fun((node(), session:id(), file_meta:path(), [binary()]) ->
    [lfm_attrs:file_attributes()])) -> ok.
assert_xattrs_listed(ListFun) ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    {DirPath, Files} = create_dir_with_files(Node, SessId, 5),

    lists:foreach(fun(FileName) ->
        ?assertEqual(ok, lfm_proxy:set_xattr(
            Node, SessId, {path, filename:join([DirPath, FileName])},
            #xattr{name = <<"name">>, value = FileName}
        ))
    end, Files),

    Listed = ListFun(Node, SessId, DirPath, [<<"name">>, <<"undefined">>]),

    lists:foreach(fun({#file_attr{xattrs = Xattrs}, FileName}) ->
        ?assertEqual(FileName, maps:get(<<"name">>, Xattrs, undefined)),
        ?assertEqual(undefined, maps:get(<<"undefined">>, Xattrs, defined))
    end, lists:zip(Listed, Files)),

    ok.


%%%===================================================================
%%% Tests of recursive listing
%%%===================================================================


get_recursive_file_list_test() ->
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    lfm_ct:set_default_context(Node, SessId),

    RootDirName = generator:gen_name(),
    RootDirPath = file_lfm_test_utils:build_space_path(RootDirName),
    RootDirGuid = lfm_ct:mkdir(RootDirPath),

    [NestedDirName | _] = DirNames = lists:sort(lists_utils:generate(fun generator:gen_name/0, 4)),
    CommonFileNames = lists:sort(lists_utils:generate(fun generator:gen_name/0, 8)),
    {[NestedDirGuid | _], AllExpectedFiles} = lists:foldl(fun(DirName, {DirsAcc, FilesAcc}) ->
        DirGuid = lfm_ct:mkdir(filename:join([RootDirPath, DirName])),
        lfm_ct:mkdir(filename:join([RootDirPath, DirName, <<"empty_dir">>])),
        {DirsAcc ++ [DirGuid], FilesAcc ++ lists:map(fun(FileName) ->
            {lfm_ct:create(filename:join([RootDirPath, DirName, FileName])), filename:join([DirName, FileName])}
        end, CommonFileNames)}
    end, {[], []}, DirNames),

    lfm_test_utils:assert_recursive_listing_from_each_start_after(
        Node, SessId, RootDirGuid, AllExpectedFiles),

    % list from the space dir - the RootDirName prefix keeps this listing
    % independent of anything else residing in the space
    AllExpectedFilesInSpace = lists:map(fun({Guid, Path}) ->
        {Guid, filename:join([RootDirName, Path])}
    end, AllExpectedFiles),
    SpaceDirGuid = file_lfm_test_utils:get_space_dir_guid(),
    lfm_test_utils:assert_recursive_listing_from_each_start_after(
        Node, SessId, SpaceDirGuid, RootDirName, AllExpectedFilesInSpace),

    AllExpectedFilesInNestedDir = lists:filtermap(fun({Guid, Path}) ->
        case filepath_utils:is_descendant(Path, NestedDirName) of
            {true, RelPath} -> {true, {Guid, RelPath}};
            _ -> false
        end
    end, AllExpectedFiles),
    lfm_test_utils:assert_recursive_listing_from_each_start_after(
        Node, SessId, NestedDirGuid, AllExpectedFilesInNestedDir),

    % listing a regular file returns just this file
    {FileNum, {FileGuid, FilePath}} = lists_utils:random_element(
        lists:zip(lists:seq(1, length(AllExpectedFiles)), AllExpectedFiles)),
    ?assertMatch({ok, [{FileGuid, <<".">>}], _, undefined},
        lfm_test_utils:get_files_recursively(Node, SessId, ?FILE_REF(FileGuid), #{limit => 1})),

    % the file that start_after points to may no longer exist
    lfm_ct:unlink(FileGuid),
    ExpectedTail = lists:nthtail(FileNum, AllExpectedFiles),
    ?assertMatch({ok, ExpectedTail, _, _},
        lfm_test_utils:get_files_recursively(Node, SessId, ?FILE_REF(RootDirGuid),
            #{start_after_path => FilePath, limit => length(AllExpectedFiles)})),

    ok.


get_recursive_file_list_with_prefix_test() ->
    %% Created file structure:
    %%                      Space
    %%                         |
    %%                      RootDir
    %%                      / | | \
    %%                     /  | |  \
    %%                    /  /   \  \
    %%                  aa  bb  bbb cc
    %%
    %%  In each directory Dir [aa, bb, bbb, cc]:
    %%                      Dir  -------
    %%                    /  |  \       |
    %%                   /   |   \    1..8 reg files with random names
    %%                  /    |    \
    %%                 /     |     \
    %%       nested_dir  empty_dir  <<Dir/binary, "_file">>
    %%           |
    %%   <<Dir/binary, "_file">>

    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    lfm_ct:set_default_context(Node, SessId),

    RootDirPath = file_lfm_test_utils:build_space_path(generator:gen_name()),
    RootDirGuid = lfm_ct:mkdir(RootDirPath),

    DirNames = [<<"aa">>, <<"bb">>, <<"bbb">>, <<"cc">>],
    CommonFileNames = lists:sort(lists_utils:generate(fun generator:gen_name/0, 8)),
    NestedDirName = <<"nested_dir">>,

    AllFilesUnsorted = lists:foldl(fun(DirName, FilesAcc) ->
        lfm_ct:mkdir(filename:join([RootDirPath, DirName])),
        lfm_ct:mkdir(filename:join([RootDirPath, DirName, <<"empty_dir">>])),
        lfm_ct:mkdir(filename:join([RootDirPath, DirName, NestedDirName])),
        FileName = <<DirName/binary, "_file">>,
        FileGuid = lfm_ct:create(filename:join([RootDirPath, DirName, FileName])),
        NestedFileGuid = lfm_ct:create(filename:join([RootDirPath, DirName, NestedDirName, FileName])),
        FilesAcc ++ lists:map(fun(F) ->
            {lfm_ct:create(filename:join([RootDirPath, DirName, F])), filename:join([DirName, F])}
        end, CommonFileNames) ++ [
            {FileGuid, filename:join([DirName, FileName])},
            {NestedFileGuid, filename:join([DirName, NestedDirName, FileName])}
        ]
    end, [], DirNames),
    AllFiles = lists:sort(fun({_G1, P1}, {_G2, P2}) -> P1 < P2 end, AllFilesUnsorted),

    NestedPrefixes = lists:flatmap(fun(DirName) -> [
        filename:join([DirName, NestedDirName]),
        filename:join([DirName, NestedDirName, DirName])
    ] end, DirNames),

    lists:foreach(fun(Prefix) ->
        AllExpectedFiles = lists:filter(fun({_Guid, Path}) ->
            str_utils:binary_starts_with(Path, Prefix)
        end, AllFiles),
        lfm_test_utils:assert_recursive_listing_from_each_start_after(
            Node, SessId, RootDirGuid, Prefix, AllExpectedFiles)
    end, DirNames ++ NestedPrefixes ++ [<<"a">>, <<"b">>, <<"c">>, <<"d">>]),

    ok.


get_recursive_file_list_with_inaccessible_paths_test() ->
    Node = file_lfm_test_utils:get_node(),
    OwnerSessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    OtherUserSessId = file_lfm_test_utils:get_session_id(?OTHER_USER_SELECTOR),
    lfm_ct:set_default_context(Node, OwnerSessId),

    % the inaccessible dir must sort before the accessible one so that it is
    % encountered first while listing
    [EaccesDirName, AccessibleDirName] = lists:sort(lists_utils:generate(fun generator:gen_name/0, 2)),

    RootDirPath = file_lfm_test_utils:build_space_path(generator:gen_name()),
    RootDirGuid = lfm_ct:mkdir(RootDirPath),
    StandaloneEaccesDirGuid = lfm_ct:mkdir(file_lfm_test_utils:build_space_path(EaccesDirName), 8#700),

    lfm_ct:mkdir(filename:join([RootDirPath, AccessibleDirName])),
    lfm_ct:mkdir(filename:join([RootDirPath, EaccesDirName]), 8#700),

    FileNames = lists:sort(lists_utils:generate(fun generator:gen_name/0, 8)),
    AllFiles = lists:map(fun(FileName) ->
        lfm_ct:create(filename:join([RootDirPath, EaccesDirName, FileName])),
        {
            lfm_ct:create(filename:join([RootDirPath, AccessibleDirName, FileName])),
            filename:join([AccessibleDirName, FileName])
        }
    end, FileNames),

    % inaccessible paths are counted towards the limit, so the result should not contain the last file
    ExpectedFilesWithinLimit = lists:sublist(AllFiles, length(AllFiles) - 1),
    ?assertMatch({ok, ExpectedFilesWithinLimit, [EaccesDirName], _},
        lfm_test_utils:get_files_recursively(Node, OtherUserSessId, ?FILE_REF(RootDirGuid),
            #{limit => length(AllFiles)})),
    ?assertMatch({ok, AllFiles, [EaccesDirName], _},
        lfm_test_utils:get_files_recursively(Node, OtherUserSessId, ?FILE_REF(RootDirGuid),
            #{limit => length(AllFiles) + 1})),
    ?assertMatch({ok, AllFiles, [], _},
        lfm_test_utils:get_files_recursively(Node, OtherUserSessId, ?FILE_REF(RootDirGuid),
            #{start_after_path => EaccesDirName, limit => length(AllFiles)})),
    ?assertMatch({error, ?EACCES},
        lfm_test_utils:get_files_recursively(Node, OtherUserSessId, ?FILE_REF(StandaloneEaccesDirGuid),
            #{limit => length(AllFiles)})),

    ok.


get_recursive_file_list_spanning_multiple_internal_batches_test() ->
    % the internal children batch limit is 1000 (see recursive_listing
    % ?LIST_RECURSIVE_BATCH_SIZE), so list with a limit larger than that
    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),
    lfm_ct:set_default_context(Node, SessId),

    RootDirPath = file_lfm_test_utils:build_space_path(generator:gen_name()),
    RootDirGuid = lfm_ct:mkdir(RootDirPath),

    FilesNum = 1500,
    GuidsAndPaths = lists:map(fun(Num) ->
        FileName = integer_to_binary(Num),
        {lfm_ct:create(filename:join([RootDirPath, FileName])), FileName}
    end, lists:seq(1, FilesNum)),
    ExpectedFiles = lists:sort(fun({_G1, P1}, {_G2, P2}) -> P1 =< P2 end, GuidsAndPaths),

    ?assertMatch({ok, ExpectedFiles, _, _},
        lfm_test_utils:get_files_recursively(Node, SessId, ?FILE_REF(RootDirGuid),
            #{limit => FilesNum})),

    ok.


%%%===================================================================
%%% Performance tests
%%%===================================================================


ls_test_performance_spec() -> [
    {repeats, ?REPEATS},
    {success_rate, ?SUCCESS_RATE},
    {parameters, [
        [{name, dir_size_multiplier}, {value, 1}, {description, "Parameter for dir size tuning."}]
    ]},
    {description, "Tests ls operation"},
    {config, [{name, medium_dir},
        {parameters, [
            [{name, dir_size_multiplier}, {value, 1}]
        ]},
        {description, ""}
    ]},
    {config, [{name, large_dir},
        {parameters, [
            [{name, dir_size_multiplier}, {value, 10}]
        ]},
        {description, ""}
    ]}
].


ls_test_base(Config) ->
    DirSizeMultiplier = ?config(dir_size_multiplier, Config),

    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),

    DirPath = file_lfm_test_utils:build_space_path(generator:gen_name()),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, SessId, DirPath)),

    AssertListed = fun(BaseOffset, BaseLimit, ExpectedNames) ->
        Offset = BaseOffset * DirSizeMultiplier,
        Limit = BaseLimit * DirSizeMultiplier,
        {ok, Listed1} = ?assertMatch({ok, _},
            lfm_proxy:get_children(Node, SessId, {path, DirPath}, Offset, Limit)),
        {ok, Listed2} = ?assertMatch({ok, _},
            lfm_proxy:get_children(Node, SessId, {path, DirPath}, 0, Offset)),
        {ok, Listed3} = ?assertMatch({ok, _},
            lfm_proxy:get_children(Node, SessId, {path, DirPath}, Offset + Limit, length(ExpectedNames))),

        ?assertEqual(
            {
                min(Limit, max(length(ExpectedNames) - Offset, 0)),
                min(Offset, length(ExpectedNames)),
                max(length(ExpectedNames) - Offset - Limit, 0)
            },
            {length(Listed1), length(Listed2), length(Listed3)}
        ),
        ?assertEqual(ExpectedNames, lists:sort(get_listed_names(Listed1 ++ Listed2 ++ Listed3)))
    end,

    FileNames = lists:sort(lists_utils:generate(fun generator:gen_name/0, 30 * DirSizeMultiplier)),
    lists:foreach(fun(FileName) ->
        ?assertMatch({ok, _}, lfm_proxy:create(Node, SessId, <<DirPath/binary, "/", FileName/binary>>))
    end, FileNames),

    AssertListed(0, 30, FileNames),
    AssertListed(0, 4, FileNames),
    AssertListed(0, 15, FileNames),
    AssertListed(0, 23, FileNames),
    AssertListed(12, 11, FileNames),
    AssertListed(20, 3, FileNames),
    AssertListed(22, 8, FileNames),
    AssertListed(0, 40, FileNames),
    AssertListed(30, 10, FileNames),
    AssertListed(35, 5, FileNames),

    DirNames = lists_utils:generate(fun generator:gen_name/0, 30 * DirSizeMultiplier),
    lists:foreach(fun(DirName) ->
        ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, SessId, <<DirPath/binary, "/", DirName/binary>>))
    end, DirNames),
    AllNames = lists:sort(FileNames ++ DirNames),

    AssertListed(0, 60, AllNames),
    AssertListed(0, 23, AllNames),
    AssertListed(12, 11, AllNames),
    AssertListed(20, 3, AllNames),
    AssertListed(22, 8, AllNames),
    AssertListed(22, 23, AllNames),
    AssertListed(45, 5, AllNames),
    AssertListed(45, 15, AllNames),
    AssertListed(10, 35, AllNames),

    {FinalLsTime, _} = measure_execution_time(fun() ->
        AssertListed(0, 80, AllNames)
    end),

    #parameter{name = final_ls_time, value = FinalLsTime, unit = "us",
        description = "Time of last full dir listing"}.


ls_with_stats_test_performance_spec() -> [
    {repeats, ?REPEATS},
    {success_rate, ?SUCCESS_RATE},
    {parameters, [
        [{name, proc_num}, {value, 1}, {description, "Number of threads used during the test."}],
        [{name, dir_level}, {value, 10}, {description, "Level of test directory."}],
        [{name, dirs_num_per_proc}, {value, 10}, {description, "Number of dirs tested by single thread."}]
    ]},
    {description, "Tests performance of ls with getting stats operation"},
    {config, [{name, low_level_single_thread_small_dir},
        {parameters, [
            [{name, dir_level}, {value, 1}],
            [{name, dirs_num_per_proc}, {value, 5}]
        ]},
        {description, ""}
    ]},
    {config, [{name, low_level_single_thread_large_dir},
        {parameters, [
            [{name, dir_level}, {value, 1}],
            [{name, dirs_num_per_proc}, {value, 100}]
        ]},
        {description, ""}
    ]},
    {config, [{name, low_level_10_threads_large_dir},
        {parameters, [
            [{name, proc_num}, {value, 10}],
            [{name, dir_level}, {value, 1}],
            [{name, dirs_num_per_proc}, {value, 10}]
        ]},
        {description, ""}
    ]},
    {config, [{name, high_level_single_thread_small_dir},
        {parameters, [
            [{name, dir_level}, {value, 100}],
            [{name, dirs_num_per_proc}, {value, 5}]
        ]},
        {description, ""}
    ]},
    {config, [{name, high_level_single_thread_large_dir},
        {parameters, [
            [{name, dir_level}, {value, 100}],
            [{name, dirs_num_per_proc}, {value, 100}]
        ]},
        {description, ""}
    ]},
    {config, [{name, high_level_10_threads_large_dir},
        {parameters, [
            [{name, proc_num}, {value, 10}],
            [{name, dir_level}, {value, 100}],
            [{name, dirs_num_per_proc}, {value, 10}]
        ]},
        {description, ""}
    ]}
].


ls_with_stats_test_base(Config) ->
    DirLevel = ?config(dir_level, Config),
    ProcNum = ?config(proc_num, Config),
    DirsNumPerProc = ?config(dirs_num_per_proc, Config),

    Node = file_lfm_test_utils:get_node(),
    SessId = file_lfm_test_utils:get_session_id(?USER_SELECTOR),

    % Generate names of dirs in the test directory tree
    [DeepestDirPath | _] = TreeDirPathsReversed = lists:foldl(fun(_, [ParentPath | _] = Acc) ->
        [<<ParentPath/binary, "/", (generator:gen_name())/binary>> | Acc]
    end, [file_lfm_test_utils:build_space_path()], lists:seq(1, DirLevel)),
    [_SpacePath | TreeDirPaths] = lists:reverse(TreeDirPathsReversed),

    {CreateTreeTime, _} = measure_execution_time(fun() ->
        lists:foreach(fun(DirPath) ->
            ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, SessId, DirPath))
        end, TreeDirPaths)
    end),

    % Create dirs at the last level of the tree (the ones to be listed)
    {CreateDirsTime, _} = measure_execution_time(fun() ->
        lists_utils:pforeach(fun(_) ->
            lists:foreach(fun(_) ->
                DirPath = <<DeepestDirPath/binary, "/", (generator:gen_name())/binary>>,
                ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, SessId, DirPath))
            end, lists:seq(1, DirsNumPerProc))
        end, lists:seq(1, ProcNum))
    end),

    {LsTime, ListedDirs} = measure_execution_time(fun() ->
        {ok, Listed} = ?assertMatch({ok, _}, lfm_proxy:get_children(
            Node, SessId, {path, DeepestDirPath}, 0, DirsNumPerProc * ProcNum)),
        ?assertEqual(DirsNumPerProc * ProcNum, length(Listed)),
        Listed
    end),

    {StatTime, _} = measure_execution_time(fun() ->
        lists_utils:pforeach(fun({DirGuid, _DirName}) ->
            ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(Node, SessId, ?FILE_REF(DirGuid)))
        end, ListedDirs, ProcNum)
    end),

    [
        #parameter{name = create_tree_time, value = CreateTreeTime, unit = "us",
            description = "Time of test tree creation"},
        #parameter{name = create_dirs_time, value = CreateDirsTime, unit = "us",
            description = "Time of test dirs creation"},
        #parameter{name = ls_time, value = LsTime, unit = "us",
            description = "Time of ls operation"},
        #parameter{name = stat_time, value = StatTime, unit = "us",
            description = "Time of all stat operations"},
        #parameter{name = ls_stat_time, value = LsTime + StatTime, unit = "us",
            description = "Total time of ls and all stat operations"}
    ].


%%%===================================================================
%%% Suite setup helpers
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% The pagination token tests zero the fold cache timeout for the duration of a
%% single listing and restore it themselves. A run interrupted in between would
%% leave the timeout at 0 on a reused deployment, expiring the fold cache of
%% every later listing - hence this defensive reset, to be called from the
%% init_per_suite posthook.
%% @end
%%--------------------------------------------------------------------
-spec ensure_default_fold_cache_timeout() -> ok.
ensure_default_fold_cache_timeout() ->
    lists:foreach(fun(Node) ->
        case get_fold_cache_timeout(Node) of
            0 -> set_fold_cache_timeout(Node, ?DEPLOYED_FOLD_CACHE_TIMEOUT_MILLIS);
            _ -> ok
        end
    end, oct_background:get_provider_nodes(?PROVIDER_SELECTOR)).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec create_dir_with_files(node(), session:id(), non_neg_integer()) ->
    {file_meta:path(), [file_meta:name()]}.
create_dir_with_files(Node, SessId, FilesNum) ->
    DirPath = file_lfm_test_utils:build_space_path(generator:gen_name()),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, SessId, DirPath)),

    FileNames = lists:sort(lists_utils:generate(fun generator:gen_name/0, FilesNum)),
    lists:foreach(fun(FileName) ->
        ?assertMatch({ok, _}, lfm_proxy:create(Node, SessId, <<DirPath/binary, "/", FileName/binary>>))
    end, FileNames),

    {DirPath, FileNames}.


%% @private
-spec get_listed_names([lfm_attrs:file_attributes()] | [{file_id:file_guid(), file_meta:name()}]) ->
    [file_meta:name()].
get_listed_names(Listed) ->
    lists:map(fun
        (#file_attr{name = Name}) -> Name;
        ({_Guid, Name}) -> Name
    end, Listed).


%% @private
-spec get_fold_cache_timeout(node()) -> time:millis().
get_fold_cache_timeout(Node) ->
    {ok, Timeout} = test_utils:get_env(Node, ?CLUSTER_WORKER_APP_NAME, fold_cache_timeout),
    Timeout.


%% @private
-spec set_fold_cache_timeout(node(), time:millis()) -> ok.
set_fold_cache_timeout(Node, Timeout) ->
    test_utils:set_env(Node, ?CLUSTER_WORKER_APP_NAME, fold_cache_timeout, Timeout).


%% @private
-spec measure_execution_time(fun(() -> Result)) -> {time:micros(), Result}.
measure_execution_time(Fun) ->
    Stopwatch = stopwatch:start(),
    Result = Fun(),
    {stopwatch:read_micros(Stopwatch), Result}.
