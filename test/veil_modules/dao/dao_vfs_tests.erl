%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of dao_vfs module.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================
-module(dao_vfs_tests).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include_lib("veil_modules/dao/dao.hrl").
-include_lib("files_common.hrl").
-endif.

-ifdef(TEST).

file_descriptor_test_() ->
    {foreach, fun setup/0, fun teardown/1,
        [fun save_descriptor/0, fun remove_descriptor/0, fun get_descriptor/0, fun list_descriptors/0]}.


file_test_() ->
    {foreach, fun setup/0, fun teardown/1,
        [fun save_file/0, fun remove_file/0, fun get_file/0, fun list_dir/0, fun rename_file/0, fun get_path_info/0]}.


setup() ->
    meck:new([dao, dao_helper]),
    meck:expect(dao, set_db, fun(_) -> ok end),
    meck:expect(dao, save_record, fun(_) -> {ok, "uuid"} end).


teardown(_) ->
    ok = meck:unload([dao, dao_helper]).


file_path_analyze_test() ->
    Expected1 = {internal_path, ["test", "test2"], "uuid"},
    Expected2 = {internal_path, ["test", "test2"], ""},
    Path = "test" ++ [?PATH_SEPARATOR] ++ "test2",
    ?assertMatch(Expected1, dao_vfs:file_path_analyze({Path, "uuid"})),
    ?assertMatch(Expected1, dao_vfs:file_path_analyze({relative_path, Path, "uuid"})),
    ?assertMatch(Expected2, dao_vfs:file_path_analyze(Path)),
    ?assertMatch(Expected2, dao_vfs:file_path_analyze({absolute_path, Path})),
    ?assertThrow({invalid_file_path, {absolute_path, "path", "uuid"}}, dao_vfs:file_path_analyze({absolute_path, "path", "uuid"})),
    ?assertThrow({invalid_file_path, 2}, dao_vfs:file_path_analyze(2)).


save_descriptor() ->
    Doc = #veil_document{record = #file_descriptor{}},
    ?assertMatch({ok, "uuid"}, dao_vfs:save_descriptor(Doc)),
    ?assert(meck:called(dao, set_db, [?DESCRIPTORS_DB_NAME])),
    ?assert(meck:called(dao, save_record, [Doc])),

    ?assertMatch({ok, "uuid"}, dao_vfs:save_descriptor(#file_descriptor{})),
    ?assert(meck:called(dao, set_db, [?DESCRIPTORS_DB_NAME])),
    ?assert(meck:called(dao, save_record, [Doc])),

    ?assert(meck:validate([dao, dao_helper])).


remove_descriptor() -> 
    ?assert(meck:validate([dao, dao_helper])).


get_descriptor() -> 
    ?assert(meck:validate([dao, dao_helper])).


list_descriptors() -> 
    ?assert(meck:validate([dao, dao_helper])).


save_file() -> 
    ?assert(meck:validate([dao, dao_helper])).


remove_file() -> 
    ?assert(meck:validate([dao, dao_helper])).


get_file() -> 
    ?assert(meck:validate([dao, dao_helper])).


get_path_info() -> 
    ?assert(meck:validate([dao, dao_helper])).


list_dir() ->
    ?assert(meck:validate([dao, dao_helper])).


rename_file() ->
    ?assert(meck:validate([dao, dao_helper])).


lock_file_test() ->
    not_yet_implemented = dao_vfs:lock_file("test", "test", write).

unlock_file_test() ->
    not_yet_implemented = dao_vfs:unlock_file("test", "test", write).



-endif.