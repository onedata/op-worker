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

%% TODO brakuje testu listowania deskryptorów po dacie wygaśnięcia
%% TODO brakuje testów funkcji list_storage oraz remove_descriptor3

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include_lib("veil_modules/dao/dao.hrl").
-include_lib("veil_modules/dao/dao_helper.hrl").
-include_lib("files_common.hrl").
-endif.

-ifdef(TEST).

file_descriptor_test_() ->
    {foreach, fun setup/0, fun teardown/1,
        [fun save_descriptor/0, fun remove_descriptor/0, fun get_descriptor/0, fun list_descriptors/0]}.


file_test_() ->
    {foreach, fun setup/0, fun teardown/1,
        [fun save_file/0, fun get_file/0, fun remove_file/0, fun list_dir/0, fun rename_file/0, fun get_path_info/0]}.

storage_test_() ->
	{foreach, fun setup/0, fun teardown/1,
		[fun save_storage/0, fun remove_storage/0, fun get_storage/0]}.


setup() ->
    meck:new([dao, dao_helper]),
    meck:expect(dao, set_db, fun(_) -> ok end),
    meck:expect(dao, save_record, fun(_) -> {ok, "uuid"} end),
    meck:expect(dao, get_record, fun(_) -> {ok, #veil_document{}} end),
    meck:expect(dao, remove_record, fun(_) -> ok end),
    meck:expect(dao_helper, name, fun(Arg) -> Arg end).


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
    ?assertMatch({ok, "uuid"}, dao_vfs:save_descriptor(#file_descriptor{})),

    ?assertEqual(2, meck:num_calls(dao, set_db, [?DESCRIPTORS_DB_NAME])),
    ?assertEqual(2, meck:num_calls(dao, save_record, [Doc])),
    ?assert(meck:validate([dao, dao_helper])).


remove_descriptor() ->
    ?assertMatch(ok, dao_vfs:remove_descriptor("uuid")),
    ?assert(meck:called(dao, set_db, [?DESCRIPTORS_DB_NAME])),
    ?assert(meck:called(dao, remove_record, ["uuid"])),
    ?assert(meck:validate([dao, dao_helper])).


get_descriptor() ->
    meck:expect(dao, get_record, fun(_) -> {ok, #veil_document{record = #file{}}} end),
    meck:expect(dao, get_record, fun("uuid") -> {ok, #veil_document{record = #file_descriptor{file = "test"}}};
                                    ("uuid2") -> {ok, #veil_document{record = #file{}}} end),
    ?assertMatch({ok, #veil_document{record = #file_descriptor{file = "test"}}},
        dao_vfs:get_descriptor("uuid")),
    ?assert(meck:called(dao, get_record, ["uuid"])),

    ?assertMatch({error, invalid_fd_record}, dao_vfs:get_descriptor("uuid2")),
    ?assert(meck:called(dao, get_record, ["uuid2"])),

    ?assertEqual(2, meck:num_calls(dao, set_db, [?DESCRIPTORS_DB_NAME])),
    ?assert(meck:validate([dao, dao_helper])).


list_descriptors() ->
    meck:expect(dao, get_record, fun(_) -> {ok, #veil_document{uuid = "uuid", record = #file{}}} end),
    meck:expect(dao, list_records, 2 , {ok, #view_result{rows = [#view_row{doc = #veil_document{record = #file_descriptor{file = "uuid"}}}]}}),
    ?assertMatch({ok, [#veil_document{record = #file_descriptor{}}]}, dao_vfs:list_descriptors({by_file, {uuid, "file"}}, 5, 0)),


    ?assert(meck:called(dao, list_records, [?FD_BY_FILE_VIEW,
        #view_query_args{start_key = ["uuid", ""], end_key = ["uuie", ""], include_docs = true, limit = 5, skip = 0}])),
    ?assert(meck:validate([dao, dao_helper])).


save_file() ->
    Doc = #veil_document{record = #file{}},
    ?assertMatch({ok, "uuid"}, dao_vfs:save_file(Doc)),
    ?assertMatch({ok, "uuid"}, dao_vfs:save_file(#file{})),

    ?assertEqual(2, meck:num_calls(dao, set_db, [?FILES_DB_NAME])),
    ?assertEqual(2, meck:num_calls(dao, save_record, [Doc])),
    ?assert(meck:validate([dao, dao_helper])).


remove_file() ->
    meck:expect(dao, get_record, fun(_) -> {ok, #veil_document{uuid = "uuid", record = #file{}}} end),

    ?assertMatch(ok, dao_vfs:remove_file({uuid, "file"})),

    ?assert(meck:called(dao, set_db, [?FILES_DB_NAME])),
    ?assert(meck:called(dao, remove_record, ["uuid"])),
    ?assert(meck:validate([dao, dao_helper])).


get_file() ->
    File = {internal_path, ["path", "test1"], "root"},
    ?assertMatch({ok, #veil_document{record = #file{name = ""}}}, dao_vfs:get_file({internal_path, "", ""})),

    meck:expect(dao, get_record, fun(_) -> {ok, #veil_document{uuid = "f", record = #file{name = "test"}}} end),
    ?assertMatch({ok, #veil_document{record = #file{name = "test"}}}, dao_vfs:get_file({uuid, "file"})),
    ?assert(meck:called(dao, get_record, ["file"])),
    ?assert(meck:called(dao, set_db, [?FILES_DB_NAME])),

    meck:expect(dao, get_record, fun(_) -> {ok, #veil_document{}} end),
    ?assertMatch({error, invalid_file_record}, dao_vfs:get_file({uuid, "file"})),
    ?assert(meck:called(dao, get_record, ["file"])),
    ?assert(meck:called(dao, set_db, [?FILES_DB_NAME])),

    meck:expect(dao, list_records, 2 , meck:seq([invalid, {ok, #view_result{rows = []}},
        {ok, #view_result{rows = [#view_row{doc = #veil_document{record = #file{name = "test"}}}]}}])),

    ?assertThrow(invalid_data, dao_vfs:get_file(File)),
    ?assertThrow(file_not_found, dao_vfs:get_file(File)),
    ?assertMatch({ok, #veil_document{record = #file{name = "test"}}}, dao_vfs:get_file(File)),

    ?assertEqual(4, meck:num_calls(dao, list_records, ['_', '_'])),
    ?assert(meck:validate([dao, dao_helper])).


get_path_info() ->
    File = {internal_path, ["path", "test1"], "root"},
    meck:expect(dao, list_records, 2 , meck:seq([
        {ok, #view_result{rows = [#view_row{doc = #veil_document{record = #file{name = "test1"}}}]}},
        {ok, #view_result{rows = [#view_row{doc = #veil_document{record = #file{name = "test2"}}}]}}])),
    ?assertMatch({ok, [#veil_document{record = #file{name = "test1"}}, #veil_document{record = #file{name = "test2"}}]},
        dao_vfs:get_path_info(File)),

    ?assertEqual(2, meck:num_calls(dao, list_records, ['_', '_'])),
    ?assert(meck:validate([dao, dao_helper])).


list_dir() ->
    meck:expect(dao, get_record, fun(_) -> {ok, #veil_document{uuid = "f", record = #file{type = ?REG_TYPE}}} end),
    ?assertThrow({dir_not_found, _}, dao_vfs:list_dir({uuid, "file"}, 1, 0)),

    meck:expect(dao, get_record, fun(_) -> {ok, #veil_document{uuid = "f", record = #file{type = ?DIR_TYPE}}} end),
    meck:expect(dao, list_records, 2, {ok, #view_result{rows =
        [#view_row{doc = #veil_document{record = #file{parent = "f"}}}]}}),

    ?assertMatch({ok, [#veil_document{record = #file{}}]}, dao_vfs:list_dir({uuid, "file"}, 5, 0)),

    ?assert(meck:called(dao, list_records, [?FILE_TREE_VIEW, '_'])),
    ?assert(meck:validate([dao, dao_helper])).


rename_file() ->
    meck:expect(dao, get_record, fun(_) -> {ok, #veil_document{uuid = "uuid", record = #file{}}} end),

    ?assertMatch({ok, "uuid"}, dao_vfs:rename_file({uuid, "file"}, "name")),

    ?assert(meck:called(dao, save_record, [#veil_document{uuid = "uuid", record = #file{name = "name"}}])),
    ?assert(meck:validate([dao, dao_helper])).


save_storage() ->
    Doc = #veil_document{record = #storage_info{}},
    ?assertMatch({ok, "uuid"}, dao_vfs:save_storage(Doc)),
    ?assertMatch({ok, "uuid"}, dao_vfs:save_storage(#storage_info{})),

    ?assertEqual(2, meck:num_calls(dao, set_db, [?SYSTEM_DB_NAME])),
    ?assertEqual(2, meck:num_calls(dao, save_record, [Doc])),
    ?assert(meck:validate([dao, dao_helper])).


remove_storage() ->
    ?assertMatch(ok, dao_vfs:remove_storage({uuid, "uuid"})),
    ?assert(meck:called(dao, set_db, [?SYSTEM_DB_NAME])),
    ?assert(meck:called(dao, remove_record, ["uuid"])),
    ?assert(meck:validate([dao, dao_helper])).


get_storage() ->
    meck:expect(dao, get_record, fun(_) -> {ok, #veil_document{record = #file{}}} end),
    meck:expect(dao, get_record, fun("uuid") -> {ok, #veil_document{record = #storage_info{name = "test"}}};
        ("uuid2") -> {ok, #veil_document{record = #file{}}} end),
    ?assertMatch({ok, #veil_document{record = #storage_info{name = "test"}}},
        dao_vfs:get_storage({uuid, "uuid"})),
    ?assert(meck:called(dao, get_record, ["uuid"])),

    ?assertMatch({error, invalid_storage_record}, dao_vfs:get_storage({uuid, "uuid2"})),
    ?assert(meck:called(dao, get_record, ["uuid2"])),

    ?assertEqual(2, meck:num_calls(dao, set_db, [?SYSTEM_DB_NAME])),
    ?assert(meck:validate([dao, dao_helper])).


lock_file_test() ->
    not_yet_implemented = dao_vfs:lock_file("test", "test", write).

unlock_file_test() ->
    not_yet_implemented = dao_vfs:unlock_file("test", "test", write).



-endif.