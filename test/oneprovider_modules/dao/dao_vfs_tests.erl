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
-include_lib("oneprovider_modules/dao/dao.hrl").
-include_lib("dao/include/dao_helper.hrl").
-include_lib("files_common.hrl").
-endif.

-ifdef(TEST).

file_descriptor_test_() ->
    {foreach, fun setup/0, fun teardown/1,
        [fun save_descriptor/0, fun remove_descriptor/0, fun get_descriptor/0, fun list_descriptors/0]}.

file_test_() ->
    {foreach, fun setup/0, fun teardown/1,
        [fun save_file/0, fun get_file/0, fun remove_file/0, fun list_dir/0, fun rename_file/0, fun get_path_info/0]}.

file_location_test_() ->
    {foreach, fun setup/0, fun teardown/1,
        [fun list_file_locations/0, fun get_file_locations/0, fun save_file_location/0, fun remove_file_location/0,
         fun list_file_blocks/0, fun get_file_blocks/0, fun save_file_block/0, fun remove_file_block/0]}.

file_meta_test_() ->
    {foreach, fun setup/0, fun teardown/1,
        [fun save_file_meta/0, fun get_file_meta/0, fun remove_file_meta/0]}.

storage_test_() ->
	{foreach, fun setup/0, fun teardown/1,
		[fun save_storage/0, fun remove_storage/0, fun get_storage/0]}.


setup() ->
    meck:new([dao_records, dao_helper, worker_host]),
    meck:expect(dao_records, save_record, fun(_) -> {ok, "uuid"} end),
    meck:expect(dao_records, get_record, fun(_) -> {ok, #db_document{}} end),
    meck:expect(dao_records, remove_record, fun(_) -> ok end),
    meck:expect(dao_helper, name, fun(Arg) -> Arg end),
    ets:new(storage_cache, [named_table, public, set, {read_concurrency, true}]).


teardown(_) ->
    ets:delete(storage_cache),
    ok = meck:unload([dao_records, dao_helper, worker_host]).


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


save_file_meta() ->
    Doc = #db_document{record = #file_meta{}},
    ?assertMatch({ok, "uuid"}, dao_vfs:save_file_meta(Doc)),
    ?assertMatch({ok, "uuid"}, dao_vfs:save_file_meta(#file_meta{})),

    ?assertEqual(2, meck:num_calls(dao_records, save_record, [Doc])),
    ?assert(meck:validate([dao_records, dao_helper])).


remove_file_meta() ->
    ?assertMatch(ok, dao_vfs:remove_file_meta("uuid")),
    ?assert(meck:called(dao_records, remove_record, ["uuid"])),
    ?assert(meck:validate([dao_records, dao_helper])).


get_file_meta() ->
    meck:expect(dao_records, get_record, fun(_) -> {ok, #db_document{record = #file_meta{}}} end),
    meck:expect(dao_records, get_record, fun("uuid") -> {ok, #db_document{record = #file_meta{ctime = 1111}}} end),
    ?assertMatch({ok, #db_document{record = #file_meta{ctime = 1111}}}, dao_vfs:get_file_meta("uuid")),
    ?assert(meck:called(dao_records, get_record, ["uuid"])),
    ?assert(meck:validate([dao_records, dao_helper])).


save_descriptor() ->
    Doc = #db_document{record = #file_descriptor{}},
    ?assertMatch({ok, "uuid"}, dao_vfs:save_descriptor(Doc)),
    ?assertMatch({ok, "uuid"}, dao_vfs:save_descriptor(#file_descriptor{})),

    ?assertEqual(2, meck:num_calls(dao_records, save_record, [Doc])),
    ?assert(meck:validate([dao_records, dao_helper])).


remove_descriptor() ->
    ?assertMatch(ok, dao_vfs:remove_descriptor("uuid")),
    ?assert(meck:called(dao_records, remove_record, ["uuid"])),
    ?assert(meck:validate([dao_records, dao_helper])).


get_descriptor() ->
    meck:expect(dao_records, get_record, fun(_) -> {ok, #db_document{record = #file{}}} end),
    meck:expect(dao_records, get_record, fun("uuid") -> {ok, #db_document{record = #file_descriptor{file = "test"}}};
                                    ("uuid2") -> {ok, #db_document{record = #file{}}} end),
    ?assertMatch({ok, #db_document{record = #file_descriptor{file = "test"}}},
        dao_vfs:get_descriptor("uuid")),
    ?assert(meck:called(dao_records, get_record, ["uuid"])),

    ?assertMatch({error, invalid_fd_record}, dao_vfs:get_descriptor("uuid2")),
    ?assert(meck:called(dao_records, get_record, ["uuid2"])),

    ?assert(meck:validate([dao_records, dao_helper])).


list_descriptors() ->
    meck:expect(dao_records, get_record, fun(_) -> {ok, #db_document{uuid = "uuid", record = #file{}}} end),
    meck:expect(dao_records, list_records, 2 , {ok, #view_result{rows = [#view_row{doc = #db_document{record = #file_descriptor{file = "uuid"}}}]}}),
    ?assertMatch({ok, [#db_document{record = #file_descriptor{}}]}, dao_vfs:list_descriptors({by_file, {uuid, "file"}}, 5, 0)),


    ?assert(meck:called(dao_records, list_records, [?FD_BY_FILE_VIEW,
        #view_query_args{start_key = ["uuid", ""], end_key = ["uuiD", ""], include_docs = true, limit = 5, skip = 0}])),
    ?assert(meck:validate([dao_records, dao_helper])).


save_file() ->
    File = #file{name = "Name"},
    Doc = #db_document{record = File},
    ?assertMatch({ok, "uuid"}, dao_vfs:save_file(Doc)),
    ?assertMatch({ok, "uuid"}, dao_vfs:save_file(File)),

    ?assertEqual(2, meck:num_calls(dao_records, save_record, [Doc#db_document{record = #file{name = {unicode_string, "Name"}}}])),
    ?assert(meck:validate([dao_records, dao_helper])).


remove_file() ->
    meck:expect(dao_records, get_record, fun(_) -> {ok, #db_document{uuid = "uuid", record = #file{meta_doc = "meta"}}} end),
    meck:expect(dao_records, list_records, fun (?FD_BY_FILE_VIEW, #view_query_args{skip = 0}) -> {ok, #view_result{rows = [#view_row{id = "fd", doc = #db_document{uuid = "fd", record = #file_descriptor{file = "uuid"}}}]}};
                                       (?FD_BY_FILE_VIEW, _) -> {ok, #view_result{rows = []}};
                                       (?SHARE_BY_FILE_VIEW, _) ->
                                           {ok, #view_result{rows = [
                                               #view_row{id = "share1", doc = #db_document{uuid = "share1", record = #share_desc{file = "uuid"}}},
                                               #view_row{id = "share2", doc = #db_document{uuid = "share2", record = #share_desc{file = "uuid"}}}
                                           ]}};
                                       (?FILE_LOCATIONS_BY_FILE, _) -> {ok, #view_result{rows = []}};
                                       (?AVAILABLE_BLOCKS_BY_FILE_ID, _) -> {ok, #view_result{rows = []}}
                                   end),


    ?assertMatch(ok, dao_vfs:remove_file({uuid, "file"})),

    ?assert(meck:called(dao_records, remove_record, ["uuid"])),
    ?assert(meck:called(dao_records, remove_record, ["meta"])),
    ?assert(meck:called(dao_records, remove_record, ["fd"])),
    ?assert(meck:called(dao_records, remove_record, ["share1"])),
    ?assert(meck:called(dao_records, remove_record, ["share2"])),
    ?assert(meck:validate([dao_records, dao_helper])).


get_file() ->
    File = {internal_path, ["path", "test1"], "root"},
    ?assertMatch({ok, #db_document{record = #file{name = ""}}}, dao_vfs:get_file({internal_path, "", ""})),

    meck:expect(dao_records, get_record, fun(_) -> {ok, #db_document{uuid = "f", record = #file{name = "test"}}} end),
    ?assertMatch({ok, #db_document{record = #file{name = "test"}}}, dao_vfs:get_file({uuid, "file"})),
    ?assert(meck:called(dao_records, get_record, ["file"])),

    meck:expect(dao_records, get_record, fun(_) -> {ok, #db_document{}} end),
    ?assertMatch({error, invalid_file_record}, dao_vfs:get_file({uuid, "file"})),
    ?assert(meck:called(dao_records, get_record, ["file"])),

    meck:expect(dao_records, list_records, 2 , meck:seq([invalid, {ok, #view_result{rows = []}},
        {ok, #view_result{rows = [#view_row{doc = #db_document{record = #file{name = "test"}}}]}}])),

    ?assertThrow(invalid_data, dao_vfs:get_file(File)),
    ?assertThrow(file_not_found, dao_vfs:get_file(File)),
    ?assertMatch({ok, #db_document{record = #file{name = "test"}}}, dao_vfs:get_file(File)),

    ?assertEqual(4, meck:num_calls(dao_records, list_records, ['_', '_'])),
    ?assert(meck:validate([dao_records, dao_helper])).


get_path_info() ->
    File = {internal_path, ["path", "test1"], "root"},
    meck:expect(dao_records, list_records, 2 , meck:seq([
        {ok, #view_result{rows = [#view_row{doc = #db_document{record = #file{name = "test1"}}}]}},
        {ok, #view_result{rows = [#view_row{doc = #db_document{record = #file{name = "test2"}}}]}}])),
    ?assertMatch({ok, [#db_document{record = #file{name = "test1"}}, #db_document{record = #file{name = "test2"}}]},
        dao_vfs:get_path_info(File)),

    ?assertEqual(2, meck:num_calls(dao_records, list_records, ['_', '_'])),
    ?assert(meck:validate([dao_records, dao_helper])).


list_dir() ->
    meck:expect(dao_records, get_record, fun(_) -> {ok, #db_document{uuid = "f", record = #file{type = ?REG_TYPE}}} end),
    ?assertThrow({dir_not_found, _}, dao_vfs:list_dir({uuid, "file"}, 1, 0)),

    meck:expect(dao_records, get_record, fun(_) -> {ok, #db_document{uuid = "f", record = #file{type = ?DIR_TYPE}}} end),
    meck:expect(dao_records, list_records, 2, {ok, #view_result{rows =
        [#view_row{doc = #db_document{record = #file{parent = "f"}}}]}}),

    ?assertMatch({ok, [#db_document{record = #file{}}]}, dao_vfs:list_dir({uuid, "file"}, 5, 0)),

    ?assert(meck:called(dao_records, list_records, [?FILE_TREE_VIEW, '_'])),
    ?assert(meck:validate([dao_records, dao_helper])).


rename_file() ->
    meck:expect(dao_records, get_record, fun(_) -> {ok, #db_document{uuid = "uuid", record = #file{}}} end),

    ?assertMatch({ok, "uuid"}, dao_vfs:rename_file({uuid, "file"}, "name")),

    ?assert(meck:called(dao_records, save_record, [#db_document{uuid = "uuid", record = #file{name = {unicode_string, "name"}}}])),
    ?assert(meck:validate([dao_records, dao_helper])).


save_storage() ->
    meck:expect(worker_host, clear_cache, fun
      ({storage_cache, [{uuid, _}, {id, _}]}) -> ok
    end),
    Doc = #db_document{record = #storage_info{}},
    ?assertMatch({ok, "uuid"}, dao_vfs:save_storage(Doc)),
    ?assertMatch({ok, "uuid"}, dao_vfs:save_storage(#storage_info{})),

    ?assertEqual(2, meck:num_calls(dao_records, save_record, [Doc])),
    ?assert(meck:validate([dao_records, dao_helper, worker_host])).


remove_storage() ->
    meck:expect(worker_host, clear_cache, fun
      ({storage_cache, [{uuid, "uuid"}, {id, _}]}) -> ok
    end),
    meck:expect(dao_records, get_record, fun("uuid") -> {ok, #db_document{uuid="uuid", record = #storage_info{}}} end),
    ?assertMatch(ok, dao_vfs:remove_storage({uuid, "uuid"})),
    ?assert(meck:called(dao_records, remove_record, ["uuid"])),
    ?assert(meck:validate([dao_records, dao_helper, worker_host])).


get_storage() ->
    meck:expect(dao_records, get_record, fun(_) -> {ok, #db_document{record = #file{}}} end),
    meck:expect(dao_records, get_record, fun("uuid") -> {ok, #db_document{record = #storage_info{name = "test"}}};
        ("uuid2") -> {ok, #db_document{record = #file{}}} end),
    ?assertMatch({ok, #db_document{record = #storage_info{name = "test"}}},
        dao_vfs:get_storage({uuid, "uuid"})),
    ?assert(meck:called(dao_records, get_record, ["uuid"])),

    ?assertMatch({error, invalid_storage_record}, dao_vfs:get_storage({uuid, "uuid2"})),
    ?assert(meck:called(dao_records, get_record, ["uuid2"])),

    ?assert(meck:validate([dao_records, dao_helper])).


lock_file_test() ->
    not_yet_implemented = dao_vfs:lock_file("test", "test", write).

unlock_file_test() ->
    not_yet_implemented = dao_vfs:unlock_file("test", "test", write).

uca_increment_test() ->
    ?assertMatch([10], dao_vfs:uca_increment("")),
    ?assertMatch("1", dao_vfs:uca_increment("0")),
    ?assertMatch("a", dao_vfs:uca_increment("9")),
    ?assertMatch("A", dao_vfs:uca_increment("a")),
    ?assertMatch("Z", dao_vfs:uca_increment("z")),
    ?assertMatch("E", dao_vfs:uca_increment("D")),
    ?assertMatch("0", dao_vfs:uca_increment("$")),
    ?assertMatch("#", dao_vfs:uca_increment("&")),
    ?assertMatch("4add42F", dao_vfs:uca_increment("4add42f")),
    ?assertMatch("4adD42Z", dao_vfs:uca_increment("4add42Z")),
    ?assertMatch("423432422", dao_vfs:uca_increment("423432421")),
    ?assertMatch("42343242a", dao_vfs:uca_increment("423432429")),
    ?assertMatch("423432430", dao_vfs:uca_increment("42343242Z")),
    ?assertMatch("423500000", dao_vfs:uca_increment("4234ZZZZZ")),
    ?assertMatch("423a00000", dao_vfs:uca_increment("4239ZZZZZ")),
    ?assertMatch("423B00000", dao_vfs:uca_increment("423AZZZZZ")),
    ?assertMatch("ZZZZ\n", dao_vfs:uca_increment("ZZZZ")),
    ?assertMatch("ZZZZ", dao_vfs:uca_increment("ZzZZ")),
    ?assertMatch("ZzCZ", dao_vfs:uca_increment("ZzcZ")),
    ?assertMatch("ZZ Z", dao_vfs:uca_increment("Zz Z")),
    ?assertMatch("Z 60", dao_vfs:uca_increment("Z 5Z")),
    ?assertMatch("Z`00", dao_vfs:uca_increment("Z ZZ")),
    ?assertMatch("1{00", dao_vfs:uca_increment("1]ZZ")),
    ?assertMatch("1C]DA", dao_vfs:uca_increment("1c]DA")),
    ?assertMatch("1C$000", dao_vfs:uca_increment("1C$$ZZ")),
    ?assertMatch("1C$$00", dao_vfs:uca_increment("1C$~ZZ")).

list_file_locations() ->
    meck:expect(dao_records, list_records, fun(?FILE_LOCATIONS_BY_FILE, #view_query_args{keys = ["fileid"]}) ->
        {ok, #view_result{rows = [#view_row{id = "1"}, #view_row{id = "2"}]}} end),
    {ok, Locations} = dao_vfs:list_file_locations("fileid"),
    ?assertEqual(2, length(Locations)),
    ?assert(lists:member("1", Locations)),
    ?assert(lists:member("2", Locations)),
    ?assert(meck:validate([dao_records])).

get_file_locations() ->
    Location1 = #db_document{record = #file_location{storage_uuid = "a", storage_file_id = 1, file_id = "c"}},
    Location2 = #db_document{record = #file_location{storage_uuid = "d", storage_file_id = 2, file_id = "f"}},
    meck:expect(dao_records, list_records, fun(?FILE_LOCATIONS_BY_FILE, #view_query_args{keys = ["fileid"]}) ->
        {ok, #view_result{rows = [#view_row{doc = Location1}, #view_row{doc = Location2}]}} end),

    {ok, Locations} = dao_vfs:get_file_locations("fileid"),
    ?assert(lists:member(Location1, Locations)),
    ?assert(lists:member(Location2, Locations)),
    ?assert(meck:validate([dao_records])).

save_file_location() ->
    FileLocation = #file_location{file_id = "fileid", storage_file_id = "storagefileid", storage_uuid = "storageuuid"},
    Doc = #db_document{record = FileLocation},
    ?assertMatch({ok, "uuid"}, dao_vfs:save_file_location(Doc)),
    ?assertMatch({ok, "uuid"}, dao_vfs:save_file_location(FileLocation)),

    ?assertEqual(2, meck:num_calls(dao_records, save_record, [Doc])),
    ?assert(meck:validate([dao_records])).

remove_file_location() ->
    meck:expect(dao_records, list_records, fun
        (?FILE_LOCATIONS_BY_FILE, #view_query_args{keys = ["fileid"]}) ->
            {ok, #view_result{rows = [#view_row{id = "1"}, #view_row{id = "2"}]}};
        (?FILE_BLOCKS_BY_FILE_LOCATION, _) ->
            {ok, #view_result{rows = [#view_row{id = "block1"}, #view_row{id = "block2"}]}} end),

    ?assertMatch(ok, dao_vfs:remove_file_location("uuid")),
    ?assert(meck:called(dao_records, remove_record, ["uuid"])),
    ?assert(meck:called(dao_records, remove_record, ["block1"])),
    ?assert(meck:called(dao_records, remove_record, ["block2"])),
    ?assert(meck:validate(dao_records)),

    ?assertMatch(ok, dao_vfs:remove_file_location({file_id, "fileid"})),
    ?assert(meck:called(dao_records, remove_record, ["1"])),
    ?assert(meck:called(dao_records, remove_record, ["2"])),
    ?assert(meck:called(dao_records, remove_record, ["block1"])),
    ?assert(meck:called(dao_records, remove_record, ["block2"])),
    ?assert(meck:validate(dao_records)).

list_file_blocks() ->
    meck:expect(dao_records, list_records, fun(?FILE_BLOCKS_BY_FILE_LOCATION, #view_query_args{keys = ["locationid"]}) ->
        {ok, #view_result{rows = [#view_row{id = "1"}, #view_row{id = "2"}]}} end),
    {ok, Blocks} = dao_vfs:list_file_blocks("locationid"),
    ?assertEqual(2, length(Blocks)),
    ?assert(lists:member("1", Blocks)),
    ?assert(lists:member("2", Blocks)),
    ?assert(meck:validate([dao_records])).

get_file_blocks() ->
    Block1 = #db_document{record = #file_block{file_location_id = "locationid"}},
    Block2 = #db_document{record = #file_block{file_location_id = "locationid"}},
    meck:expect(dao_records, list_records, fun(?FILE_BLOCKS_BY_FILE_LOCATION, #view_query_args{keys = ["locationid"]}) ->
        {ok, #view_result{rows = [#view_row{doc = Block1}, #view_row{doc = Block2}]}} end),

    {ok, Locations} = dao_vfs:get_file_blocks("locationid"),
    ?assert(lists:member(Block1, Locations)),
    ?assert(lists:member(Block2, Locations)),
    ?assert(meck:validate([dao_records])).

save_file_block() ->
    Block = #file_block{file_location_id = "a", offset = 1, size = 2},
    Doc = #db_document{record = Block},
    ?assertMatch({ok, "uuid"}, dao_vfs:save_file_block(Doc)),
    ?assertMatch({ok, "uuid"}, dao_vfs:save_file_block(Block)),

    ?assertEqual(2, meck:num_calls(dao_records, save_record, [Doc])),
    ?assert(meck:validate([dao_records])).

remove_file_block() ->
    meck:expect(dao_records, list_records,
        fun(?FILE_BLOCKS_BY_FILE_LOCATION, #view_query_args{keys = ["locationid"]}) ->
            {ok, #view_result{rows = [#view_row{id = "1"}, #view_row{id = "2"}]}} end),

    ?assertMatch(ok, dao_vfs:remove_file_block("uuid")),
    ?assert(meck:called(dao_records, remove_record, ["uuid"])),
    ?assert(meck:validate(dao_records)),

    ?assertMatch(ok, dao_vfs:remove_file_block({file_location_id, "locationid"})),
    ?assert(meck:called(dao_records, remove_record, ["1"])),
    ?assert(meck:called(dao_records, remove_record, ["2"])),

    ?assert(meck:validate(dao_records)).

-endif.
