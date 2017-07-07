%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @doc
%%% WRITEME
%%% @end
%%%-------------------------------------------------------------------
-author("Jakub Kudzia").

-include_lib("ctool/include/test/test_utils.hrl").

-define(ATTEMPTS, 30).

-define(STORAGE(Config, Mnt),
    atom_to_binary(?config(host_path, ?config(Mnt, ?config(posix, ?config(storages, Config)))), latin1)).

%% defaults
-define(SCAN_INTERVAL, 10).
-define(WRITE_ONCE, false).
-define(DELETE_ENABLE, false).
-define(MAX_DEPTH, 9999999999999999999999).

%% test data
-define(USER, <<"user1">>).
-define(SPACE_ID, <<"space1">>).
-define(SPACE_NAME, <<"space_name1">>).
-define(SPACE_PATH, <<"/", (?SPACE_NAME)/binary>>).
-define(TEST_DIR, <<"test_dir">>).
-define(TEST_DIR2, <<"test_dir2">>).
-define(TEST_FILE1, <<"test_file">>).
-define(TEST_FILE2, <<"test_file2">>).
-define(TEST_FILE3, <<"test_file3">>).
-define(TEST_FILE4, <<"test_file4">>).
-define(INIT_FILE, <<".__onedata__init_file">>).
-define(SPACE_TEST_DIR_PATH(DirName), filename:join(["/", ?SPACE_NAME, DirName])).
-define(SPACE_TEST_FILE_IN_DIR_PATH(DirName, FileName), filename:join(["/", ?SPACE_NAME, DirName, FileName])).
-define(SPACE_TEST_DIR_PATH, filename:join(["/", ?SPACE_NAME, ?TEST_DIR])).
-define(SPACE_TEST_DIR_PATH2, filename:join(["/", ?SPACE_NAME, ?TEST_DIR2])).
-define(SPACE_TEST_FILE_PATH, filename:join(["/", ?SPACE_NAME, ?TEST_FILE1])).
-define(SPACE_TEST_FILE_PATH2, filename:join(["/", ?SPACE_NAME, ?TEST_FILE2])).
-define(SPACE_TEST_FILE_PATH3, filename:join(["/", ?SPACE_NAME, ?TEST_FILE3])).
-define(SPACE_TEST_FILE_PATH4, filename:join(["/", ?SPACE_NAME, ?TEST_FILE4])).
-define(SPACE_TEST_FILE_IN_DIR_PATH, filename:join([?SPACE_TEST_DIR_PATH, ?TEST_FILE1])).
-define(SPACE_TEST_FILE_IN_DIR_PATH2, filename:join([?SPACE_TEST_DIR_PATH, ?TEST_FILE2])).
-define(SPACE_TEST_FILE_IN_DIR_PATH3, filename:join([?SPACE_TEST_DIR_PATH, ?TEST_FILE3])).
-define(SPACE_INIT_FILE_PATH, filename:join(["/", ?SPACE_NAME, ?INIT_FILE])).
-define(TEST_DATA, <<"test_data">>).
-define(TEST_DATA2, <<"test_data2">>).
