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
-endif.

-ifdef(TEST).

file_descriptor_test_() ->
    {foreach, fun setup/0, fun teardown/1,
        [fun save_descriptor/0, fun remove_descriptor/0, fun get_descriptor/0, fun list_descriptors/0]}.


file_test_() ->
    {foreach, fun setup/0, fun teardown/1,
        [fun save_file/0, fun remove_file/0, fun get_file/0, fun list_dir/0, fun rename_file/0, fun get_path_info/0]}.


setup() ->
    ?assert(true).


teardown(_) ->
    ?assert(true).


file_path_analyze_test() ->
    ?assert(true).

list_dir() -> 
    ?assert(true).


rename_file() ->
    ?assert(true).


save_descriptor() -> 
    ?assert(true).


remove_descriptor() -> 
    ?assert(true).


get_descriptor() -> 
    ?assert(true).


list_descriptors() -> 
    ?assert(true).


save_file() -> 
    ?assert(true).


remove_file() -> 
    ?assert(true).


get_file() -> 
    ?assert(true).


get_path_info() -> 
    ?assert(true).


lock_file_test() ->
    not_yet_implemented = dao_vfs:lock_file("test", "test", write).

unlock_file_test() ->
    not_yet_implemented = dao_vfs:unlock_file("test", "test", write).



-endif.