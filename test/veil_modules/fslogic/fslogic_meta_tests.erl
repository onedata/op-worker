%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013, ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of fslogic_meta.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================
-module(fslogic_meta_tests).
-author("Rafal Slota").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include("veil_modules/dao/dao.hrl").

setup() ->
    meck:new([dao_lib]).

teardown(_) ->
    ok = meck:unload().

file_meta_test_() ->
    {foreach, fun setup/0, fun teardown/1, [fun init_file_meta/0]}.

init_file_meta() ->
    File = #file{uid = "1"},
    meck:expect(dao_lib, apply,
        fun (dao_vfs, save_file_meta, [#file_meta{uid = "1"}], _) ->
                {ok, "uuid"};
            (dao_vfs, get_file_meta, ["uuid"], _) ->
                {ok, #veil_document{uuid = "uuid", record = #file_meta{uid = File#file.uid}}}
        end),

    ?assertMatch({#file{uid = "1", meta_doc = "uuid"}, #veil_document{record = #file_meta{uid = "1"}}},
        fslogic_meta:init_file_meta(File)).

-endif.