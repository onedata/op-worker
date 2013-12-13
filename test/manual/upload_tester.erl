%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module checks upload speed.
%% @end
%% ===================================================================
-module(upload_tester).
%% ====================================================================
%% API
%% ====================================================================
-export([upload/2, upload/5]).

%% ====================================================================
%% API functions
%% ====================================================================

%% This test calls upload test from logical_files_manager.
%% First - some data is cached in memory,
%% next it is stored using files manager.
%% It simulates GUI work during the data upload.
upload(Node, WriteFunNum) ->
  upload(Node, WriteFunNum, "test_file", 4096, 1000).

upload(Node, WriteFunNum, File, Size, Times) ->
  Ans = rpc:call(Node, logical_files_manager, doUploadTest, [File, WriteFunNum, Size, Times]),
  io:format("Final test ans: ~p~n", [Ans]).