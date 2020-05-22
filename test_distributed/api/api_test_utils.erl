%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions used in API (REST + gs) tests.
%%% @end
%%%-------------------------------------------------------------------
-module(api_test_utils).
-author("Bartosz Walkowicz").

-include("api_test_runner.hrl").

-export([
    randomly_choose_file_type_for_test/0,
    create_file/4, create_file/5,
    wait_for_file_sync/3
]).


%%%===================================================================
%%% API
%%%===================================================================


randomly_choose_file_type_for_test() ->
    FileType = ?RANDOM_FILE_TYPE(),
    ct:pal("Chosen file type for test: ~s", [FileType]),
    FileType.


create_file(FileType, Node, SessId, Path) ->
    create_file(FileType, Node, SessId, Path, 8#777).


create_file(<<"file">>, Node, SessId, Path, Mode) ->
    lfm_proxy:create(Node, SessId, Path, Mode);
create_file(<<"dir">>, Node, SessId, Path, Mode) ->
    lfm_proxy:mkdir(Node, SessId, Path, Mode).


-spec wait_for_file_sync(node(), session:id(), file_id:file_guid()) -> ok.
wait_for_file_sync(Node, SessId, FileGuid) ->
    ?assertMatch({ok, _}, lfm_proxy:stat(Node, SessId, {guid, FileGuid}), ?ATTEMPTS),
    ok.
