%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests of multipart upload.
%%% @end
%%%-------------------------------------------------------------------
-module(multipart_upload_test_SUITE).
-author("Michal Stanisz").

-include("modules/logical_file_manager/lfm.hrl").
-include("onenv_test_utils.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").


-export([
    groups/0, all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_group/2, end_per_group/2,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    create_test/1,
    abort_test/1,
    complete_test/1,
    list_test/1,
    upload_part_test/1,
    list_parts_test/1
]).

groups() -> [
    {all_tests, [parallel], [
        create_test,
        abort_test,
        complete_test,
        list_test,
        upload_part_test,
        list_parts_test
    ]}
].

all() -> [
    {group, all_tests}
].


%%%====================================================================
%%% Test function
%%%====================================================================

create_test(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    SessId = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space_krk),
    Path = filename:join(<<"/some/upload/path">>, generator:gen_name()),
    
    ?assertMatch({ok, _}, lfm_proxy:create_multipart_upload(Node, SessId, SpaceId, Path)),
    % there can be multiple uploads for the same path
    ?assertMatch({ok, _}, lfm_proxy:create_multipart_upload(Node, SessId, SpaceId, Path)).


abort_test(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    SessId = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space_krk),
    Path = filename:join(<<"/some/upload/path">>, generator:gen_name()),
    
    ?assertMatch({error, ?EINVAL}, lfm_proxy:abort_multipart_upload(Node, SessId, <<"invalid_upload_id">>)),
    {ok, UploadId} = ?assertMatch({ok, _}, lfm_proxy:create_multipart_upload(Node, SessId, SpaceId, Path)),
    ?assertMatch(ok, lfm_proxy:abort_multipart_upload(Node, SessId, UploadId)),
    ?assertMatch({error, ?EINVAL}, lfm_proxy:abort_multipart_upload(Node, SessId, UploadId)).


complete_test(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    SessId = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space_krk),
    Path = filename:join(<<"/some/upload/path">>, generator:gen_name()),
    
    ?assertMatch({error, ?EINVAL}, lfm_proxy:complete_multipart_upload(Node, SessId, <<"invalid_upload_id">>)),
    {ok, UploadId} = ?assertMatch({ok, _}, lfm_proxy:create_multipart_upload(Node, SessId, SpaceId, Path)),
    ?assertMatch(ok, lfm_proxy:complete_multipart_upload(Node, SessId, UploadId)),
    ?assertMatch({error, ?EINVAL}, lfm_proxy:complete_multipart_upload(Node, SessId, UploadId)).


list_test(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    SessId = oct_background:get_user_session_id(user2, krakow), % run as different user so there is no interference with other tests
    SpaceId = oct_background:get_space_id(space_krk),
    Path = filename:join(<<"/some/upload/path">>, generator:gen_name()),

    ?assertMatch({ok, [], _, true}, lfm_proxy:list_multipart_uploads(Node, SessId, SpaceId, 20, undefined)),
    
    CreateUploadsFun = fun() ->
        lists:map(fun(Num) ->
            P = filename:join(Path, list_to_binary(lists:duplicate(Num, <<"a">>))),
            {ok, UploadId} = ?assertMatch({ok, _}, lfm_proxy:create_multipart_upload(Node, SessId, SpaceId, P)),
            UploadId
        end, lists:seq(1, 20))
    end,
    Uploads1 = CreateUploadsFun(),
    timer:sleep(100), % ensure different timestamps
    Uploads2 = CreateUploadsFun(),
    ExpectedUploads = lists:reverse(lists:foldl(fun({U1, U2}, Acc) ->
        [U2, U1 | Acc]
    end, [], lists:zip(Uploads1, Uploads2))),
    ?assertEqual(ExpectedUploads, list_all_uploads(Node, SessId, SpaceId, 1000)),
    ?assertEqual(ExpectedUploads, list_all_uploads(Node, SessId, SpaceId, 100)),
    ?assertEqual(ExpectedUploads, list_all_uploads(Node, SessId, SpaceId, 10)),
    ?assertEqual(ExpectedUploads, list_all_uploads(Node, SessId, SpaceId, 1)).


upload_part_test(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    SessId = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space_krk),
    Path = filename:join(<<"/some/upload/path">>, generator:gen_name()),
    
    {ok, UploadId} = ?assertMatch({ok, _}, lfm_proxy:create_multipart_upload(Node, SessId, SpaceId, Path)),
    
    Part = #multipart_upload_part{
        number = 1,
        size = 8,
        etag = <<"etag">>,
        last_modified = 123
    },
    ?assertEqual({error, ?EINVAL}, lfm_proxy:upload_multipart_part(Node, SessId, <<"invalid_upload_id">>, Part)),
    ?assertEqual(ok, lfm_proxy:upload_multipart_part(Node, SessId, UploadId, Part)),
    ?assertEqual(ok, lfm_proxy:upload_multipart_part(Node, SessId, UploadId, Part#multipart_upload_part{size = 123})).


list_parts_test(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    SessId = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space_krk),
    Path = filename:join(<<"/some/upload/path">>, generator:gen_name()),
    
    
    ?assertEqual({error, ?EINVAL}, lfm_proxy:list_multipart_parts(Node, SessId, <<"invalid_upload_id">>, 10, 0)),
    {ok, UploadId} = ?assertMatch({ok, _}, lfm_proxy:create_multipart_upload(Node, SessId, SpaceId, Path)),
    
    AllParts = lists:map(fun(Num) ->
        Part = #multipart_upload_part{
            number = Num,
            size = 8,
            etag = <<"etag">>,
            last_modified = 123
        },
        ?assertEqual(ok, lfm_proxy:upload_multipart_part(Node, SessId, UploadId, Part)),
        Part
    end, lists:seq(1, 100, 2)),
    ListPartsFun = fun(Limit, StartAfter) ->
        {ok, Result, _} = ?assertMatch({ok, _, _}, lfm_proxy:list_multipart_parts(Node, SessId, UploadId, Limit, StartAfter)),
        Result
    end,
    ?assertEqual(AllParts, ListPartsFun(100, 0)),
    ?assertEqual(lists:sublist(AllParts, 20), ListPartsFun(20, 0)),
    ?assertEqual(lists:sublist(AllParts, 11, 20), ListPartsFun(20, 20)).

%%%===================================================================
%%% Helper functions
%%%===================================================================

list_all_uploads(Node, SessId, SpaceId, Limit) ->
    list_all_uploads(Node, SessId, SpaceId, Limit, undefined).

list_all_uploads(Node, SessId, SpaceId, Limit, Token) ->
    {ok, Result, NextToken, IsLast} = ?assertMatch({ok, _, _, _}, lfm_proxy:list_multipart_uploads(Node, SessId, SpaceId, Limit, Token)),
    MappedResult = lists:map(fun(#multipart_upload{multipart_upload_id = UploadId}) -> UploadId end, Result),
    case IsLast of
        true -> MappedResult;
        false -> MappedResult ++ list_all_uploads(Node, SessId, SpaceId, Limit, NextToken)
    end.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    oct_background:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "1op",
        envs = [{op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}]
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_group(_Group, Config) ->
    lfm_proxy:init(Config, false).


end_per_group(_Group, Config) ->
    lfm_proxy:teardown(Config).


init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 5}),
    Config.


end_per_testcase(_Case, _Config) ->
    ok.
