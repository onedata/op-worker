%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module tests events management.
%%% @end
%%%--------------------------------------------------------------------
-module(client_events_test_SUITE).
-author("Michal Wrzeszcz").

-include_lib("clproto/include/messages.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, init_per_testcase/2,
    end_per_testcase/2, end_per_suite/1]).

-export([
    subscribe_on_dir_test/1
]).

all() ->
    ?ALL([
        subscribe_on_dir_test
    ]).

%%%===================================================================
%%% Tests
%%%===================================================================

subscribe_on_dir_test(Config) ->
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker1)}}, Config),
    SpaceGuid = client_simulation_test_base:get_guid(Worker1, SessionId, <<"/space_name1">>),

    {ok, {_, RootHandle}} = ?assertMatch({ok, _}, lfm_proxy:create_and_open(Worker1, <<"0">>, SpaceGuid,
        generator:gen_name(), 8#755)),
    ?assertEqual(ok, lfm_proxy:close(Worker1, RootHandle)),

    {ok, {Sock, _}} = fuse_test_utils:connect_via_macaroon(Worker1, [{active, true}], SessionId),

    Filename = generator:gen_name(),
    Dirname = generator:gen_name(),

    DirId = fuse_test_utils:create_directory(Sock, SpaceGuid, Dirname),
    client_simulation_test_base:create_new_file_subscriptions(Sock, DirId, 0),
    timer:sleep(2000), % there is no sync between subscription and unlink

    {FileGuid, HandleId} = fuse_test_utils:create_file(Sock, DirId, Filename),
    fuse_test_utils:close(Sock, FileGuid, HandleId),

    ?assertEqual(ok, lfm_proxy:unlink(Worker1, <<"0">>, {guid, FileGuid})),
    ?assertEqual(ok, receive_messages([])),
    ok.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    client_simulation_test_base:init_per_suite(Config).

init_per_testcase(_Case, Config) ->
    client_simulation_test_base:init_per_testcase(Config).

end_per_testcase(_Case, Config) ->
    client_simulation_test_base:end_per_testcase(Config).

end_per_suite(_Case) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

receive_messages(Cache) ->
    case fuse_test_utils:receive_server_message([message_stream_reset, subscription, message_request,
        message_acknowledgement, processing_status]) of
        #'ServerMessage'{
            message_body = {events, #'Events'{events = [#'Event'{
                type = {file_removed, #'FileRemovedEvent'{}}
            }]}}
        } -> ok;
        {error, timeout} -> Cache;
        Msg -> receive_messages([Msg | Cache])
    end.