%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module provides base for testing cleanup of memory pools
%%% @end
%%%--------------------------------------------------------------------
-module(client_simulation_test_base).
-author("Michal Wrzeszcz").

-include("fuse_test_utils.hrl").
-include("proto/oneclient/event_messages.hrl").
-include("proto/oneclient/client_messages.hrl").
-include_lib("clproto/include/messages.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_links.hrl").

-export([init_per_suite/1, init_per_testcase/1, end_per_testcase/1]).
-export([simulate_client/5, verify_streams/1, get_guid/3]).

-define(req(W, SessId, FuseRequest), element(2, rpc:call(W, worker_proxy, call,
    [fslogic_worker, {fuse_request, SessId, #fuse_request{fuse_request = FuseRequest}}]))).

-define(SeqID, erlang:unique_integer([positive, monotonic]) - 2).

%%%===================================================================
%%% API
%%%===================================================================

simulate_client(_Config, Args, Sock, SpaceGuid, Close) ->
    AllArgs = [write, read, release, unsub, directory],
    [Write, Read, Release, Unsub, Directory] = lists:map(fun(A) -> lists:member(A, Args) end, AllArgs),


    Filename = generator:gen_name(),
    {ParentGuid, DirSubs} = maybe_create_directory(Sock, SpaceGuid, Directory),

    {FileGuid, HandleId} = fuse_test_utils:create_file(Sock, ParentGuid, Filename),
    Subs = create_new_file_subscriptions(Sock, FileGuid, 0),

    fuse_test_utils:fsync(Sock, FileGuid, HandleId, false),

    ExpectedData = maybe_write(Sock, FileGuid, HandleId, Write),
    maybe_read(Sock, FileGuid, HandleId, ExpectedData, Read),
    % TODO - check if streams are defined
%%    verify_streams(Config),
    maybe_release(Sock, FileGuid, HandleId, Release),
    maybe_ls(Sock, ParentGuid, Directory),
    maybe_unsub(Sock, Subs++DirSubs, Unsub),

    ok = case Close of
        true -> ssl:close(Sock);
        _ -> ok
    end.

verify_streams([]) ->
    ok;
verify_streams([Worker | Workers]) when is_atom(Worker) ->
    Check = rpc:call(Worker, ets, tab2list, [session]),

    case get({init, Worker}) of
        undefined -> put({init, Worker}, Check);
        InitCheck -> ct:print("Ets size ~p", [length(Check -- InitCheck)])
    end,
    verify_streams(Workers);
verify_streams(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    verify_streams(Workers).

get_guid(Worker, SessId, Path) ->
    #fuse_response{fuse_response = #guid{guid = Guid}} =
        ?assertMatch(
            #fuse_response{status = #status{code = ?OK}},
            ?req(Worker, SessId, #resolve_guid{path = Path}),
            30
        ),
    Guid.

%%%===================================================================
%%% Internal functions
%%%===================================================================

maybe_create_directory(Sock, SpaceGuid, Directory) ->
    case Directory of
        true ->
            Dirname = generator:gen_name(),
            DirId = fuse_test_utils:create_directory(Sock, SpaceGuid, Dirname),
            {DirId, create_new_file_subscriptions(Sock, DirId, 0)};
        false ->
            {SpaceGuid, []}
    end.

maybe_write(Sock, FileGuid, HandleId, Write) ->
    Data = <<"test_data">>,
    case Write of
        true ->
            SubId = ?SeqID,
            ok = ssl:send(Sock, fuse_test_utils:generate_file_location_changed_subscription_message(
                0, SubId, -SubId, FileGuid, 500)),
            fuse_test_utils:proxy_write(Sock, FileGuid, HandleId, 0, Data),
            fuse_test_utils:emit_file_written_event(Sock, 0, SubId, FileGuid, [#file_block{offset = 0, size = byte_size(Data)}]),
            fuse_test_utils:fsync(Sock, FileGuid, HandleId, false),
            cancel_subscriptions(Sock, 0, [-SubId]),
            Data;
        _ ->
            <<>>
    end.

maybe_read(Sock, FileGuid, HandleId, ExpectedData, Read) ->
    case Read of
        true ->
            SubId1 = ?SeqID,
            ok = ssl:send(Sock, fuse_test_utils:generate_file_location_changed_subscription_message(
                0, SubId1,-SubId1, FileGuid, 500)),
            ?assertMatch(ExpectedData, fuse_test_utils:proxy_read(Sock, FileGuid, HandleId, 0, byte_size(ExpectedData))),
            fuse_test_utils:emit_file_read_event(Sock, 0, SubId1, FileGuid, [#file_block{offset = 0, size = byte_size(ExpectedData)}]),
            fuse_test_utils:fsync(Sock, FileGuid, HandleId, false),
            cancel_subscriptions(Sock, 0, [-SubId1]);
        _ ->
            ok
    end.

maybe_release(Sock, FileGuid, HandleId, Release) ->
    case Release of
        true ->
            fuse_test_utils:close(Sock, FileGuid, HandleId);
        _ ->
            ok
    end.

maybe_ls(Sock, ParentGuid, Directory) ->
    case Directory of
        true ->
            fuse_test_utils:ls(Sock, ParentGuid);
        _ ->
            ok
    end.

maybe_unsub(Sock, Subs, Unsub) ->
    case Unsub of
        true ->
            cancel_subscriptions(Sock, 0, Subs);
        _ ->
            ok
    end.

create_new_file_subscriptions(Sock, Guid, StreamId) ->
    Subs = [Sub1, Sub2, Sub3] = [?SeqID, ?SeqID, ?SeqID],
    ok = ssl:send(Sock, fuse_test_utils:generate_file_removed_subscription_message(
        StreamId, Sub1, -Sub1, Guid)),
    ok = ssl:send(Sock, fuse_test_utils:generate_file_renamed_subscription_message(
        StreamId, Sub2, -Sub2, Guid)),
    ok = ssl:send(Sock, fuse_test_utils:generate_file_attr_changed_subscription_message(
        StreamId, Sub3, -Sub3, Guid, 500)),
    Subs.

cancel_subscriptions(Sock, StreamId, Subs) ->
    lists:foreach(fun(SubId) ->
        ok = ssl:send(Sock, fuse_test_utils:generate_subscription_cancellation_message(StreamId,?SeqID,SubId))
    end, Subs).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) -> initializer:setup_storage(NewConfig) end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer, pool_utils, ?MODULE]} | Config].

init_per_testcase(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    initializer:remove_pending_messages(),
    ssl:start(),
    Config2 = lfm_proxy:init(Config),

    test_utils:mock_new(Workers, user_identity),
    test_utils:mock_expect(Workers, user_identity, get_or_fetch,
        fun(#macaroon_auth{macaroon = ?MACAROON, disch_macaroons = ?DISCH_MACAROONS}) ->
            {ok, #document{value = #user_identity{user_id = <<"user1">>}}}
        end
    ),
    initializer:create_test_users_and_spaces(?TEST_FILE(Config2, "env_desc.json"),
        % Shorten ttl to force quicker client session removal
        [{fuse_session_ttl_seconds, 10} | Config2]).

end_per_testcase(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lfm_proxy:teardown(Config),
    ssl:stop(),
    test_utils:mock_validate_and_unload(Workers, [user_identity]),
    initializer:clean_test_users_and_spaces_no_validate(Config).
