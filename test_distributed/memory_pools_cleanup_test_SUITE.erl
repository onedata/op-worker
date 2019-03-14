%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module tests cleanup of memory pools
%%% @end
%%%--------------------------------------------------------------------
-module(memory_pools_cleanup_test_SUITE).
-author("Michal Stanisz").

-include("fuse_test_utils.hrl").
-include("proto/oneclient/event_messages.hrl").
-include("proto/oneclient/client_messages.hrl").
-include_lib("clproto/include/messages.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_links.hrl").

%% export for ct
-export([all/0, init_per_suite/1, init_per_testcase/2,
    end_per_testcase/2, end_per_suite/1]).

-export([
    connection_closed_after_file_create/1,
    connection_closed_after_file_create_write/1,
    connection_closed_after_file_create_read/1,
    connection_closed_after_file_create_write_read/1,
    connection_closed_after_file_create_release/1,
    connection_closed_after_file_create_write_release/1,
    connection_closed_after_file_create_read_release/1,
    connection_closed_after_file_create_write_read_release/1,
    connection_closed_after_file_create_unsub/1,
    connection_closed_after_file_create_write_unsub/1,
    connection_closed_after_file_create_read_unsub/1,
    connection_closed_after_file_create_write_read_unsub/1,
    connection_closed_after_file_create_release_unsub/1,
    connection_closed_after_file_create_write_release_unsub/1,
    connection_closed_after_file_create_read_release_unsub/1,
    connection_closed_after_file_create_write_read_release_unsub/1,
    connection_closed_after_dir_create_ls/1,
    connection_closed_after_dir_create_ls_unsub/1
]).

all() ->
    ?ALL([
        connection_closed_after_file_create,
        connection_closed_after_file_create_write,
        connection_closed_after_file_create_read,
        connection_closed_after_file_create_write_read,
        connection_closed_after_file_create_release,
        connection_closed_after_file_create_write_release,
        connection_closed_after_file_create_read_release,
        connection_closed_after_file_create_write_read_release,
        connection_closed_after_file_create_unsub,
        connection_closed_after_file_create_write_unsub,
        connection_closed_after_file_create_read_unsub,
        connection_closed_after_file_create_write_read_unsub,
        connection_closed_after_file_create_release_unsub,
        connection_closed_after_file_create_write_release_unsub,
        connection_closed_after_file_create_read_release_unsub,
        connection_closed_after_file_create_write_read_release_unsub,
        connection_closed_after_dir_create_ls,
        connection_closed_after_dir_create_ls_unsub
    ]).

connection_closed_after_file_create(Config) ->
    memory_pools_cleared_after_disconnection_test_base(Config, []).

connection_closed_after_file_create_write(Config) ->
    memory_pools_cleared_after_disconnection_test_base(Config, [write]).

connection_closed_after_file_create_read(Config) ->
    memory_pools_cleared_after_disconnection_test_base(Config, [read]).

connection_closed_after_file_create_write_read(Config) ->
    memory_pools_cleared_after_disconnection_test_base(Config, [write, read]).

connection_closed_after_file_create_release(Config) ->
    memory_pools_cleared_after_disconnection_test_base(Config, [release]).

connection_closed_after_file_create_write_release(Config) ->
    memory_pools_cleared_after_disconnection_test_base(Config, [write, release]).

connection_closed_after_file_create_read_release(Config) ->
    memory_pools_cleared_after_disconnection_test_base(Config, [read, release]).

connection_closed_after_file_create_write_read_release(Config) ->
    memory_pools_cleared_after_disconnection_test_base(Config, [write, read, release]).

connection_closed_after_file_create_unsub(Config) ->
    memory_pools_cleared_after_disconnection_test_base(Config, [unsub]).

connection_closed_after_file_create_write_unsub(Config) ->
    memory_pools_cleared_after_disconnection_test_base(Config, [write, unsub]).

connection_closed_after_file_create_read_unsub(Config) ->
    memory_pools_cleared_after_disconnection_test_base(Config, [read, unsub]).

connection_closed_after_file_create_write_read_unsub(Config) ->
    memory_pools_cleared_after_disconnection_test_base(Config, [write, read, unsub]).

connection_closed_after_file_create_release_unsub(Config) ->
    memory_pools_cleared_after_disconnection_test_base(Config, [release, unsub]).

connection_closed_after_file_create_write_release_unsub(Config) ->
    memory_pools_cleared_after_disconnection_test_base(Config, [write, release, unsub]).

connection_closed_after_file_create_read_release_unsub(Config) ->
    memory_pools_cleared_after_disconnection_test_base(Config, [read, release, unsub]).

connection_closed_after_file_create_write_read_release_unsub(Config) ->
    memory_pools_cleared_after_disconnection_test_base(Config, [write, read, release, unsub]).

connection_closed_after_dir_create_ls(Config) ->
    memory_pools_cleared_after_disconnection_test_base(Config, [directory]).

connection_closed_after_dir_create_ls_unsub(Config) ->
    memory_pools_cleared_after_disconnection_test_base(Config, [directory, unsub]).


-define(req(W, SessId, FuseRequest), element(2, rpc:call(W, worker_proxy, call,
    [fslogic_worker, {fuse_request, SessId, #fuse_request{fuse_request = FuseRequest}}]))).

-define(SeqID, erlang:unique_integer([positive, monotonic]) - 2).

%%%===================================================================
%%% Test base
%%%===================================================================

memory_pools_cleared_after_disconnection_test_base(Config, Args) ->
    AllArgs = [write, read, release, unsub, directory],
    [Write, Read, Release, Unsub, Directory] = lists:map(fun(A) -> lists:member(A, Args) end, AllArgs),
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker1)}}, Config),
    SpaceGuid = get_guid(Worker1, SessionId, <<"/space_name1">>),

    {ok, {_, RootHandle}} = ?assertMatch({ok, _}, lfm_proxy:create_and_open(Worker1, <<"0">>, SpaceGuid,
        generator:gen_name(), 8#755)),
    ?assertEqual(ok, lfm_proxy:close(Worker1, RootHandle)),

    {ok, {Sock, _}} = fuse_test_utils:connect_via_macaroon(Worker1, [{active, true}], SessionId),

    {Before, _SizesBefore} = get_memory_pools_entries_and_sizes(Worker1),

    Filename = generator:gen_name(),
    {ParentGuid, DirSubs} = maybe_create_directory(Sock, SpaceGuid, Directory),

    {FileGuid, HandleId} = fuse_test_utils:create_file(Sock, ParentGuid, Filename),
    Subs = create_new_file_subscriptions(Sock, FileGuid, 0),

    fuse_test_utils:fuse_test_utils(Sock, FileGuid, HandleId, false),

    ExpectedData = maybe_write(Sock, FileGuid, HandleId, Write),
    maybe_read(Sock, FileGuid, HandleId, ExpectedData, Read),
    maybe_release(Sock, FileGuid, HandleId, Release),
    maybe_ls(Sock, ParentGuid, Directory),
    maybe_unsub(Sock, Subs++DirSubs, Unsub),
    ok = ssl:close(Sock),

    timer:sleep(timer:seconds(30)),

    {After, _SizesAfter} = get_memory_pools_entries_and_sizes(Worker1),
    Res = get_documents_diff(Worker1, After, Before),
    ?assertEqual([], Res).

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

get_guid(Worker, SessId, Path) ->
    #fuse_response{fuse_response = #guid{guid = Guid}} =
        ?assertMatch(
            #fuse_response{status = #status{code = ?OK}},
            ?req(Worker, SessId, #resolve_guid{path = Path}),
            30
        ),
    Guid.

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

get_memory_pools_entries_and_sizes(Worker) ->
    MemoryPools = rpc:call(Worker, datastore_multiplier, get_names, [memory]),
    Entries = lists:map(fun(Pool) ->
        PoolName = list_to_atom("datastore_cache_active_pool_" ++ atom_to_list(Pool)),
        rpc:call(Worker, ets, foldl, [fun(Entry, Acc) -> Acc ++ [Entry] end, [], PoolName])
    end, MemoryPools),
    Sizes = lists:map(fun(Slot) -> rpc:call(Worker, datastore_cache_manager, get_size, [Slot]) end, MemoryPools),
    {Entries, Sizes}.

get_documents_diff(Worker, After, Before) ->
    Ans = lists:flatten(lists:zipwith(fun(A,B) ->
        Diff = A--B,
        lists:map(fun({Key, Driver, DriverCtx}) ->
            rpc:call(Worker, Driver, get, [DriverCtx, Key])
        end, [{Key, Driver, DriverCtx} || {_,Key,_,_,Driver, DriverCtx} <- Diff]) 
    end, After, Before)),
    lists:filter(fun
        ({ok, #document{value = #links_node{model = luma_cache}}}) -> false;
        ({ok, #document{value = #links_forest{model = luma_cache}}}) -> false;
        ({ok, #document{value = #links_node{model = task_pool}}}) -> false;
        ({ok, #document{value = #links_forest{model = task_pool}}}) -> false;
        ({ok, #document{value = #task_pool{}}}) -> false;
        ({ok, #document{value = #permissions_cache{}}}) -> false;
        ({ok, #document{value = #permissions_cache_helper{}}}) -> false;
        ({ok, #document{value = #permissions_cache_helper2{}}}) -> false;
        (_) -> true
    end, Ans).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) -> initializer:setup_storage(NewConfig) end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].

init_per_testcase(_Case, Config) ->
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

end_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lfm_proxy:teardown(Config),
    ssl:stop(),
    test_utils:mock_validate_and_unload(Workers, [user_identity]),
    initializer:clean_test_users_and_spaces_no_validate(Config).

end_per_suite(_Case) ->
    ok.
