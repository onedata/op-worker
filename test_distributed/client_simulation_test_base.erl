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
-include("test_utils/initializer.hrl").
-include_lib("clproto/include/messages.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_links.hrl").

-export([init_per_suite/1, init_per_testcase/1, end_per_testcase/1]).
-export([simulate_client/5, verify_streams/1, verify_streams/2, get_guid/3]).
-export([prepare_file/2, use_file/4]).

-define(req(W, SessId, FuseRequest), element(2, rpc:call(W, worker_proxy, call,
    [fslogic_worker, {fuse_request, SessId, #fuse_request{fuse_request = FuseRequest}}]))).

-define(SeqID, erlang:unique_integer([positive, monotonic]) - 1).
-define(MsgID, integer_to_binary(fuse_test_utils:generate_msg_id())).

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
    % TODO VFS-5363 - check if streams are defined
%%    verify_streams(Config),
    maybe_release(Sock, FileGuid, HandleId, Release),
    maybe_ls(Sock, ParentGuid, Directory),
    maybe_unsub(Sock, Subs++DirSubs, Unsub),

    ok = case Close of
        true -> ssl:close(Sock);
        _ -> ok
    end.

prepare_file(Sock, SpaceGuid) ->
    Filename = generator:gen_name(),
    {FileGuid, HandleId} = fuse_test_utils:create_file(Sock, SpaceGuid, Filename),
     create_new_file_subscriptions(Sock, FileGuid, 0),
    fuse_test_utils:fsync(Sock, FileGuid, HandleId, false),

    SubId = ?SeqID,
    ok = ssl:send(Sock, fuse_test_utils:generate_file_location_changed_subscription_message(
        0, SubId, -SubId, FileGuid, 500)),

    {FileGuid, HandleId, SubId}.

use_file(Sock, FileGuid, HandleId, SubId) ->
    Data = <<"test_data">>,
    fuse_test_utils:proxy_write(Sock, FileGuid, HandleId, 0, Data),
    fuse_test_utils:emit_file_written_event(Sock, 0, SubId, FileGuid, [#file_block{offset = 0, size = byte_size(Data)}]),

    ?assertMatch(Data, fuse_test_utils:proxy_read(Sock, FileGuid, HandleId, 0, byte_size(Data))),
    fuse_test_utils:emit_file_read_event(Sock, 0, SubId, FileGuid, [#file_block{offset = 0, size = byte_size(Data)}]),

    fuse_test_utils:fsync(Sock, FileGuid, HandleId, false).

verify_streams(Workers) ->
    verify_streams(Workers, true).

verify_streams([], _SessionClosed) ->
    ok;
verify_streams([Worker | Workers], SessionClosed) when is_atom(Worker) ->
    Check = rpc:call(Worker, ets, tab2list, [session]),

    case get({init, Worker}) of
        undefined -> put({init, Worker}, Check);
        InitCheck ->
            Diff = Check -- InitCheck,
            case SessionClosed of
                true ->
                    ?assertEqual([], Diff);
                _ ->
                    case length(Diff) =< 2 of
                        true -> ok; % two elements for with ref for sequencer_in_stream may exist if session is open
                        _ -> ?assertEqual([], Diff)
                    end
            end
    end,
    verify_streams(Workers, SessionClosed);
verify_streams(Config, SessionClosed) ->
    Workers = ?config(op_worker_nodes, Config),
    verify_streams(Workers, SessionClosed).

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
    [Seq1, Seq2, Seq3] = [?SeqID, ?SeqID, ?SeqID],
    ok = ssl:send(Sock, fuse_test_utils:generate_file_removed_subscription_message(
        StreamId, Seq1, -Seq1, Guid)),
    ok = ssl:send(Sock, fuse_test_utils:generate_file_renamed_subscription_message(
        StreamId, Seq2, -Seq2, Guid)),
    ok = ssl:send(Sock, fuse_test_utils:generate_file_attr_changed_subscription_message(
        StreamId, Seq3, -Seq3, Guid, 500)),
    [-Seq1, -Seq2, -Seq3]. % Return subscription numbers (opposite to sequence numbers)

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
    initializer:remove_pending_messages(),
    ssl:start(),
    Config2 = lfm_proxy:init(Config),

    initializer:mock_auth_manager(Config),
    initializer:create_test_users_and_spaces(?TEST_FILE(Config2, "env_desc.json"),
        % Shorten ttl to force quicker client session removal
        [{fuse_session_grace_period_seconds, 10} | Config2]).

end_per_testcase(Config) ->
    lfm_proxy:teardown(Config),
    ssl:stop(),
    initializer:unmock_auth_manager(Config),
    initializer:clean_test_users_and_spaces_no_validate(Config).
