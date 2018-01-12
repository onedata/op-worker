%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains DBSync tests.
%%% @end
%%%-------------------------------------------------------------------
-module(dbsync_test_SUITE).
-author("Krzysztof Trzepla").

-include("modules/datastore/datastore_models.hrl").
-include("proto/oneprovider/dbsync_messages2.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    stream_should_be_started_on_init/1,
    stream_should_be_stared_for_new_space/1,
    stream_should_be_restarted_after_crash/1,
    status_should_be_broadcast_periodically/1,
    local_changes_should_be_broadcast/1,
    remote_changes_should_not_be_broadcast/1,
    remote_changes_should_be_applied/1,
    resent_changes_should_be_applied/1,
    remote_changes_duplicates_should_be_ignored/1,
    remote_changes_should_be_forwarded/1,
    missing_changes_should_be_requested/1,
    future_changes_should_be_stashed/1,
    changes_request_should_be_handled/1
]).

all() ->
    ?ALL([
        stream_should_be_started_on_init,
        stream_should_be_stared_for_new_space,
        stream_should_be_restarted_after_crash,
        status_should_be_broadcast_periodically,
        local_changes_should_be_broadcast,
        remote_changes_should_not_be_broadcast,
        remote_changes_should_be_applied,
        resent_changes_should_be_applied,
        remote_changes_duplicates_should_be_ignored,
        remote_changes_should_be_forwarded,
        missing_changes_should_be_requested,
        future_changes_should_be_stashed,
        changes_request_should_be_handled
    ]).

-define(DOC(Mutator, Seq), #document{
    mutators = [Mutator],
    seq = Seq,
    value = crypto:strong_rand_bytes(16)
}).
-define(TIMEOUT, timer:seconds(30)).
-define(ATTEMPTS, 30).

-define(call(Worker, ProviderId, Request), worker_proxy:call(
    {dbsync_worker, Worker},
    {dbsync_message, get_provider_session(ProviderId), Request}
)).

%%%====================================================================
%%% Test function
%%%====================================================================

stream_should_be_started_on_init(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SpaceIds = ?config(spaces, Config),

    lists:foreach(fun(Module) ->
        lists:foreach(fun(SpaceId) ->
            ?assertEqual(true, is_pid(rpc:call(Worker, global, whereis_name,
                [{Module, SpaceId}]
            )), ?ATTEMPTS)
        end, SpaceIds)
    end, [dbsync_in_stream, dbsync_out_stream]).

stream_should_be_stared_for_new_space(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SpaceIds = ?config(spaces, Config),
    NewSpaceId = <<"s4">>,
    test_utils:mock_expect(Worker, dbsync_utils, get_spaces, fun() ->
        [NewSpaceId | SpaceIds]
    end),

    lists:foreach(fun(Module) ->
        ?assertEqual(true, is_pid(rpc:call(Worker, global, whereis_name,
            [{Module, NewSpaceId}]
        )), ?ATTEMPTS)
    end, [dbsync_in_stream, dbsync_out_stream]),

    lists:keystore(spaces, 1, Config, {spaces, [NewSpaceId | SpaceIds]}).

stream_should_be_restarted_after_crash(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    [SpaceId | _] = ?config(spaces, Config),

    lists:foreach(fun(Module) ->
        ?assertEqual(true, is_pid(rpc:call(Worker, global, whereis_name,
            [{Module, SpaceId}]
        )), ?ATTEMPTS),
        Pid = rpc:call(Worker, global, whereis_name, [{Module, SpaceId}]),
        exit(Pid, kill),
        ?assertEqual(true, is_pid(rpc:call(Worker, global, whereis_name,
            [{Module, SpaceId}]
        )) andalso Pid =/= rpc:call(Worker, global, whereis_name,
            [{Module, SpaceId}]
        ), ?ATTEMPTS)
    end, [dbsync_in_stream, dbsync_out_stream]).

status_should_be_broadcast_periodically(Config) ->
    lists:foreach(fun(SpaceId) ->
        ProviderIds = lists:usort(get_providers(SpaceId)),
        ProviderIds2 = lists:delete(<<"p1">>, ProviderIds),
        LowProviderId = hd(ProviderIds2),
        HighProviderId = lists:last(ProviderIds2),
        ?assertReceivedMatch({send, _, #tree_broadcast2{
            src_provider_id = <<"p1">>,
            low_provider_id = LowProviderId,
            high_provider_id = HighProviderId,
            message_body = #changes_batch{
                space_id = SpaceId,
                since = 1,
                until = 1,
                compressed_docs = []
            }
        }}, ?TIMEOUT)
    end, ?config(spaces, Config)).

local_changes_should_be_broadcast(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    lists:foreach(fun(SpaceId) ->
        Pid = rpc:call(Worker, global, whereis_name, [
            {dbsync_out_stream, SpaceId}
        ]),
        Docs = lists:map(fun(N) ->
            timer:sleep(100),
            Doc = ?DOC(<<"p1">>, N),
            gen_server:cast(Pid, {change, {ok, Doc}}),
            Doc
        end, lists:seq(1, 100)),
        assert_broadcast(Docs, <<"p1">>, get_providers(SpaceId))
    end, ?config(spaces, Config)).

remote_changes_should_not_be_broadcast(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    lists:foreach(fun(SpaceId) ->
        Pid = rpc:call(Worker, global, whereis_name, [
            {dbsync_out_stream, SpaceId}
        ]),
        lists:foreach(fun(ProviderId) ->
            Doc = ?DOC(ProviderId, 1),
            gen_server:cast(Pid, {change, {ok, Doc}})
        end, lists:delete(<<"p1">>, get_providers(SpaceId))),
        ?assertNotReceivedMatch({send, _, #tree_broadcast2{
            message_body = #changes_batch{compressed_docs = [_ | _]}
        }}, 500)
    end, ?config(spaces, Config)).

remote_changes_should_be_applied(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    lists:foreach(fun(SpaceId) ->
        lists:foreach(fun(ProviderId) ->
            Docs = lists:map(fun(N) ->
                Doc = ?DOC(ProviderId, N),
                ?call(Worker, ProviderId, #tree_broadcast2{
                    src_provider_id = ProviderId,
                    low_provider_id = <<"p1">>,
                    high_provider_id = <<"p1">>,
                    message_id = crypto:strong_rand_bytes(16),
                    message_body = #changes_batch{
                        space_id = SpaceId,
                        since = N,
                        until = N + 1,
                        compressed_docs = [Doc]
                    }
                }),
                Doc
            end, lists:seq(1, 10)),
            lists:foreach(fun(Doc) ->
                ?assertReceivedMatch({apply, Doc}, ?TIMEOUT)
            end, Docs)
        end, lists:delete(<<"p1">>, get_providers(SpaceId)))
    end, ?config(spaces, Config)).

resent_changes_should_be_applied(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    lists:foreach(fun(SpaceId) ->
        lists:foreach(fun(ProviderId) ->
            Doc = ?DOC(ProviderId, 1),
            Doc2 = ?DOC(ProviderId, 2),
            ?call(Worker, ProviderId, #tree_broadcast2{
                src_provider_id = ProviderId,
                low_provider_id = <<"p1">>,
                high_provider_id = <<"p1">>,
                message_id = crypto:strong_rand_bytes(16),
                message_body = #changes_batch{
                    space_id = SpaceId,
                    since = 2,
                    until = 3,
                    compressed_docs = [Doc2]
                }
            }),
            ?call(Worker, ProviderId, #changes_batch{
                space_id = SpaceId,
                since = 1,
                until = 2,
                compressed_docs = [Doc]
            }),
            ?assertReceivedMatch({apply, Doc}, ?TIMEOUT),
            ?assertReceivedMatch({apply, Doc2}, ?TIMEOUT)
        end, lists:delete(<<"p1">>, get_providers(SpaceId)))
    end, ?config(spaces, Config)).

remote_changes_duplicates_should_be_ignored(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    lists:foreach(fun(SpaceId) ->
        lists:foreach(fun(ProviderId) ->
            Doc = ?DOC(ProviderId, 1),
            Request = #tree_broadcast2{
                src_provider_id = ProviderId,
                low_provider_id = <<"p1">>,
                high_provider_id = <<"p1">>,
                message_id = crypto:strong_rand_bytes(16),
                message_body = #changes_batch{
                    space_id = SpaceId,
                    since = 1,
                    until = 2,
                    compressed_docs = [Doc]
                }
            },
            ?call(Worker, ProviderId, Request),
            ?call(Worker, ProviderId, Request),
            ?assertReceivedMatch({apply, Doc}, ?TIMEOUT),
            ?assertNotReceivedMatch({apply, Doc}, 500)
        end, lists:delete(<<"p1">>, get_providers(SpaceId)))
    end, ?config(spaces, Config)).

remote_changes_should_be_forwarded(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    lists:foreach(fun(SpaceId) ->
        ProviderIds = lists:usort(get_providers(SpaceId)),
        ProviderIds2 = lists:delete(<<"p1">>, ProviderIds),
        lists:foreach(fun(SrcProviderId) ->
            lists:foreach(fun(N) ->
                Doc = ?DOC(SrcProviderId, N),
                Request = #tree_broadcast2{
                    src_provider_id = SrcProviderId,
                    low_provider_id = <<"p1">>,
                    high_provider_id = lists:nth(N, ProviderIds2),
                    message_id = crypto:strong_rand_bytes(16),
                    message_body = #changes_batch{
                        space_id = SpaceId,
                        since = N,
                        until = N + 1,
                        compressed_docs = [Doc]
                    }
                },
                ?call(Worker, SrcProviderId, Request),
                RecvProviderIds = lists:sublist(ProviderIds2, N),
                assert_forward([Doc], SrcProviderId, RecvProviderIds)
            end, lists:seq(1, length(ProviderIds2)))
        end, ProviderIds2)
    end, ?config(spaces, Config)).

missing_changes_should_be_requested(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    lists:foreach(fun(SpaceId) ->
        ProviderIds = lists:usort(get_providers(SpaceId)),
        ProviderIds2 = lists:delete(<<"p1">>, ProviderIds),
        lists:foreach(fun(SrcProviderId) ->
            Doc = ?DOC(SrcProviderId, 10),
            Request = #tree_broadcast2{
                src_provider_id = SrcProviderId,
                low_provider_id = <<"p1">>,
                high_provider_id = lists:last(ProviderIds2),
                message_id = crypto:strong_rand_bytes(16),
                message_body = #changes_batch{
                    space_id = SpaceId,
                    since = 10,
                    until = 11,
                    compressed_docs = [Doc]
                }
            },
            ?call(Worker, SrcProviderId, Request),
            lists:foreach(fun(_) ->
                ?assertReceivedMatch({send, SrcProviderId, #changes_request2{
                    space_id = SpaceId,
                    since = 1,
                    until = 10
                }}, ?TIMEOUT)
            end, lists:seq(1, 3))
        end, ProviderIds2)
    end, ?config(spaces, Config)).

future_changes_should_be_stashed(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    lists:foreach(fun(SpaceId) ->
        ProviderIds = lists:usort(get_providers(SpaceId)),
        ProviderIds2 = lists:delete(<<"p1">>, ProviderIds),
        lists:foreach(fun(SrcProviderId) ->
            Docs = lists:map(fun(N) ->
                Doc = ?DOC(SrcProviderId, N),
                Request = #tree_broadcast2{
                    src_provider_id = SrcProviderId,
                    low_provider_id = <<"p1">>,
                    high_provider_id = lists:last(ProviderIds2),
                    message_id = crypto:strong_rand_bytes(16),
                    message_body = #changes_batch{
                        space_id = SpaceId,
                        since = N,
                        until = N + 1,
                        compressed_docs = [Doc]
                    }
                },
                ?call(Worker, SrcProviderId, Request),
                case N of
                    1 -> ok;
                    _ ->
                        ?assertReceivedMatch({send, SrcProviderId,
                            #changes_request2{
                                space_id = SpaceId,
                                since = 1,
                                until = N
                            }
                        }, ?TIMEOUT),
                        ?assertNotReceivedMatch({apply, Doc}, 500)
                end,
                Doc
            end, lists:seq(10, 1, -1)),
            lists:foreach(fun(Doc) ->
                ?assertReceivedMatch({apply, Doc}, ?TIMEOUT)
            end, lists:reverse(Docs))
        end, ProviderIds2)
    end, ?config(spaces, Config)).

changes_request_should_be_handled(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    lists:foreach(fun(SpaceId) ->
        ProviderIds = lists:usort(get_providers(SpaceId)),
        ProviderIds2 = lists:delete(<<"p1">>, ProviderIds),
        lists:foreach(fun(ProviderId) ->
            StreamId = <<SpaceId/binary, "-", ProviderId/binary>>,
            test_utils:mock_expect(Worker, dbsync_utils, gen_request_id, fun() ->
                StreamId
            end),
            Request = #changes_request2{
                space_id = SpaceId,
                since = 1,
                until = 2
            },
            ?call(Worker, ProviderId, Request),
            Children = rpc:call(Worker, supervisor, which_children, [
                dbsync_worker_sup
            ]),
            {_, Pid, _, _} = lists:keyfind(
                {dbsync_out_stream, StreamId}, 1, Children
            ),
            ?assertEqual(true, is_pid(Pid)),
            Doc = ?DOC(<<"p1">>, 1),
            gen_server:cast(Pid, {change, {ok, Doc}}),
            ?assertReceivedMatch({send, ProviderId, #changes_batch{
                space_id = SpaceId,
                since = 1,
                until = 2,
                compressed_docs = [Doc]
            }}, ?TIMEOUT)
        end, ProviderIds2)
    end, ?config(spaces, Config)).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_testcase(_Case, Config) ->
    [Worker | _] = Workers = ?config(op_worker_nodes, Config),
    lists:foreach(fun(W) ->
        test_utils:set_env(W, ?APP_NAME,
            multipath_broadcast_min_support, 1)
    end, Workers),
    Self = self(),

    test_utils:mock_new(Worker, [
        dbsync_changes, dbsync_communicator
    ]),

    initializer:mock_provider_id(
        Workers, <<"p1">>, <<"auth-macaroon">>, <<"identity-macaroon">>
    ),

    test_utils:mock_expect(Worker, dbsync_utils, get_spaces, fun() ->
        [<<"s1">>, <<"s2">>, <<"s3">>]
    end),
    test_utils:mock_expect(Worker, dbsync_utils, get_provider, fun
        (<<"sp1">>) -> <<"p1">>;
        (<<"sp2">>) -> <<"p2">>;
        (<<"sp3">>) -> <<"p3">>;
        (<<"sp4">>) -> <<"p4">>;
        (<<"sp5">>) -> <<"p5">>
    end),
    test_utils:mock_expect(Worker, dbsync_utils, get_providers, fun
        (SpaceId) -> get_providers(SpaceId)
    end),
    test_utils:mock_expect(Worker, dbsync_utils, compress, fun
        (Docs) -> Docs
    end),
    test_utils:mock_expect(Worker, dbsync_utils, uncompress, fun
        (CompressedDocs) -> CompressedDocs
    end),
    test_utils:mock_expect(Worker, dbsync_communicator, send, fun
        (ProviderId, Msg) -> Self ! {send, ProviderId, Msg}, ok
    end),
    test_utils:mock_expect(Worker, dbsync_changes, apply, fun
        (Doc) ->
            Self ! {apply, Doc},
            ok
    end),

    lists:foreach(fun({Name, Value}) ->
        test_utils:set_env(Worker, op_worker, Name, Value)
    end, [
        {dbsync_changes_request_delay, timer:seconds(1)},
        {dbsync_changes_broadcast_interval, timer:seconds(1)}
    ]),
    rpc:call(Worker, worker_proxy, call, [dbsync_worker, streams_healthcheck]),

    [{spaces, [<<"s1">>, <<"s2">>, <<"s3">>]} | Config].

end_per_testcase(_Case, Config) ->
    [Worker | _] = Workers = ?config(op_worker_nodes, Config),
    lists:foreach(fun(SpaceId) ->
        rpc:call(Worker, dbsync_state, delete, [SpaceId]),
        lists:foreach(fun(Module) ->
            Pid = rpc:call(Worker, global, whereis_name, [{Module, SpaceId}]),
            exit(Pid, kill)
        end, [dbsync_in_stream, dbsync_out_stream])
    end, ?config(spaces, Config)),
    initializer:unmock_provider_ids(Workers),
    test_utils:mock_unload(Worker, [
        dbsync_changes, dbsync_communicator
    ]).

%%%===================================================================
%%% Internal functions
%%%===================================================================

get_providers(SpaceId) ->
    ProviderIds = case SpaceId of
        (<<"s1">>) -> [<<"p1">>, <<"p2">>, <<"p3">>, <<"p4">>, <<"p5">>];
        (<<"s2">>) -> [<<"p1">>, <<"p2">>, <<"p3">>];
        (<<"s3">>) -> [<<"p1">>, <<"p4">>, <<"p5">>];
        (<<"s4">>) -> [<<"p2">>, <<"p3">>, <<"p4">>, <<"p5">>]
    end,
    utils:random_shuffle(ProviderIds).

get_provider_session(ProviderId) ->
    <<"s", ProviderId/binary>>.

assert_forward(_, _, []) ->
    ok;
assert_forward(Docs, SrcProviderId, RecvProviderIds) ->
    RecvProviderIds2 = lists:delete(SrcProviderId, RecvProviderIds),
    RecvProviders = lists:map(fun(RecvProviderId) ->
        {RecvProviderId, {Docs, []}}
    end, RecvProviderIds2),
    assert_broadcast(SrcProviderId, RecvProviders).

assert_broadcast(_, _, []) ->
    ok;
assert_broadcast(Docs, SrcProviderId, RecvProviderIds) ->
    RecvProviderIds2 = lists:delete(SrcProviderId, RecvProviderIds),
    RecvProviders = lists:map(fun(RecvProviderId) ->
        {RecvProviderId, {Docs, Docs}}
    end, RecvProviderIds2),
    assert_broadcast(SrcProviderId, RecvProviders).

assert_broadcast(_SrcProviderId, []) ->
    ok;
assert_broadcast(SrcProviderId, RecvProviders) ->
    {send, RecvProviderId, #tree_broadcast2{
        low_provider_id = LowProviderId,
        high_provider_id = HighProviderId,
        message_body = #changes_batch{
            compressed_docs = RecvDocs
        }
    }} = ?assertReceivedMatch({send, _, #tree_broadcast2{
        src_provider_id = SrcProviderId,
        message_body = #changes_batch{compressed_docs = [_ | _]}
    }}, ?TIMEOUT),

    ?assertEqual(true, RecvProviderId =:= LowProviderId orelse
        RecvProviderId =:= HighProviderId),

    RecvProviders3 = lists:foldl(fun({ProviderId, Docs}, RecvProviders2) ->
        case LowProviderId =< ProviderId andalso ProviderId =< HighProviderId of
            true ->
                Docs3 = lists:foldl(fun
                    (RecvDoc, {[Doc | LDocs], RDocs}) when Doc =:= RecvDoc ->
                        {LDocs, RDocs};
                    (RecvDoc, {LDocs, [Doc | RDocs]}) when Doc =:= RecvDoc ->
                        {LDocs, RDocs}
                end, Docs, RecvDocs),
                case Docs3 of
                    {[], []} -> RecvProviders2;
                    _ -> [{ProviderId, Docs3} | RecvProviders2]
                end;
            false ->
                [{ProviderId, Docs} | RecvProviders2]
        end
    end, [], RecvProviders),

    assert_broadcast(SrcProviderId, RecvProviders3).
