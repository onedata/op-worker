%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for monitoring and restarting DBSync streams
%%% and routing messages to them.
%%% @end
%%%-------------------------------------------------------------------
-module(dbsync_worker2).
-author("Krzysztof Trzepla").

-behaviour(worker_plugin_behaviour).

-include("global_definitions.hrl").
-include("proto/oneprovider/dbsync_messages2.hrl").
-include_lib("ctool/include/logging.hrl").

%% worker_plugin_behaviour callbacks
-export([init/1, handle/1, cleanup/0]).

%% API
-export([supervisor_flags/0]).

-define(DBSYNC_WORKER_SUP, dbsync_worker2_sup).
-define(STREAMS_HEALTHCHECK_INTERVAL, application:get_env(?APP_NAME,
    dbsync_streams_healthcheck_interval, timer:seconds(5))).

%%%===================================================================
%%% worker_plugin_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback init/1.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, worker_host:plugin_state()} | {error, Reason :: term()}.
init(_Args) ->
    couchbase_changes:enable([dbsync_utils2:get_bucket()]),
    start_streams(),
    erlang:send_after(?STREAMS_HEALTHCHECK_INTERVAL, self(),
        {sync_timer, streams_healthcheck}
    ),
    {ok, #{}}.

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback handle/1.
%% @end
%%--------------------------------------------------------------------
-spec handle(ping | healthcheck) -> pong | ok.
handle(ping) ->
    pong;
handle(healthcheck) ->
    ok;
handle(streams_healthcheck) ->
    try
        start_streams()
    catch
        _:Reason ->
            ?error_stacktrace("Failed to start streams due to: ~p", [Reason])
    end,
    erlang:send_after(?STREAMS_HEALTHCHECK_INTERVAL, self(),
        {sync_timer, streams_healthcheck}
    ),
    ok;
handle({dbsync_message, _SessId, Msg = #tree_broadcast2{}}) ->
    handle_tree_broadcast(Msg);
handle({dbsync_message, SessId, Msg = #changes_request2{}}) ->
    handle_changes_request(dbsync_utils2:get_provider(SessId), Msg);
handle({dbsync_message, SessId, Msg = #changes_batch{}}) ->
    handle_changes_batch(dbsync_utils2:get_provider(SessId), undefined, Msg);
handle(Request) ->
    ?log_bad_request(Request).

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback cleanup/0
%% @end
%%--------------------------------------------------------------------
-spec cleanup() -> ok | {error, timeout | term()}.
cleanup() ->
    ok.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns a DBSync worker supervisor flags.
%% @end
%%--------------------------------------------------------------------
-spec supervisor_flags() -> supervisor:sup_flags().
supervisor_flags() ->
    #{strategy => one_for_one, intensity => 0, period => 1}.

%%--------------------------------------------------------------------
%% @doc
%% Returns a child spec for a DBSync in stream worker.
%% @end
%%--------------------------------------------------------------------
-spec dbsync_in_stream_spec(od_space:id()) -> supervisor:child_spec().
dbsync_in_stream_spec(SpaceId) ->
    #{
        id => {dbsync_in_stream, SpaceId},
        start => {dbsync_in_stream, start_link, [SpaceId]},
        restart => temporary,
        shutdown => timer:seconds(10),
        type => worker,
        modules => [dbsync_in_stream]
    }.

%%--------------------------------------------------------------------
%% @doc
%% Returns a child spec for a DBSync out stream worker.
%% @end
%%--------------------------------------------------------------------
-spec dbsync_out_stream_spec(binary(), od_space:id(),
    [dbsync_out_stream:option()]) -> supervisor:child_spec().
dbsync_out_stream_spec(ReqId, SpaceId, Opts) ->
    #{
        id => {dbsync_out_stream, ReqId},
        start => {dbsync_out_stream, start_link, [SpaceId, Opts]},
        restart => temporary,
        shutdown => timer:seconds(10),
        type => worker,
        modules => [dbsync_out_stream]
    }.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Starts incoming and outgoing DBSync streams for all supported spaces.
%% Ignores spaces for which given stream is already present.
%% @end
%%--------------------------------------------------------------------
-spec start_streams() -> ok.
start_streams() ->
    lists:foreach(fun(Module) ->
        lists:foreach(fun(SpaceId) ->
            Name = {Module, SpaceId},
            Pid = global:whereis_name(Name),
            Node = consistent_hasing:get_node(Name),
            case {Pid, Node =:= node(), Module} of
                {undefined, true, dbsync_in_stream} ->
                    start_in_stream(SpaceId);
                {undefined, true, dbsync_out_stream} ->
                    start_out_stream(SpaceId);
                _ ->
                    ok
            end
        end, dbsync_utils2:get_spaces())
    end, [dbsync_in_stream, dbsync_out_stream]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Starts incoming DBSync stream for a given space.
%% @end
%%--------------------------------------------------------------------
-spec start_in_stream(od_space:id()) -> supervisor:startchild_ret().
start_in_stream(SpaceId) ->
    Spec = dbsync_in_stream_spec(SpaceId),
    supervisor:start_child(?DBSYNC_WORKER_SUP, Spec).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Starts outgoing DBSync stream for a given space.
%% @end
%%--------------------------------------------------------------------
-spec start_out_stream(od_space:id()) -> supervisor:startchild_ret().
start_out_stream(SpaceId) ->
    Filter = fun
        (#document{mutator = [Mutator | _]}) ->
            Mutator =:= oneprovider:get_provider_id();
        (#document{}) ->
            false
    end,
    Handler = fun
        (Since, Until, Docs) when Since =:= Until ->
            dbsync_communicator:broadcast_changes(SpaceId, Since, Until, Docs);
        (Since, Until, Docs) ->
            ProviderId = oneprovider:get_provider_id(),
            dbsync_communicator:broadcast_changes(SpaceId, Since, Until, Docs),
            dbsync_state2:set_seq(SpaceId, ProviderId, Until)
    end,
    Spec = dbsync_out_stream_spec(SpaceId, SpaceId, [
        {register, true},
        {filter, Filter},
        {handler, Handler},
        {handling_interval, application:get_env(
            ?APP_NAME, dbsync_changes_broadcast_interval, timer:seconds(5)
        )}
    ]),
    supervisor:start_child(?DBSYNC_WORKER_SUP, Spec).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Forwards changes batch to an associated incoming stream.
%% @end
%%--------------------------------------------------------------------
-spec handle_changes_batch(od_provider:id(), undefined |
    dbsync_communicator:msg_id(), dbsync_communicator:changes_batch()) -> ok.
handle_changes_batch(ProviderId, MsgId, #changes_batch{
    space_id = SpaceId,
    since = Since,
    until = Until,
    compressed_docs = CompressedDocs
}) ->
    Name = {dbsync_in_stream, SpaceId},
    Docs = dbsync_utils2:uncompress(CompressedDocs),
    gen_server:cast(
        {global, Name}, {changes_batch, MsgId, ProviderId, Since, Until, Docs}
    ).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Starts outgoing DBSync recovery stream.
%% @end
%%--------------------------------------------------------------------
-spec handle_changes_request(od_provider:id(),
    dbsync_communicator:changes_request()) -> supervisor:startchild_ret().
handle_changes_request(ProviderId, #changes_request2{
    space_id = SpaceId,
    since = Since,
    until = Until
}) ->
    Handler = fun
        (BatchSince, end_of_stream, Docs) ->
            dbsync_communicator:send_changes(
                ProviderId, SpaceId, BatchSince, Until, Docs
            );
        (BatchSince, BatchUntil, Docs) ->
            dbsync_communicator:send_changes(
                ProviderId, SpaceId, BatchSince, BatchUntil, Docs
            )
    end,
    ReqId = dbsync_utils2:gen_request_id(),
    Spec = dbsync_out_stream_spec(ReqId, SpaceId, [
        {since, Since},
        {until, Until},
        {except_mutator, ProviderId},
        {handler, Handler},
        {handling_interval, application:get_env(
            ?APP_NAME, dbsync_changes_resend_interval, timer:seconds(1)
        )}
    ]),
    supervisor:start_child(?DBSYNC_WORKER_SUP, Spec).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles changes batch and forwards broadcast message.
%% @end
%%--------------------------------------------------------------------
-spec handle_tree_broadcast(dbsync_communicator:tree_broadcast()) -> ok.
handle_tree_broadcast(BroadcastMsg = #tree_broadcast2{
    src_provider_id = SrcProviderId,
    message_id = MsgId,
    message_body = Msg = #changes_batch{}
}) ->
    handle_changes_batch(SrcProviderId, MsgId, Msg),
    dbsync_communicator:forward(BroadcastMsg).