%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for monitoring and restarting DBSync streams
%%% and routing messages to them.
%%% @end
%%%-------------------------------------------------------------------
-module(dbsync_worker).
-author("Krzysztof Trzepla").

-behaviour(worker_plugin_behaviour).

-include("global_definitions.hrl").
-include("proto/oneprovider/dbsync_messages2.hrl").
-include_lib("ctool/include/logging.hrl").

%% worker_plugin_behaviour callbacks
-export([init/1, handle/1, cleanup/0]).

%% API
-export([supervisor_flags/0]).

-define(DBSYNC_WORKER_SUP, dbsync_worker_sup).
-define(STREAMS_HEALTHCHECK_INTERVAL, application:get_env(?APP_NAME,
    dbsync_streams_healthcheck_interval, timer:seconds(5))).
-define(DBSYNC_STATE_REPORT_INTERVAL, timer:seconds(application:get_env(?APP_NAME,
    dbsync_state_report_interval_sec, 15))).

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
    couchbase_changes:enable([dbsync_utils:get_bucket()]),
    start_streams(),
    erlang:send_after(?STREAMS_HEALTHCHECK_INTERVAL, self(), {sync_timer, streams_healthcheck}),
    erlang:send_after(?DBSYNC_STATE_REPORT_INTERVAL, self(), {sync_timer, dbsync_state_report}),
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
        Type:Reason ->
            ?error_stacktrace("Failed to start streams due to ~p:~p", [Type, Reason])
    end,
    erlang:send_after(?STREAMS_HEALTHCHECK_INTERVAL, self(), {sync_timer, streams_healthcheck}),
    ok;
handle(dbsync_state_report) ->
    try
        report_dbsync_state()
    catch
        Type:Reason ->
            ?error_stacktrace("Failed to report DBSync state to Onezone due to ~p:~p", [Type, Reason])
    end,
    erlang:send_after(?DBSYNC_STATE_REPORT_INTERVAL, self(), {sync_timer, dbsync_state_report}),
    ok;
handle({dbsync_message, _SessId, Msg = #tree_broadcast2{}}) ->
    handle_tree_broadcast(Msg);
handle({dbsync_message, SessId, Msg = #changes_request2{}}) ->
    handle_changes_request(dbsync_utils:get_provider(SessId), Msg);
handle({dbsync_message, SessId, Msg = #changes_batch{}}) ->
    handle_changes_batch(dbsync_utils:get_provider(SessId), undefined, Msg);
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
            case {Pid, is_this_responsible_node(SpaceId), Module} of
                {undefined, true, dbsync_in_stream} ->
                    start_in_stream(SpaceId);
                {undefined, true, dbsync_out_stream} ->
                    start_out_stream(SpaceId);
                _ ->
                    ok
            end
        end, dbsync_utils:get_spaces())
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
        (#document{mutators = [Mutator | _]}) ->
            Mutator =:= oneprovider:get_id();
        (#document{}) ->
            false
    end,
    Handler = fun
        (Since, Until, Timestamp, Docs) when Since =:= Until ->
            dbsync_communicator:broadcast_changes(SpaceId, Since, Until, Timestamp, Docs);
        (Since, Until, Timestamp, Docs) ->
            ProviderId = oneprovider:get_id(),
            dbsync_communicator:broadcast_changes(SpaceId, Since, Until, Timestamp, Docs),
            dbsync_state:set_seq_and_timestamp(SpaceId, ProviderId, Until, Timestamp)
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
    timestamp = Timestamp,
    compressed_docs = CompressedDocs
}) ->
    Name = {dbsync_in_stream, SpaceId},
    Docs = dbsync_utils:uncompress(CompressedDocs),
    gen_server:cast(
        {global, Name}, {changes_batch, MsgId, ProviderId, Since, Until, Timestamp, Docs}
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
        (BatchSince, end_of_stream, Timestamp, Docs) ->
            dbsync_communicator:send_changes(
                ProviderId, SpaceId, BatchSince, Until, Timestamp, Docs
            );
        (BatchSince, BatchUntil, Timestamp, Docs) ->
            dbsync_communicator:send_changes(
                ProviderId, SpaceId, BatchSince, BatchUntil, Timestamp, Docs
            )
    end,
    Name = base64:encode(term_to_binary({SpaceId, ProviderId})),
    case global:whereis_name({dbsync_out_stream, Name}) of
        undefined ->
            Spec = dbsync_out_stream_spec(Name, SpaceId, [
                {since, Since},
                {until, Until},
                {except_mutator, ProviderId},
                {handler, Handler},
                {handling_interval, application:get_env(
                    ?APP_NAME, dbsync_changes_resend_interval, timer:seconds(1)
                )}
            ]),
            Node = datastore_key:responsible_node(SpaceId),
            rpc:call(Node, supervisor, start_child, [?DBSYNC_WORKER_SUP, Spec]),
            ok;
        _ ->
            ok
    end.

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

%% @private
-spec report_dbsync_state() -> ok.
report_dbsync_state() ->
    Spaces = dbsync_utils:get_spaces(),
    lists:foreach(fun(SpaceId) ->
        is_this_responsible_node(SpaceId) andalso report_dbsync_state(SpaceId)
    end, Spaces).

%% @private
-spec report_dbsync_state(od_space:id()) -> ok.
report_dbsync_state(SpaceId) ->
    {ok, Providers} = space_logic:get_provider_ids(SpaceId),
    SeqPerProvider = lists:foldl(fun(ProviderId, Acc) ->
        Acc#{ProviderId => dbsync_state:get_seq(SpaceId, ProviderId)}
    end, #{}, Providers),
    space_logic:report_dbsync_state(SpaceId, SeqPerProvider).

%% @private
-spec is_this_responsible_node(od_space:id()) -> boolean().
is_this_responsible_node(SpaceId) ->
    node() == datastore_key:responsible_node(SpaceId).