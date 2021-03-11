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
%%%
%%% The main flow of changes is as follows. After document is saved to couchbase,
%%% it is queried by couchbase_changes_worker started by dbsync_out_stream and then
%%% broadcast to other providers by dbsync_out_stream. When document broadcast by
%%% other provider appears, it is handled by dbsync_in_stream. Sequences from incoming
%%% documents are marked as pending by dbsync_in_stream that means that these documents
%%% has been saved with sequence number of remote provider inside document and do not
%%% have sequence number assigned by local couchbase_driver yet. Afterwards, the sequences
%%% assigned by local couchbase_driver are analysed by dbsync_out_stream to describe correlation
%%% between local and remote sequences. Remote sequence that appears in dbsync_out_stream
%%% is removed from pending sequences list. For more information about sequences
%%% correlations see dbsync_seqs_correlation.erl.
%%% @end
%%%-------------------------------------------------------------------
-module(dbsync_worker).
-author("Krzysztof Trzepla").

-behaviour(worker_plugin_behaviour).

-include("global_definitions.hrl").
-include("modules/dbsync/dbsync.hrl").
-include("proto/oneprovider/dbsync_messages2.hrl").
-include_lib("ctool/include/logging.hrl").

%% worker_plugin_behaviour callbacks
-export([init/1, handle/1, cleanup/0]).

%% API
-export([supervisor_flags/0, start_streams/0, start_streams/1, get_main_out_stream_opts/1]).

-type batch_docs() :: [datastore:doc()].
% Type describing mutators whose changes should be included in stream
-type mutators_to_include() :: single_provider | all_providers.
% Extension of changes batch used to handle custom changes requests.
% TODO VFS-7247: The field usage will be extend in ticket VFS-7247. Define type more in more
%   detail and prepare functions processing this type (currently this extension is empty or
%   contains dbsync_processed_seqs_history:encoded_correlations()).
-type custom_request_extension() :: binary().
% Record used to represent changes batch internally in dbsync modules.
-type internal_changes_batch() ::#internal_changes_batch{}.
-export_type([batch_docs/0, mutators_to_include/0, custom_request_extension/0, internal_changes_batch/0]).

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
handle({dbsync_message, _SessId, Msg = #tree_broadcast2{}}) ->
    handle_tree_broadcast(Msg);
handle({dbsync_message, SessId, Msg = #changes_request2{}}) ->
    handle_changes_request(dbsync_utils:get_provider(SessId), Msg);
handle({dbsync_message, SessId, Msg = #custom_changes_request{}}) ->
    handle_custom_changes_request(dbsync_utils:get_provider(SessId), Msg);
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
    dbsync_worker_sup:supervisor_flags().

-spec start_streams() -> ok.
start_streams() ->
    start_streams(dbsync_utils:get_spaces()).

-spec start_streams([od_space:id()]) -> ok.
start_streams(Spaces) ->
    case whereis(?MODULE) of
        undefined ->
            ?warning("Ignoring request to start streams for spaces: ~p - ~p is not running", [
                Spaces, ?MODULE
            ]);
        _ ->
            lists:foreach(fun(SpaceId) ->
                ok = internal_services_manager:start_service(dbsync_worker_sup, <<"dbsync_in_stream", SpaceId/binary>>,
                    start_in_stream, stop_in_stream, [SpaceId], SpaceId),
                ok = internal_services_manager:start_service(dbsync_worker_sup, <<"dbsync_out_stream", SpaceId/binary>>,
                    start_out_stream, stop_out_stream, [SpaceId], SpaceId)
            end, Spaces)
    end.

-spec get_main_out_stream_opts(od_space:id()) -> [dbsync_out_stream:option()].
get_main_out_stream_opts(SpaceId) ->
    Handler = fun
        (Since, Until, Timestamp, Docs) when Since =:= Until ->
            dbsync_communicator:broadcast_changes(SpaceId, Since, Until, Timestamp, Docs);
        (Since, Until, Timestamp, Docs) ->
            ProviderId = oneprovider:get_id(),
            dbsync_communicator:broadcast_changes(SpaceId, Since, Until, Timestamp, Docs),
            dbsync_state:set_sync_progress(SpaceId, ProviderId, Until, Timestamp)
    end,
    [
        {main_stream, true},
        {local_changes_only, true},
        {batch_handler, Handler}
    ].

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Forwards changes batch to an associated incoming stream.
%% @end
%%--------------------------------------------------------------------
-spec handle_changes_batch(od_provider:id(), undefined |
dbsync_communicator:msg_id(), dbsync_communicator:changes_batch()) -> ok.
handle_changes_batch(Distributor, MsgId, #changes_batch{
    space_id = SpaceId,
    since = Since,
    until = Until,
    timestamp = Timestamp,
    compressed_docs = CompressedDocs,
    mutator_id = MutatorId,
    custom_request_extension = CustomRequestExtension
}) ->
    Name = ?IN_STREAM_ID(SpaceId),
    Docs = dbsync_utils:uncompress(CompressedDocs),
    InternalBatch = #internal_changes_batch{
        since = Since,
        until = Until,
        timestamp = Timestamp,
        docs = Docs,
        distributor_id = Distributor,
        custom_request_extension = CustomRequestExtension
    },
    gen_server:cast(
        {global, Name}, {changes_batch, MsgId, MutatorId, InternalBatch}
    ).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Starts outgoing DBSync recovery stream.
%% @end
%%--------------------------------------------------------------------
-spec handle_changes_request(od_provider:id(), dbsync_communicator:changes_request()) -> ok.
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

    Opts = [
        {since, Since},
        {until, Until},
        {local_changes_only, true},
        {batch_handler, Handler},
        {handling_interval, op_worker:get_env(dbsync_changes_resend_interval, timer:seconds(1))}
    ],

    dbsync_worker_sup:start_on_demand_stream(SpaceId, ProviderId, Opts).

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
%% @doc
-spec handle_custom_changes_request(od_provider:id(), dbsync_communicator:custom_changes_request()) -> ok.
handle_custom_changes_request(CallerProviderId, Request = #custom_changes_request{
    space_id = SpaceId,
    mutator_id = MutatorId,
    since = RequestedSince,
    until = RequestedUntil,
    include_mutators = IncludeMutators
}) ->
    SingleProviderChangesRequested = case IncludeMutators of
        single_provider -> true;
        all_providers -> false
    end,
    LocalSince = dbsync_seqs_correlations_history:map_remote_seq_to_local_start_seq(
        SpaceId, MutatorId, RequestedSince, SingleProviderChangesRequested),
    % Information about remote sequences for last batch has to be prepared getting local until sequence
    % see `dbsync_seqs_correlation_history:map_remote_seq_to_local_stop_params` function doc for more information
    {LocalUntil, FinalEncodedCorrelations} =
        dbsync_seqs_correlations_history:map_remote_seq_to_local_stop_params(SpaceId, MutatorId, RequestedUntil),
    case LocalSince < LocalUntil of
        true ->
            send_custom_request_stream_initial_message(CallerProviderId, LocalSince, Request),
            % TODO VFS-7204 - filter documents not mutated by MutatorId (with remote seq outside range)
            Handler = fun
                (BatchSince, end_of_stream, Timestamp, Docs) ->
                    dbsync_communicator:send_changes_and_correlations(CallerProviderId, MutatorId,
                        SpaceId, BatchSince, LocalUntil, Timestamp, Docs, FinalEncodedCorrelations);
                (BatchSince, BatchUntil, Timestamp, Docs) ->
                    EncodedCorrelations = dbsync_processed_seqs_history:get(SpaceId, BatchUntil),
                    dbsync_communicator:send_changes_and_correlations(CallerProviderId, MutatorId,
                        SpaceId, BatchSince, BatchUntil, Timestamp, Docs, EncodedCorrelations)
            end,

            Opts = [
                {since, LocalSince},
                {until, LocalUntil},
                {except_mutator, CallerProviderId},
                {batch_handler, Handler},
                {handling_interval, op_worker:get_env(
                    dbsync_custom_request_changes_handling_interval, timer:seconds(1))}
            ],

            dbsync_worker_sup:start_on_demand_stream(SpaceId, CallerProviderId, Opts);
        false ->
            % TODO VFS-7247 - maybe send empty batch
            ok
    end.

-spec send_custom_request_stream_initial_message(od_provider:id(), couchbase_changes:since(),
    dbsync_communicator:custom_changes_request()) -> ok.
send_custom_request_stream_initial_message(CallerProviderId, LocalSince, #custom_changes_request{
    space_id = SpaceId,
    mutator_id = MutatorId,
    since = RequestedSince
}) when is_integer(RequestedSince) ->
    dbsync_communicator:send_changes_and_correlations(CallerProviderId, MutatorId, SpaceId, LocalSince, LocalSince,
        undefined, [], ?CUSTOM_CHANGES_STREAM_INIT(integer_to_binary(RequestedSince)));
send_custom_request_stream_initial_message(_, _, _) ->
    % TODO VFS-7262 - handle requesting with sequences map
    ok.