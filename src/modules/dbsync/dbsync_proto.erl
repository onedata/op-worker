%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Implementation of TreeBroadcast protocol for DBSync.
%%% @end
%%%-------------------------------------------------------------------
-module(dbsync_proto).
-author("Rafal Slota").

-include("global_definitions.hrl").
-include("modules/dbsync/common.hrl").
-include("proto/oneprovider/dbsync_messages.hrl").
-include("proto/oneclient/common_messages.hrl").
-include_lib("ctool/include/logging.hrl").


-export([send_batch/3, changes_request/3, status_report/3]).

-export([send_tree_broadcast/4, send_tree_broadcast/5, send_direct_message/3]).
-export([handle/2, handle_impl/2]).
-export([reemit/1]).

%%%==================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Sends given batch from the queue to providers. Batch may be skipped if empty.
%% @end
%%--------------------------------------------------------------------
-spec send_batch(dbsync_worker:queue(), SpaceId :: binary(), dbsync_worker:batch()) ->
    skip | ok | no_return().
send_batch(_, _, #batch{since = X, until = X}) ->
    skip;
send_batch(global, SpaceId, #batch{changes = Changes, since = Since, until = Until} = Batch) ->
    ?debug("[ DBSync ] Sending batch from space ~p to all providers: ~p", [SpaceId, Batch]),
    ToSend = #batch_update{space_id = SpaceId, since_seq = dbsync_utils:encode_term(Since), until_seq = dbsync_utils:encode_term(Until),
        changes_encoded = dbsync_utils:encode_term(Changes)},
    Providers = dbsync_utils:get_providers_for_space(SpaceId),
    send_tree_broadcast(SpaceId, Providers, ToSend, 3),
    ok;
send_batch({provider, ProviderId, _}, SpaceId, #batch{changes = Changes, since = Since, until = Until} = Batch) ->
    ?debug("[ DBSync ] Sending batch to provider ~p: ~p", [ProviderId, Batch]),
    %% @todo: filter spaces for given provider
    send_direct_message(ProviderId, #batch_update{space_id = SpaceId, since_seq = dbsync_utils:encode_term(Since), until_seq = dbsync_utils:encode_term(Until),
        changes_encoded = dbsync_utils:encode_term(Changes)}, 3).


%%--------------------------------------------------------------------
%% @doc
%% Sends request for missing changes to given provider.
%% @end
%%--------------------------------------------------------------------
-spec changes_request(oneprovider:id(), Since :: non_neg_integer(), Until :: non_neg_integer()) ->
    ok | {error, Reason :: term()}.
changes_request(ProviderId, Since, Until) ->
%%    ?info("Requesting direct changes ~p ~p ~p", [ProviderId, Since, Until]),
    send_direct_message(ProviderId, #changes_request{since_seq = dbsync_utils:encode_term(Since), until_seq = dbsync_utils:encode_term(Until)}, 3).


%%--------------------------------------------------------------------
%% @doc
%% Sends status report to given providers.
%% @end
%%--------------------------------------------------------------------
-spec status_report(SpaceId :: binary(), Providers :: [oneprovider:id()], CurrentSeq :: non_neg_integer()) ->
    ok | {error, Reason :: term()}.
status_report(SpaceId, Providers, CurrentSeq) ->
    AllProviders = Providers,
    ?debug("Sending status ~p ~p ~p", [SpaceId, Providers, CurrentSeq]),
    send_tree_broadcast(SpaceId, AllProviders, #status_report{space_id = SpaceId, seq = dbsync_utils:encode_term(CurrentSeq)}, 3).


%%--------------------------------------------------------------------
%% @doc Sends direct message to given provider and block until response arrives.
%% @end
%%--------------------------------------------------------------------
-spec send_direct_message(ProviderId :: oneprovider:id(), Request :: term(), Attempts :: non_neg_integer()) ->
    ok | {error, Reason :: any()}.
send_direct_message(ProviderId, Request, Attempts) when Attempts > 0 ->
    PushTo = ProviderId,
    case dbsync_utils:communicate(PushTo, Request) of
        {ok, _} -> ok;
        {error, Reason} ->
            ?error("Unable to send direct message to ~p due to: ~p", [ProviderId, Reason]),
            send_direct_message(ProviderId, Request, Attempts - 1)
    end;
send_direct_message(_ProviderId, _Request, _) ->
    {error, unable_to_connect}.


%%--------------------------------------------------------------------
%% @doc Sends broadcast message to all providers from SyncWith list.
%%      SyncWith has to be sorted.
%% @end
%%--------------------------------------------------------------------
-spec send_tree_broadcast(SpaceId :: binary(), SyncWith :: [ProviderId :: oneprovider:id()], Request :: term(), Attempts :: non_neg_integer()) ->
    ok | no_return().
send_tree_broadcast(SpaceId, SyncWith, Request, Attempts) ->
    BaseRequest = #tree_broadcast{space_id = SpaceId, request_id = dbsync_utils:gen_request_id(), message_body = Request, excluded_providers = [], l_edge = <<"">>, r_edge = <<"">>, depth = 0},
    send_tree_broadcast(SpaceId, SyncWith, Request, BaseRequest, Attempts).
send_tree_broadcast(SpaceId, SyncWith, Request, BaseRequest, Attempts) ->
    SyncWith1 = SyncWith -- [oneprovider:get_provider_id()],
    case SyncWith1 of
        [] -> ok;
        _ ->
            {LSync, RSync} = lists:split(crypto:rand_uniform(0, length(SyncWith1)), SyncWith1),
            ExclProviders = [oneprovider:get_provider_id() | BaseRequest#tree_broadcast.excluded_providers],
            NewBaseRequest = BaseRequest#tree_broadcast{excluded_providers = lists:usort(ExclProviders), space_id = SpaceId},
            do_emit_tree_broadcast(LSync, Request, NewBaseRequest, Attempts),
            do_emit_tree_broadcast(RSync, Request, NewBaseRequest, Attempts)
    end.


%%--------------------------------------------------------------------
%% @doc Internal helper function for tree_broadcast/4. This function broadcasts message blindly
%%      to one of given providers (while passing to him responsibility to reemit the message)
%%      without any additional logic.
%% @end
%%--------------------------------------------------------------------
-spec do_emit_tree_broadcast(SyncWith :: [ProviderId :: binary()], Request :: term(), #tree_broadcast{}, Attempts :: non_neg_integer()) ->
    Response :: term() | {error, Reason :: any()}.
do_emit_tree_broadcast([], _Request, _NewBaseRequest, _Attempts) ->
    ok;
do_emit_tree_broadcast(SyncWith, Request, #tree_broadcast{depth = Depth} = BaseRequest, Attempts) when Attempts > 0 ->
    PushTo = lists:nth(crypto:rand_uniform(1, 1 + length(SyncWith)), SyncWith),
    [LEdge | _] = SyncWith,
    REdge = lists:last(SyncWith),
    SyncRequest = BaseRequest#tree_broadcast{l_edge = LEdge, r_edge = REdge, depth = Depth + 1},

    case dbsync_utils:communicate(PushTo, SyncRequest) of
        {ok, _} -> ok;
        {error, Reason} ->
            ?error("Unable to send tree message to ~p due to: ~p", [PushTo, Reason]),
            do_emit_tree_broadcast(SyncWith, Request, #tree_broadcast{} = BaseRequest, Attempts - 1)
    end;
do_emit_tree_broadcast(_SyncWith, _Request, _BaseRequest, 0) ->
    {error, unable_to_connect}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Sends given TreeBroadcast message to all remaining providers.
%% @end
%%--------------------------------------------------------------------
-spec reemit(#tree_broadcast{}) ->
    ok | no_return().
reemit(#tree_broadcast{l_edge = LEdge, r_edge = REdge, space_id = SpaceId, message_body = Request} = BaseRequest) ->
    AllProviders = dbsync_utils:get_providers_for_space(SpaceId),
    SortedProviders = lists:usort(AllProviders),
    WOLeftEdge = lists:dropwhile(fun(Elem) -> Elem =/= LEdge end, SortedProviders),
    UpToRightEdge = lists:takewhile(fun(Elem) -> Elem =/= REdge end, WOLeftEdge),
    ProvidersToSync = lists:usort([REdge | UpToRightEdge]),
    ok = send_tree_broadcast(SpaceId, ProvidersToSync, Request, BaseRequest, 3).


%%--------------------------------------------------------------------
%% @doc
%% Handles request from non-local DBSync server.
%% @end
%%--------------------------------------------------------------------
-spec handle(SessId :: session:id(), #dbsync_request{}) ->
    #status{}.
handle(SessId, #dbsync_request{message_body = MessageBody}) ->
    ?debug("DBSync request from ~p ~p", [SessId, MessageBody]),
    {ok, #document{value = #session{identity = #identity{provider_id = ProviderId}}}} = session:get(SessId),
    try handle_impl(ProviderId, MessageBody) of
        ok ->
            #status{code = ?OK}
    catch
        _:Reason0 ->
            ?error_stacktrace("DBSync error ~p", [Reason0]),
            #status{code = ?EAGAIN}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Handles unpacked request from non-local DBSync server.
%% @end
%%--------------------------------------------------------------------
-spec handle_impl(From :: oneprovider:id(), #tree_broadcast{} | #changes_request{} | #batch_update{}) ->
    ok | no_return().
handle_impl(From, #tree_broadcast{message_body = Request, request_id = ReqId} = BaseRequest) ->
    Ignore =
        case dbsync_utils:temp_get({request, ReqId}) of
            undefined ->
                dbsync_utils:temp_put({request, ReqId}, erlang:system_time(), timer:minutes(1)),
                false;
            _MTime ->
                true
        end,

    ?debug("DBSync request (ignored: ~p) from ~p ~p", [Ignore, From, BaseRequest]),

    case Ignore of
        false ->
            try handle_broadcast(From, Request, BaseRequest) of
%%                ok -> ok; %% This case should be safely ignored but is not used right now.
                reemit ->
                    case worker_proxy:cast(dbsync_worker, {reemit, BaseRequest}) of
                        ok -> ok;
                        {error, Reason} ->
                            ?error("Cannot reemit tree broadcast due to: ~p", [Reason]),
                            {error, Reason}
                    end
            catch
                _:Reason ->
                    ?error("Error while handling tree broadcast: ~p", [Reason]),
                    {error, Reason}
            end;
        true -> ok
    end;
handle_impl(From, #changes_request{since_seq = Since, until_seq = Until} = _BaseRequest) ->
%%    ?info("Changes request form ~p: Since ~p, Until: ~p", [From, Since, Until]),
    {ok, _} = dbsync_worker:init_stream(dbsync_utils:decode_term(Since), dbsync_utils:decode_term(Until), {provider, From, dbsync_utils:gen_request_id()}),
    ok;
handle_impl(From, #batch_update{space_id = SpaceId, since_seq = Since, until_seq = Until, changes_encoded = ChangesBin}) ->
    ProviderId = From,

    Batch = #batch{since = dbsync_utils:decode_term(Since), until = dbsync_utils:decode_term(Until), changes = dbsync_utils:decode_term(ChangesBin)},
    dbsync_worker:apply_batch_changes(ProviderId, SpaceId, Batch).


%%--------------------------------------------------------------------
%% @doc
%% General handler for tree_broadcast{} inner-messages. Shall return whether
%% broadcast should be canceled (ok | {error, _}) or reemitted (reemit).
%% @end
%%--------------------------------------------------------------------
-spec handle_broadcast(From :: oneprovider:id(), Request :: term(), BaseRequest :: term()) ->
    ok | reemit | {error, Reason :: any()}.
handle_broadcast(From, #batch_update{space_id = SpaceId, since_seq = Since, until_seq = Until, changes_encoded = ChangesBin} = Request, BaseRequest) ->
    ProviderId = From,

    Batch = #batch{since = dbsync_utils:decode_term(Since), until = dbsync_utils:decode_term(Until), changes = dbsync_utils:decode_term(ChangesBin)},
    dbsync_worker:apply_batch_changes(ProviderId, SpaceId, Batch),
    reemit;
handle_broadcast(From, #status_report{space_id = SpaceId, seq = SeqBin} = _Request, _BaseRequest) ->
    dbsync_worker:on_status_received(From, SpaceId, dbsync_utils:decode_term(SeqBin)),
    reemit;
handle_broadcast(_From, #status_request{} = _Request, _BaseRequest) ->
    worker_proxy:cast(dbsync_worker, requested_bcast_status),
    reemit.


%%--------------------------------------------------------------------
%% Misc
%%--------------------------------------------------------------------
