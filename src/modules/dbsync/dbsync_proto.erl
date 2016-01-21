%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc @todo: Write me!
%%% @end
%%%-------------------------------------------------------------------
-module(dbsync_proto).
-author("Rafal Slota").

-include("global_definitions.hrl").
-include("proto/oneprovider/dbsync_messages.hrl").
-include("proto/oneclient/common_messages.hrl").
%%-include_lib("cluster_worker/include/modules/datastore/datastore_engine.hrl").
-include_lib("ctool/include/logging.hrl").

-record(change, {
    seq,
    doc,
    model
}).

-record(seq_range, {
    since,
    until
}).

-record(batch, {
    changes = #{},
    since,
    until
}).


-record(queue, {
    key,
    current_batch,
    last_send,
    removed = false
}).

-record(space_info, {
    space_id,
    providers
}).


-export([send_batch/2, changes_request/3, status_report/3]).

-export([send_tree_broadcast/4, send_tree_broadcast/5, send_direct_message/3]).
-export([handle/2]).
-export([reemit/1]).

%%%==================================================================
%%% API
%%%===================================================================


send_batch(global, #batch{changes = Changes, since = Since, until = Until} = Batch) ->
    ?debug("[ DBSync ] Sending batch to all providers: ~p", [Batch]),
    lists:foreach(
        fun({SpaceId, ChangeList}) ->
            ?info("Processing space ~p", [SpaceId]),
            ToSend = #batch_update{ since_seq = dbsync_utils:encode_term(Since), until_seq = dbsync_utils:encode_term(Until),
                changes_encoded = dbsync_utils:encode_term(maps:from_list([{SpaceId, ChangeList}]))},
            Providers = dbsync_utils:get_providers_for_space(SpaceId),
            send_tree_broadcast(SpaceId, Providers, ToSend, 3)
        end, maps:to_list(Changes)),
    ok;
send_batch({provider, ProviderId, _}, #batch{changes = Changes, since = Since, until = Until} = Batch) ->
    ?info("[ DBSync ] Sending batch to provider ~p: ~p", [ProviderId, Batch]),
    SpaceIds = dbsync_utils:get_spaces_for_provider(ProviderId),
    NewChanges = lists:foldl(
        fun(SpaceId, CMap) ->
            maps:remove(SpaceId, CMap)
        end, Changes, SpaceIds),

    send_direct_message(ProviderId, #batch_update{since_seq = dbsync_utils:encode_term(Since), until_seq = dbsync_utils:encode_term(Until),
        changes_encoded = dbsync_utils:encode_term(NewChanges)}, 3).


changes_request(ProviderId, Since, Until) ->
    ?info("Requesting direct changes ~p ~p ~p", [ProviderId, Since, Until]),
    send_direct_message(ProviderId, #changes_request{since_seq = dbsync_utils:encode_term(Since), until_seq = dbsync_utils:encode_term(Until)}, 3).



status_report(SpaceId, Providers, CurrentSeq) ->
    AllProviders = Providers,
    ?info("Sending status ~p ~p ~p", [SpaceId, Providers, CurrentSeq]),
    send_tree_broadcast(SpaceId, AllProviders, #status_report{seq = dbsync_utils:encode_term(CurrentSeq)}, 3).



%%--------------------------------------------------------------------
%% @doc Sends direct message to given provider and block until response arrives.
%% @end
%%--------------------------------------------------------------------
-spec send_direct_message(ProviderId :: binary(), Request :: term(), Attempts :: non_neg_integer()) ->
    Reposne :: term() | {error, Reason :: any()}.
send_direct_message(ProviderId, Request, Attempts) when Attempts > 0 ->
    PushTo = ProviderId,
    case communicate(PushTo, Request) of
        ok -> ok;
        {error, Reason} ->
            ?error("Unable to send direct message to ~p due to: ~p", [ProviderId, Reason]),
            send_direct_message(ProviderId, Request, Attempts - 1)
    end;
send_direct_message(_ProviderId, _Request, 0) ->
    {error, unable_to_connect}.


%%--------------------------------------------------------------------
%% @doc Sends broadcast message to all providers from SyncWith list.
%%      SyncWith has to be sorted.
%% @end
%%--------------------------------------------------------------------
-spec send_tree_broadcast(SpaceId :: binary(), SyncWith :: [ProviderId :: binary()], Request :: term(), Attempts :: non_neg_integer()) ->
    ok | no_return().
send_tree_broadcast(SpaceId, SyncWith, Request, Attempts) ->
    BaseRequest = #tree_broadcast{space_id = SpaceId, request_id = dbsync_utils:gen_request_id(), message_body = Request, excluded_providers = [], l_edge = <<"">>, r_edge = <<"">>, depth = 0},
    send_tree_broadcast(SpaceId, SyncWith, Request, BaseRequest, Attempts).
send_tree_broadcast(SpaceId, SyncWith, Request, BaseRequest, Attempts) ->
    SyncWith1 = SyncWith -- [oneprovider:get_provider_id()],
    case SyncWith1 of
        [] -> ok;
        _  ->
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
    Reponse :: term() | {error, Reson :: any()}.
do_emit_tree_broadcast([], _Request, _NewBaseRequest, _Attempts) ->
    ok;
do_emit_tree_broadcast(SyncWith, Request, #tree_broadcast{depth = Depth} = BaseRequest, Attempts) when Attempts > 0 ->
    PushTo = lists:nth(crypto:rand_uniform(1, 1 + length(SyncWith)), SyncWith),
    [LEdge | _] = SyncWith,
    REdge = lists:last(SyncWith),
    SyncRequest = BaseRequest#tree_broadcast{l_edge = LEdge, r_edge = REdge, depth = Depth + 1},

    case communicate(PushTo, SyncRequest) of
        ok -> ok;
        {error, Reason} ->
            ?error("Unable to send tree message to ~p due to: ~p", [PushTo, Reason]),
            do_emit_tree_broadcast(SyncWith, Request, #tree_broadcast{} = BaseRequest, Attempts)
    end;
%%    SyncRequestData = dbsync_pb:encode_tree_broadcast(SyncRequest),
%%    MsgId = provider_proxy_con:get_msg_id(),
%%    {AnswerDecoderName, AnswerType} = {rtcore, atom},
%%
%%
%%    RTRequest = #rtrequest{answer_decoder_name = a2l(AnswerDecoderName), answer_type = a2l(AnswerType), input = SyncRequestData, message_decoder_name = a2l(dbsync),
%%        message_id = MsgId, message_type = a2l(utils:record_type(SyncRequest)), module_name = a2l(dbsync), protocol_version = 1, synch = true},
%%    RTRequestData = iolist_to_binary(rtcore_pb:encode_rtrequest(RTRequest)),
%%
%%    URL = dbsync_utils:get_provider_url(PushTo),
%%    Timeout = 1000,
%%    provider_proxy_con:send({URL, <<"oneprovider">>}, MsgId, RTRequestData),
%%    receive
%%        {response, MsgId, AnswerStatus, WorkerAnswer} ->
%%            provider_proxy_con:report_ack({URL, <<"oneprovider">>}),
%%            ?debug("Answer for inter-provider pull request: ~p ~p", [AnswerStatus, WorkerAnswer]),
%%            case AnswerStatus of
%%                ?VOK ->
%%                    #atom{value = RValue} = erlang:apply(pb_module(AnswerDecoderName), decoder_method(AnswerType), [WorkerAnswer]),
%%                    case RValue of
%%                        ?VOK -> ok;
%%                        _ -> throw(RValue)
%%                    end;
%%                InvalidStatus ->
%%                    ?error("Cannot send message ~p due to invalid answer status: ~p", [get_message_type(SyncRequest), InvalidStatus]),
%%                    do_emit_tree_broadcast(SyncWith, Request, BaseRequest, Attempts - 1)
%%            end
%%    after Timeout ->
%%        provider_proxy_con:report_timeout({URL, <<"oneprovider">>}),
%%        do_emit_tree_broadcast(SyncWith, Request, BaseRequest, Attempts - 1)
%%    end;

do_emit_tree_broadcast(_SyncWith, _Request, _BaseRequest, 0) ->
    {error, unable_to_connect}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% Reemitting tree broadcast messages
reemit(#tree_broadcast{l_edge = LEdge, r_edge = REdge, space_id = SpaceId, message_body = Request} = BaseRequest) ->
    Providers = dbsync_utils:get_providers_for_space(SpaceId),
    Providers1 = lists:usort(Providers),
    Providers2 = lists:dropwhile(fun(Elem) -> Elem =/= LEdge end, Providers1),
    Providers3 = lists:takewhile(fun(Elem) -> Elem =/= REdge end, Providers2),
    Providers4 = lists:usort([REdge | Providers3]),
    ok = send_tree_broadcast(SpaceId, Providers4, Request, BaseRequest, 3).


handle(SessId, #dbsync_request{message_body = MessageBody}) ->
    ?info("Request from  ~p", [SessId]),
    {ok, #document{value = #session{identity = #identity{provider_id = ProviderId}}}} = session:get(SessId),
    try handle(ProviderId, MessageBody) of
        ok ->
            #dbsync_response{status = #status{code = ?OK}};
        {error, Reason} ->
            ?error("DBSync error ~p", [Reason]),
            #dbsync_response{status = #status{code = ?EAGAIN}}
    catch
        _:Reason0 ->
            ?error_stacktrace("DBSync error ~p", [Reason0]),
            #dbsync_response{status = #status{code = ?EAGAIN}}
    end;
%% Handle tree_broadcast{} message
handle(From, #tree_broadcast{message_body = Request, request_id = ReqId} = BaseRequest) ->
    Ignore =
        case worker_host:state_get(dbsync_worker, {request, ReqId}) of
            undefined ->
                worker_host:state_put(dbsync_worker, {request, ReqId}, utils:mtime()),
                false;
            _MTime ->
                true
        end,

    case Ignore of
        false ->
            case handle_broadcast(From, Request, BaseRequest) of
                ok -> ok;
                reemit ->
                    case worker_proxy:cast(dbsync_worker, {reemit, BaseRequest}) of
                        ok -> ok;
                        {error, Reason} ->
                            ?debug("Cannot reemit tree broadcast due to: ~p", [Reason]),
                            {error, Reason}
                    end;
                {error, Reason} ->
                    ?debug("Error while handling tree broadcast: ~p", [Reason]),
                    {error, Reason}
            end;
        true -> ok
    end;

handle(From, #changes_request{since_seq = Since, until_seq = Until} = _BaseRequest) ->
    ok = dbsync_worker:init_stream(Since, Until, {provider, From, dbsync_utils:gen_request_id()}).



%%--------------------------------------------------------------------
%% @doc General handler for tree_broadcast{} inner-messages. Shall return whether
%%      broadcast should be canceled (ok | {error, _}) or reemitted (reemit).
%% @end
%%--------------------------------------------------------------------
-spec handle_broadcast(From :: binary(), Request :: term(), BaseRequest :: term()) ->
    ok | reemit | {error, Reason :: any()}.
handle_broadcast(From, #batch_update{since_seq = Since, until_seq = Until, changes_encoded = ChangesBin} = Request, BaseRequest) ->
    ProviderId = From,

    Batch = #batch{since = dbsync_utils:decode_term(Since), until = dbsync_utils:decode_term(Until), changes = dbsync_utils:decode_term(ChangesBin)},
    dbsync_worker:apply_batch_changes(ProviderId, Batch),
    reemit;

handle_broadcast(From, #status_report{seq = SeqBin} = _Request, _BaseRequest) ->
    ?info("Got dbsync report from provider ~p: ~p", [From, SeqBin]),
    dbsync_worker:on_status_received(From, dbsync_utils:decode_term(SeqBin)),
    reemit;


handle_broadcast(From, #status_request{} = _Request, _BaseRequest) ->
    worker_proxy:cast(dbsync_worker, requested_bcast_status),
    reemit.




%%--------------------------------------------------------------------
%% Misc
%%--------------------------------------------------------------------


communicate(ProviderId, Message) ->
    SessId = session_manager:get_provider_session_id(outgoing, ProviderId),
    provider_communicator:communicate(#dbsync_request{message_body = Message}, SessId).