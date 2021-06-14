%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements remote_driver behaviour and enables fetching
%%% documents stored on remote providers.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_remote_driver).
-author("Krzysztof Trzepla").
-behavior(remote_driver).

-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("proto/oneprovider/remote_driver_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([handle/1]).

%% remote_driver callbacks
-export([get_async/2, wait/1]).

-type ctx() :: #{model := datastore_model:model(),
                 routing_key := key(),
                 source_ids := [oneprovider:id()]}.
-type key() :: datastore:key().
-type doc() :: datastore:doc().
-type communicator_ans() :: {ok, clproto_message_id:id()} | {error, term()}.
-type future() :: {communicator_ans(), session:id()}.


%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Handles remote driver requests.
%% @end
%%--------------------------------------------------------------------
-spec handle(#get_remote_document{}) -> #remote_document{}.
handle(#get_remote_document{
    model = Model, key = Key, routing_key = RoutingKey
}) ->
    Ctx = datastore_model_default:get_ctx(Model),
    Ctx2 = datastore_multiplier:extend_name(RoutingKey, Ctx),
    Ctx3 = Ctx2#{include_deleted => true, remote_driver => undefined, routing_key => RoutingKey},
    case datastore_router:route(get, [Ctx3, Key]) of
        {ok, Doc} ->
            Data = zlib:compress(jiffy:encode(datastore_json:encode(Doc))),
            #remote_document{
                status = #status{code = ?OK},
                compressed_data = Data
            };
        {error, not_found} ->
            #remote_document{
                status = #status{code = ?ENOENT}
            };
        {error, Reason} ->
            #remote_document{
                status = #status{
                    code = ?EAGAIN,
                    description = erlang:term_to_binary(Reason)
                }
            }
    end.

%%%===================================================================
%%% remote_driver callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Implementation of {@link remote_driver:get_async/2}.
%% @end
%%--------------------------------------------------------------------
-spec get_async(ctx(), key()) -> future().
get_async(#{
    model := Model,
    routing_key := RoutingKey,
    source_ids := [ProviderId | _]
}, Key) ->
    try
        case oneprovider:get_id() of
            ProviderId ->
                {error, not_found};
            _ ->
                SessId = session_utils:get_provider_session_id(outgoing, ProviderId),
                Msg = #get_remote_document{
                    model = Model,
                    key = Key,
                    routing_key = RoutingKey
                },
                SendAns = try
                    communicator:send_to_provider(SessId, Msg, self(), 1, throw)
                catch
                    _:Reason ->
                        {error, Reason}
                end,
                {SendAns, SessId}
        end
    catch
        _:Reason2 ->
            ?error_stacktrace("Datastore remote get failed due to: ~p", [Reason2]),
            {error, Reason2}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Implementation of {@link remote_driver:wait/1}.
%% @end
%%--------------------------------------------------------------------
-spec wait(future()) -> {ok, doc()} | {error, term()}.
wait({{ok, MsgId}, _} = Future) ->
    Timeout = op_worker:get_env(datastore_remote_driver_timeout, timer:minutes(1)),
    receive
        #server_message{
            message_id = MsgId,
            message_body = #remote_document{
                status = #status{code = ?OK},
                compressed_data = Data
            }
        } -> {ok, datastore_json:decode(jiffy:decode(zlib:uncompress(Data)))};
        #server_message{
            message_id = MsgId,
            message_body = #remote_document{
                status = #status{code = ?ENOENT}
            }
        } -> {error, not_found};
        #server_message{
            message_id = MsgId,
            message_body = #remote_document{
                status = #status{
                    code = ?EAGAIN,
                    description = Description
                }
            }
        } -> {error, binary_to_term(Description)};
        #server_message{
            message_id = MsgId,
            message_body = #processing_status{code = 'IN_PROGRESS'}
        } -> wait(Future)
    after
        % TODO VFS-4025 - multiprovider communication
        Timeout -> {error, timeout}
    end;
wait({{error, {badmatch, {error, internal_call}}}, SessId}) ->
    ?debug("Remote driver internal call"),
    spawn(fun() ->
        session_connections:ensure_connected(SessId)
    end),
    {error, interrupted_call};
wait({{error, Reason}, _}) ->
    ?debug("Remote driver error ~p", [Reason]),
    {error, interrupted_call}.