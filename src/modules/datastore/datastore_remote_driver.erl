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
                 provider_id := oneprovider:id()}.
-type key() :: datastore:key().
-type doc() :: datastore:doc().
-type future() :: {ok, message_id:id()} | {error, term()}.


%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Handles remote driver requests.
%% @end
%%--------------------------------------------------------------------
-spec handle(#remote_driver_request{}) -> #remote_driver_response{}.
handle(#remote_driver_request{request = #get_remote_document{
    model = Model, key = Key, routing_key = RoutingKey
} = R}) ->
    ?alert("RECEIVED REQUEST: ~p", [R]),
    Ctx = datastore_model_default:get_ctx(Model),
    case datastore_router:route(Ctx, RoutingKey, get, [Ctx, Key]) of
        {ok, Doc} ->
            #remote_driver_response{
                status = #status{code = ?OK},
                response = #remote_document{
                    compressed_data = zlib:compress(datastore_json:encode(Doc))
                }
            };
        {error, not_found} ->
            #remote_driver_response{
                status = #status{code = ?ENOENT}
            };
        {error, Reason} ->
            #remote_driver_response{
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
    provider_id := ProviderId
} = A, Key) ->
    try
        case oneprovider:get_provider_id() of
            ProviderId ->
                {error, not_found};
            _ ->
                ?info("SENDING REQUEST: ~p, ~p", [A, Key]),
                SessId = session_manager:get_provider_session_id(outgoing, ProviderId),
                provider_communicator:communicate_async(#remote_driver_request{
                    request = #get_remote_document{
                        model = Model,
                        key = Key,
                        routing_key = RoutingKey
                    }
                }, SessId)
        end
    catch
        _:Reason ->
            ?info_stacktrace("SENDING REQUEST ERROR: ~p", [Reason]),
            {error, not_found}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Implementation of {@link remote_driver:wait/1}.
%% @end
%%--------------------------------------------------------------------
-spec wait(future()) -> {ok, doc()} | {error, term()}.
wait({ok, MsgId}) ->
    Timeout = application:get_env(op_worker, datastore_remote_driver_timeout,
        timer:minutes(1)),
    R = receive
        #server_message{
            message_id = MsgId,
            message_body = #remote_driver_response{
                status = #status{code = ?OK},
                response = #remote_document{compressed_data = Data}
            }
        } -> {ok, datastore_json:decode(zlib:uncompress(Data))};
        #server_message{
            message_id = MsgId,
            message_body = #remote_driver_response{
                status = #status{code = ?ENOENT}
            }
        } -> {error, not_found};
        #server_message{
            message_id = MsgId,
            message_body = #remote_driver_response{
                status = #status{
                    code = ?EAGAIN,
                    description = Description
                }
            }
        } -> {error, binary_to_term(Description)}
    after
        Timeout -> {error, timeout}
    end,
    ?info("RECEIVED RESPONSE: ~p", [R]),
    R;
wait({error, Reason}) ->
    {error, Reason}.