%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: Protocol API for dbsync worker
%% @end
%% ===================================================================
-module(dbsync_protocol).
-author("Rafal Slota").

-include("oneprovider_modules/dao/dao.hrl").
-include("oneprovider_modules/fslogic/fslogic.hrl").
-include("fuse_messages_pb.hrl").
-include("dbsync_pb.hrl").
-include("rtcore_pb.hrl").
-include("communication_protocol_pb.hrl").
-include("registered_names.hrl").
-include("cluster_elements/request_dispatcher/gsi_handler.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([tree_broadcast/4, tree_broadcast/5]).
-export([decoder_method/1, send_direct_message/4]).

send_direct_message(ProviderId, Request, {AnswerDecoderName, AnswerType} = AnswerConf, Attempts) when Attempts > 0 ->
    PushTo = ProviderId,
    ReqEncoder = encoder_method(get_message_type(Request)),
    RequestData = iolist_to_binary(apply(dbsync_pb, ReqEncoder, [Request])),
    MsgId = provider_proxy_con:get_msg_id(),

    RTRequest = #rtrequest{answer_decoder_name = a2l(AnswerDecoderName), answer_type = a2l(AnswerType), input = RequestData, message_decoder_name = a2l(dbsync),
        message_id = MsgId, message_type = a2l(utils:record_type(Request)), module_name = a2l(dbsync), protocol_version = 1, synch = true},
    RTRequestData = iolist_to_binary(rtcore_pb:encode_rtrequest(RTRequest)),

    URL = dbsync_utils:get_provider_url(PushTo),
    Timeout = 1000,
    provider_proxy_con:send({URL, <<"oneprovider">>}, MsgId, RTRequestData),
    receive
        {response, MsgId, AnswerStatus, WorkerAnswer} ->
            provider_proxy_con:report_ack({URL, <<"oneprovider">>}),
            ?debug("Answer for inter-provider pull request: ~p ~p", [AnswerStatus, WorkerAnswer]),
            case AnswerStatus of
                ?VOK ->
                    erlang:apply(pb_module(AnswerDecoderName), decoder_method(AnswerType), [WorkerAnswer]);
                InvalidStatus ->
                    ?error("Cannot reroute message ~p due to invalid answer status: ~p", [get_message_type(Request), InvalidStatus]),
                    send_direct_message(ProviderId, Request, AnswerConf, Attempts - 1)
            end
    after Timeout ->
        provider_proxy_con:report_timeout({URL, <<"oneprovider">>}),
        send_direct_message(ProviderId, Request, AnswerConf, Attempts - 1)
    end;
send_direct_message(_ProviderId, _Request, {_AnswerDecoderName, _AnswerType} = _AnswerConf, 0) ->
    {error, unable_to_connect}.


tree_broadcast(SpaceInfo, SyncWith, Request, Attempts) ->
    ReqEncoder = encoder_method(get_message_type(Request)),
    BaseRequest = #treebroadcast{request_id = dbsync_utils:gen_request_id(), input = <<"">>, message_type = a2l(ReqEncoder), excluded_providers = [], ledge = <<"">>, redge = <<"">>, depth = 0, space_id = <<"">>},
    tree_broadcast(SpaceInfo, SyncWith, Request, BaseRequest, Attempts).
tree_broadcast(SpaceInfo, SyncWith, Request, BaseRequest, Attempts) ->
    SyncWith1 = SyncWith -- [cluster_manager_lib:get_provider_id()],
    case SyncWith1 of
        [] -> ok;
        _  ->
            {LSync, RSync} = lists:split(crypto:rand_uniform(0, length(SyncWith1)), SyncWith1),
            ExclProviders = [cluster_manager_lib:get_provider_id() | BaseRequest#treebroadcast.excluded_providers],
            NewBaseRequest = BaseRequest#treebroadcast{excluded_providers = lists:usort(ExclProviders)},
            do_emit_tree_broadcast(SpaceInfo, LSync, Request, NewBaseRequest, Attempts),
            do_emit_tree_broadcast(SpaceInfo, RSync, Request, NewBaseRequest, Attempts)
    end.

do_emit_tree_broadcast(_SpaceInfo, [], _Request, _NewBaseRequest, _Attempts) ->
    ok;
do_emit_tree_broadcast(#space_info{space_id = SpaceId} = SpaceInfo, SyncWith, Request, #treebroadcast{depth = Depth} = BaseRequest, Attempts) when Attempts > 0 ->
    PushTo = lists:nth(crypto:rand_uniform(1, 1 + length(SyncWith)), SyncWith),
    [LEdge | _] = SyncWith,
    REdge = lists:last(SyncWith),
    ReqEncoder = encoder_method(get_message_type(Request)),
    RequestData = iolist_to_binary(apply(dbsync_pb, ReqEncoder, [Request])),
    SyncRequest = BaseRequest#treebroadcast{ledge = LEdge, redge = REdge, depth = Depth + 1,
        message_type = a2l(get_message_type(Request)), input = RequestData, space_id = utils:ensure_binary(SpaceId
        )},
    SyncRequestData = dbsync_pb:encode_treebroadcast(SyncRequest),
    MsgId = provider_proxy_con:get_msg_id(),


    {AnswerDecoderName, AnswerType} = {rtcore, atom},


    RTRequest = #rtrequest{answer_decoder_name = a2l(AnswerDecoderName), answer_type = a2l(AnswerType), input = SyncRequestData, message_decoder_name = a2l(dbsync),
        message_id = MsgId, message_type = a2l(utils:record_type(SyncRequest)), module_name = a2l(dbsync), protocol_version = 1, synch = true},
    RTRequestData = iolist_to_binary(rtcore_pb:encode_rtrequest(RTRequest)),

    URL = dbsync_utils:get_provider_url(PushTo),
    Timeout = 1000,
    provider_proxy_con:send({URL, <<"oneprovider">>}, MsgId, RTRequestData),
    receive
        {response, MsgId, AnswerStatus, WorkerAnswer} ->
            provider_proxy_con:report_ack({URL, <<"oneprovider">>}),
            ?debug("Answer for inter-provider pull request: ~p ~p", [AnswerStatus, WorkerAnswer]),
            case AnswerStatus of
                ?VOK ->
                    #atom{value = RValue} = erlang:apply(pb_module(AnswerDecoderName), decoder_method(AnswerType), [WorkerAnswer]),
                    case RValue of
                        ?VOK -> ok;
                        _ -> throw(RValue)
                    end;
                InvalidStatus ->
                    ?error("Cannot send message ~p due to invalid answer status: ~p", [get_message_type(SyncRequest), InvalidStatus]),
                    do_emit_tree_broadcast(SpaceInfo, SyncWith, Request, BaseRequest, Attempts - 1)
            end
    after Timeout ->
        provider_proxy_con:report_timeout({URL, <<"oneprovider">>}),
        do_emit_tree_broadcast(SpaceInfo, SyncWith, Request, BaseRequest, Attempts - 1)
    end;
do_emit_tree_broadcast(_SpaceInfo, _SyncWith, _Request, _BaseRequest, 0) ->
    {error, unable_to_connect}.



%% ====================================================================
%% Misc
%% ====================================================================

%% a2l/1
%% ====================================================================
%% @doc Converts given list/atom to atom.
%% @end
-spec a2l(AtomOrList :: atom() | list()) -> Result :: atom().
%% ====================================================================
a2l(Atom) when is_atom(Atom) ->
    atom_to_list(Atom);
a2l(List) when is_list(List) ->
    List.


%% encoder_method/1
%% ====================================================================
%% @doc Get name of protobuf's encoder method for given message type.
%% @end
-spec encoder_method(MType :: atom() | list()) -> EncoderName :: atom().
%% ====================================================================
encoder_method(MType) when is_atom(MType) ->
    encoder_method(atom_to_list(MType));
encoder_method(MType) when is_list(MType) ->
    list_to_atom("encode_" ++ MType).


%% decoder_method/1
%% ====================================================================
%% @doc Get name of protobuf's decoder method for given message type.
%% @end
-spec decoder_method(MType :: atom() | list()) -> DecoderName :: atom().
%% ====================================================================
decoder_method(MType) when is_atom(MType) ->
    decoder_method(atom_to_list(MType));
decoder_method(MType) when is_list(MType) ->
    list_to_atom("decode_" ++ MType).


%% pb_module/1
%% ====================================================================
%% @doc Get protobuf's decoder's module name for given decoder's name.
%% @end
-spec pb_module(ModuleName :: atom() | list()) -> PBModule :: atom().
%% ====================================================================
pb_module(ModuleName) ->
    list_to_atom(utils:ensure_list(ModuleName) ++ "_pb").


%% get_message_type/1
%% ====================================================================
%% @doc Get message type.
%% @end
-spec get_message_type(Msg :: tuple()) -> Type :: atom().
%% ====================================================================
get_message_type(Msg) when is_tuple(Msg) ->
    utils:record_type(Msg).
