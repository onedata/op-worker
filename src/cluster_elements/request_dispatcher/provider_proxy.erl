%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module handles inter-provider message rerouting.
%% @end
%% ===================================================================
-module(provider_proxy).
-author("Rafal Slota").

-include("communication_protocol_pb.hrl").
-include("fuse_messages_pb.hrl").
-include("oneprovider_modules/fslogic/fslogic.hrl").
-include("remote_file_management_pb.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/global_registry/gr_providers.hrl").

%% API
-export([reroute_pull_message/4, reroute_push_message/3]).


%% ====================================================================
%% API functions
%% ====================================================================


%% reroute_pull_message/4
%% ====================================================================
%% @doc Reroute given pull request (fusemessage or remotefilemangement) to selected Provider.
%%      This method returns decoded #answer.input or fails with exception.
%% @end
-spec reroute_pull_message( ProviderId :: binary(),
                            {GlobalID :: binary(), AccessToken :: binary()},
                            FuseId :: binary() | list(),
                            Message :: #fusemessage{} | #remotefilemangement{}) -> Response :: term() | no_return().
%% ====================================================================
reroute_pull_message(ProviderId, {GlobalID, AccessToken}, FuseId, Message) ->
    {ok, #provider_details{urls = URLs}} = gr_providers:get_details(provider, ProviderId),

    %% @todo: use all URLs for redundancy
    [URL | _] = URLs,   %% Select provider URL for rerouting

    TargetModule =
        case Message of
            #fusemessage{}              -> fslogic;
            #remotefilemangement{}      -> remote_files_manager
        end,

    {AnswerDecoderName, AnswerType} = records_translator:get_answer_decoder_and_type(Message),
    MsgBytes = encode(Message),

    TokenHash = utils:access_token_hash(AccessToken),

    MsgId = provider_proxy_con:get_msg_id(),

    ClusterMessage =
        #clustermsg{protocol_version = 1, module_name = a2l(TargetModule), message_id = MsgId,
                    answer_decoder_name = a2l(AnswerDecoderName), answer_type = a2l(AnswerType), input = MsgBytes,
                    token_hash = TokenHash, global_user_id = GlobalID,
                    message_decoder_name = a2l(get_message_decoder(Message)), message_type = a2l(get_message_type(Message))},

    CLMBin = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(ClusterMessage)),

    ProviderMsg = #providermsg{message_type = "clustermsg", input = CLMBin, fuse_id = utils:ensure_binary(FuseId)},
    PRMBin = erlang:iolist_to_binary(communication_protocol_pb:encode_providermsg(ProviderMsg)),

    provider_proxy_con:send(URL, MsgId, PRMBin),

    Timeout = timeout_for_message(Message),

    receive
        {response, MsgId, AnswerStatus, WorkerAnswer} ->
            provider_proxy_con:report_ack(URL),
            ?debug("Answer for inter-provider pull request: ~p ~p", [AnswerStatus, WorkerAnswer]),
            case AnswerStatus of
                ?VOK ->
                    Answer = erlang:apply(pb_module(AnswerDecoderName), decoder_method(AnswerType), [WorkerAnswer]),
                    Answer;
                InvalidStatus ->
                    ?error("Cannot reroute message ~p due to invalid answer status: ~p", [get_message_type(Message), InvalidStatus]),
                    throw({invalid_status, InvalidStatus})
            end
    after Timeout ->
        provider_proxy_con:report_timeout(URL),
        throw(reroute_timeout)
    end.


%% reroute_push_message/3
%% ====================================================================
%% @doc Reroute given push request to selected Provider.
%%      This method returns decoded #answer.input or fails with exception.
%% @end
-spec reroute_push_message({ProviderId :: binary(), FuseId :: binary() | list()},
    Message :: term(), MessageDecoder :: list()) -> ok | error | no_return().
%% ====================================================================
reroute_push_message({ProviderId, FuseId}, Message, MessageDecoder) ->
    {ok, #provider_details{urls = URLs}} = gr_providers:get_details(provider, ProviderId),
    ?info("Reroute push ~p -> ~p", [Message, URLs]),

    [URL | _] = URLs,

    MType = a2l(get_message_type(Message)),
    MsgBytes = erlang:iolist_to_binary(erlang:apply(pb_module(MessageDecoder), encoder_method(MType), [Message])),

    Answer =
        #answer{answer_status = "push", message_id = 0, message_type = MType,
                message_decoder_name = MessageDecoder, worker_answer = MsgBytes},

    CLMBin = erlang:iolist_to_binary(communication_protocol_pb:encode_answer(Answer)),

    ProviderMsg = #providermsg{message_type = "answer", input = CLMBin, fuse_id = utils:ensure_binary(FuseId)},
    PRMBin = erlang:iolist_to_binary(communication_protocol_pb:encode_providermsg(ProviderMsg)),

    provider_proxy_con:send(URL, 0, PRMBin).


%% ====================================================================
%% Internal functions
%% ====================================================================


%% encode/1
%% ====================================================================
%% @doc Encodes given message.
%% @end
-spec encode(Message :: #fusemessage{} | #remotefilemangement{}) -> EncodedMessage :: iolist().
%% ====================================================================
encode(#fusemessage{input = Input, message_type = MType} = FM) ->
    FMBin = erlang:iolist_to_binary(erlang:apply(fuse_messages_pb, encoder_method(MType), [Input])),
    erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FM#fusemessage{input = FMBin, message_type = a2l(MType)}));
encode(#remotefilemangement{input = Input, message_type = MType} = RFM) ->
    RFMBin = erlang:iolist_to_binary(erlang:apply(remote_file_management_pb, encoder_method(MType), [Input])),
    erlang:iolist_to_binary(remote_file_management_pb:encode_remotefilemangement(RFM#remotefilemangement{input = RFMBin, message_type = a2l(MType)})).


%% get_message_type/1
%% ====================================================================
%% @doc Get message type.
%% @end
-spec get_message_type(Msg :: tuple()) -> Type :: atom().
%% ====================================================================
get_message_type(Msg) when is_tuple(Msg) ->
    utils:record_type(Msg).


%% get_message_decoder/1
%% ====================================================================
%% @doc Get decoder name for given message.
%% @end
-spec get_message_decoder(Message :: #fusemessage{} | #remotefilemangement{}) -> Decoder :: atom() | no_return().
%% ====================================================================
get_message_decoder(#fusemessage{}) ->
    fuse_messages;
get_message_decoder(#remotefilemangement{}) ->
    remote_file_management;
get_message_decoder(Msg) ->
    ?error("Cannot get decoder for message of unknown type: ~p", [get_message_type(Msg)]),
    throw(unknown_decoder).


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


%% timeout_for_message/1
%% ====================================================================
%% @doc Returns timeout (ms) for given request.
%% @end
-spec timeout_for_message(Request :: term()) -> Timeout :: non_neg_integer().
%% ====================================================================
timeout_for_message(#fusemessage{input = #getfilechildren{}}) ->
    timer:seconds(3);
timeout_for_message(#fusemessage{input = #getfileattr{}}) ->
    timer:seconds(2);
timeout_for_message(#fusemessage{input = #renamefile{}}) ->
    timer:hours(1);
timeout_for_message(#remotefilemangement{input = #readfile{size = Bytes}}) ->
    timer:seconds(3) + Bytes; %% 1 byte -> 1ms ~= 1kB/s
timeout_for_message(#remotefilemangement{input = #writefile{data = Data}}) ->
    timer:seconds(3) + size(Data); %% 1 byte -> 1ms ~= 1kB/s
timeout_for_message(_) ->
    timer:seconds(10).
