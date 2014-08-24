%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 26. Jul 2014 03:07
%%%-------------------------------------------------------------------
-module(provider_proxy).
-author("Rafal Slota").

-include("communication_protocol_pb.hrl").
-include("fuse_messages_pb.hrl").
-include("veil_modules/fslogic/fslogic.hrl").
-include("remote_file_management_pb.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([reroute_pull_message/4, reroute_push_message/3]).

reroute_pull_message(ProviderId, {GlobalID, AccessToken}, FuseId, Message) ->
    {ok, #{<<"urls">> := URLs}} = registry_providers:get_provider_info(ProviderId),

    [URL | _] = URLs,

    TargetModule =
        case Message of
            #fusemessage{}              -> fslogic;
            #remotefilemangement{}      -> remote_files_manager
        end,

    ?info("Reroute pull (-> ~p): ~p", [URL, Message]),

    {AnswerDecoderName, AnswerType} = records_translator:get_answer_decoder_and_type(Message),
    MsgBytes = encode(Message),

    TokenHash = access_token_hash(GlobalID, AccessToken),

    MsgId = provider_proxy_con:get_msg_id(),

    ClusterMessage =
        #clustermsg{synch = true, protocol_version = 1, module_name = a2l(TargetModule), message_id = MsgId,
                    answer_decoder_name = a2l(AnswerDecoderName), answer_type = a2l(AnswerType), input = MsgBytes,
                    token_hash = TokenHash, global_user_id = GlobalID,
                    message_decoder_name = a2l(get_message_decoder(Message)), message_type = a2l(get_message_type(Message))},

    ?info("1 ~p", [FuseId]),

    CLMBin = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(ClusterMessage)),

    ProviderMsg = #providermsg{message_type = "clustermsg", input = CLMBin, fuse_id = vcn_utils:ensure_binary(FuseId)},
    PRMBin = erlang:iolist_to_binary(communication_protocol_pb:encode_providermsg(ProviderMsg)),

    %% AnswerBin = communicate_bin({ProviderId, URL}, PRMBin),

    provider_proxy_con:send(URL, MsgId, PRMBin),

    receive
        {response, MsgId, AnswerStatus, WorkerAnswer} ->
            ?info("Answer0: ~p ~p", [AnswerStatus, WorkerAnswer]),
            case AnswerStatus of
                ?VOK ->
                    Answer = erlang:apply(pb_module(AnswerDecoderName), decoder_method(AnswerType), [WorkerAnswer]),
                    ?info("Answer1: ~p", [Answer]),
                    Answer;
                InvalidStatus ->
                    ?error("Cannot reroute message ~p due to invalid answer status: ~p", [get_message_type(Message), InvalidStatus]),
                    throw({invalid_status, InvalidStatus})
            end
    after 5000 ->
        throw(reroute_timeout)
    end.

reroute_push_message({ProviderId, FuseId}, Message, MessageDecoder) ->
    {ok, #{<<"urls">> := URLs}} = registry_providers:get_provider_info(ProviderId),
    ?info("Reroute push to: ~p", [URLs]),

    [URL | _] = URLs,

    MType = a2l(get_message_type(Message)),
    MsgBytes = erlang:iolist_to_binary(erlang:apply(pb_module(MessageDecoder), encoder_method(MType), [Message])),

    Answer =
        #answer{answer_status = "push", message_id = 0, message_type = MType,
                message_decoder_name = MessageDecoder, worker_answer = MsgBytes},

    CLMBin = erlang:iolist_to_binary(communication_protocol_pb:encode_answer(Answer)),

    ProviderMsg = #providermsg{message_type = "answer", input = CLMBin, fuse_id = vcn_utils:ensure_binary(FuseId)},
    PRMBin = erlang:iolist_to_binary(communication_protocol_pb:encode_providermsg(ProviderMsg)),

    provider_proxy_con:send(URL, 0, PRMBin).


%% communicate_bin({ProviderId, URL}, PRMBin) ->
%%     {ok, Socket} = connect(URL, 5555, [{certfile, global_registry:get_provider_cert_path()}, {keyfile, global_registry:get_provider_key_path()}]),
%%     send(Socket, PRMBin),
%%     case recv(Socket, 5000) of
%%         {ok, Data} ->
%%             ?info("Received data from ~p: ~p", [ProviderId, Data]),
%%             Data;
%%         {error, Reason} ->
%%             ?error("Could not receive response from provider ~p due to ~p", [ProviderId, Reason]),
%%             throw(Reason)
%%     end.

access_token_hash(_GlobalId, AccessToken) ->
    base64:encode(crypto:hash(sha512, AccessToken)).


encode(#fusemessage{input = Input, message_type = MType} = FM) ->
    ?info("Message o encode0: ~p", [Input]),
    FMBin = erlang:iolist_to_binary(erlang:apply(fuse_messages_pb, encoder_method(MType), [Input])),
    ?info("Message o encode1: ~p", [FM#fusemessage{input = FMBin}]),
    erlang:iolist_to_binary(fuse_messages_pb:encode_fusemessage(FM#fusemessage{input = FMBin, message_type = a2l(MType)}));
encode(#remotefilemangement{input = Input, message_type = MType} = RFM) ->
    ?info("Message o encode0: ~p", [Input]),
    RFMBin = erlang:iolist_to_binary(erlang:apply(remote_file_management_pb, encoder_method(MType), [Input])),
    ?info("Message o encode1: ~p", [RFM#remotefilemangement{input = RFMBin}]),
    erlang:iolist_to_binary(remote_file_management_pb:encode_remotefilemangement(RFM#remotefilemangement{input = RFMBin, message_type = a2l(MType)})).



get_message_type(Msg) when is_tuple(Msg) ->
    element(1, Msg).

get_message_decoder(#fusemessage{}) ->
    fuse_messages;
get_message_decoder(#remotefilemangement{}) ->
    remote_file_management;
get_message_decoder(Msg) ->
    ?error("Cannot get decoder for message of unknown type: ~p", [get_message_type(Msg)]),
    throw(unknown_decoder).





encoder_method(MType) when is_atom(MType) ->
    encoder_method(atom_to_list(MType));
encoder_method(MType) when is_list(MType) ->
    list_to_atom("encode_" ++ MType).

decoder_method(MType) when is_atom(MType) ->
    decoder_method(atom_to_list(MType));
decoder_method(MType) when is_list(MType) ->
    list_to_atom("decode_" ++ MType).


pb_module(ModuleName) ->
    list_to_atom(ensure_list(ModuleName) ++ "_pb").


a2l(Atom) when is_atom(Atom) ->
    atom_to_list(Atom);
a2l(List) when is_list(List) ->
    List.

ensure_list(Binary) when is_binary(Binary) ->
    binary_to_list(Binary);
ensure_list(Atom) when is_atom(Atom) ->
    atom_to_list(Atom);
ensure_list(Num) when is_integer(Num) ->
    integer_to_list(Num);
ensure_list(List) when is_list(List) ->
    List.

