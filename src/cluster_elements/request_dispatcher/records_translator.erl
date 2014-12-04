%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module is able to do additional translation of record
%% decoded using protocol buffer e.g. it can change record "atom" to
%% Erlang atom type.
%% @end
%% ===================================================================

-module(records_translator).
-include("communication_protocol_pb.hrl").
-include("remote_file_management_pb.hrl").
-include("fuse_messages_pb.hrl").
-include_lib("ctool/include/logging.hrl").

%% ====================================================================
%% API
%% ====================================================================
-export([translate/2, translate_to_record/1, get_answer_decoder_and_type/1]).

%% ====================================================================
%% API functions
%% ====================================================================

%% translate/2
%% ====================================================================
%% @doc Translates record to simpler terms if possible.
-spec translate(Record :: tuple(), DecoderName :: string()) -> Result when
  Result ::  term().
%% ====================================================================
translate(Record, _DecoderName) when is_record(Record, atom) ->
  try
    list_to_existing_atom(Record#atom.value)
  catch
    _:_ ->
      ?warning("Unsupported atom: ~p", [Record#atom.value]),
      throw(message_not_supported)
  end;

translate(Record, DecoderName) when is_tuple(Record) ->
    RecordList = lists:reverse(tuple_to_list(Record)),
    {NotBin, BinPrefix} = lists:splitwith(fun(Elem) -> not is_binary(Elem) end, RecordList),
    RecordList2 = case BinPrefix of
        [End, Type | T] when is_binary(End), is_list(Type) ->
            try
                DecodedEnd = erlang:apply(list_to_existing_atom(DecoderName ++ "_pb"), list_to_existing_atom("decode_" ++ Type), [End]),
                NotBin ++ [DecodedEnd | [list_to_existing_atom(Type) | T]]
            catch
            _:_ ->
                ?warning("Can not translate record: ~p, using decoder: ~p", [Record, DecoderName]),
                RecordList
            end;
        _ -> RecordList
    end,
    TmpAns = lists:foldl(fun(E, Sum) -> [translate(E, DecoderName) | Sum] end, [], RecordList2),
    list_to_tuple(TmpAns);

translate(Record, _DecoderName) ->
  Record.

%% translate_to_record/1
%% ====================================================================
%% @doc Translates term to record if possible.
-spec translate_to_record(Value :: term()) -> Result when
  Result ::  tuple() | term().
%% ====================================================================
translate_to_record(Value) when is_atom(Value) ->
    #atom{value = atom_to_list(Value)};

translate_to_record(Value) ->
    Value.


%% get_answer_decoder_and_type/1
%% ====================================================================
%% @doc Returns answer's decoder name and message type for given request message.
%% @end
-spec get_answer_decoder_and_type(Message :: #fusemessage{} | #remotefilemangement{}) ->
    {AnswerDecoderName :: atom(), AnswerType :: atom()} | no_return().
%% ====================================================================
get_answer_decoder_and_type(#fusemessage{input = #getfileattr{}}) ->
    {fuse_messages, fileattr};
get_answer_decoder_and_type(#fusemessage{input = #getfilelocation{}}) ->
    {fuse_messages, filelocation};
get_answer_decoder_and_type(#fusemessage{input = #getnewfilelocation{}}) ->
    {fuse_messages, filelocation};
get_answer_decoder_and_type(#fusemessage{input = #filenotused{}}) ->
    {communication_protocol, atom};
get_answer_decoder_and_type(#fusemessage{input = #renamefile{}}) ->
    {communication_protocol, atom};
get_answer_decoder_and_type(#fusemessage{input = #deletefile{}}) ->
    {communication_protocol, atom};
get_answer_decoder_and_type(#fusemessage{input = #createdir{}}) ->
    {communication_protocol, atom};
get_answer_decoder_and_type(#fusemessage{input = #changefileowner{}}) ->
    {communication_protocol, atom};
get_answer_decoder_and_type(#fusemessage{input = #changefilegroup{}}) ->
    {communication_protocol, atom};
get_answer_decoder_and_type(#fusemessage{input = #changefileperms{}}) ->
    {communication_protocol, atom};
get_answer_decoder_and_type(#fusemessage{input = #checkfileperms{}}) ->
    {communication_protocol, atom};
get_answer_decoder_and_type(#fusemessage{input = #updatetimes{}}) ->
    {communication_protocol, atom};
get_answer_decoder_and_type(#fusemessage{input = #createlink{}}) ->
    {communication_protocol, atom};
get_answer_decoder_and_type(#fusemessage{input = #renewfilelocation{}}) ->
    {fuse_messages, filelocationvalidity};
get_answer_decoder_and_type(#fusemessage{input = #getfilechildrencount{}}) ->
    {fuse_messages, filechildrencount};
get_answer_decoder_and_type(#fusemessage{input = #getfilechildren{}}) ->
    {fuse_messages, filechildren};
get_answer_decoder_and_type(#fusemessage{input = #getlink{}}) ->
    {fuse_messages, linkinfo};
get_answer_decoder_and_type(#fusemessage{input = #testchannel{}}) ->
    {communication_protocol, atom};
get_answer_decoder_and_type(#fusemessage{input = #createfileack{}}) ->
    {communication_protocol, atom};
get_answer_decoder_and_type(#fusemessage{input = #getfileuuid{}}) ->
    {fuse_messages, fileuuid};
get_answer_decoder_and_type(#fusemessage{input = #getxattr{}}) ->
    {fuse_messages, xattr};
get_answer_decoder_and_type(#fusemessage{input = #setxattr{}}) ->
    {communication_protocol, atom};
get_answer_decoder_and_type(#fusemessage{input = #removexattr{}}) ->
    {communication_protocol, atom};
get_answer_decoder_and_type(#fusemessage{input = #listxattr{}}) ->
    {fuse_messages, xattrlist};
get_answer_decoder_and_type(#fusemessage{input = #getacl{}}) ->
    {fuse_messages, acl};
get_answer_decoder_and_type(#fusemessage{input = #setacl{}}) ->
    {communication_protocol, atom};
get_answer_decoder_and_type(#fusemessage{input = #synchronizefileblock{}}) ->
    {communication_protocol, atom};
get_answer_decoder_and_type(#fusemessage{input = #fileblockmodified{}}) ->
    {communication_protocol, atom};
get_answer_decoder_and_type(#fusemessage{input = #filetruncated{}}) ->
    {communication_protocol, atom};
get_answer_decoder_and_type(#fusemessage{input = #getfileblockmap{}}) ->
    {fuse_messages, fileblockmap};

get_answer_decoder_and_type(#remotefilemangement{input = #createfile{}}) ->
    {communication_protocol, atom};
get_answer_decoder_and_type(#remotefilemangement{input = #getattr{}}) ->
    {remote_file_management, storageattibutes};
get_answer_decoder_and_type(#remotefilemangement{input = #deletefileatstorage{}}) ->
    {communication_protocol, atom};
get_answer_decoder_and_type(#remotefilemangement{input = #truncatefile{}}) ->
    {communication_protocol, atom};
get_answer_decoder_and_type(#remotefilemangement{input = #changepermsatstorage{}}) ->
    {communication_protocol, atom};
get_answer_decoder_and_type(#remotefilemangement{input = #readfile{}}) ->
    {remote_file_management, filedata};
get_answer_decoder_and_type(#remotefilemangement{input = #writefile{}}) ->
    {remote_file_management, writeinfo};
get_answer_decoder_and_type(Unk) ->
    throw({unknown_message, Unk}).