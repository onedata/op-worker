%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Translations between protobuff and internal protocol
%%% @end
%%%-------------------------------------------------------------------
-module(translator).
-author("Tomasz Lichon").
-author("Rafal Slota").

-include("global_definitions.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("proto/oneclient/stream_messages.hrl").
-include("proto/oneclient/handshake_messages.hrl").
-include("proto/oneclient/event_messages.hrl").
-include("proto/oneclient/diagnostic_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("clproto/include/messages.hrl").

%% API
-export([translate_from_protobuf/1, translate_to_protobuf/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% traslate protobuf record to internal record
%% @end
%%--------------------------------------------------------------------
-spec translate_from_protobuf(tuple() | undefined) -> tuple() | undefined.
translate_from_protobuf(#'Status'{code = Code, description = Desc}) ->
    #status{code = Code, description = Desc};
translate_from_protobuf(#'Event'{event = {_, Record}}) ->
    #event{event = translate_from_protobuf(Record)};
translate_from_protobuf(#'ReadEvent'{} = Record) ->
    #read_event{
       counter = Record#'ReadEvent'.counter,
       file_uuid = Record#'ReadEvent'.file_id,
       size = Record#'ReadEvent'.size,
       blocks = [translate_from_protobuf(B) || B <- Record#'ReadEvent'.blocks]
      };
translate_from_protobuf(#'WriteEvent'{} = Record) ->
    #write_event{
       counter = Record#'WriteEvent'.counter,
       file_uuid = Record#'WriteEvent'.file_id,
       size = Record#'WriteEvent'.size,
       file_size = Record#'WriteEvent'.file_size,
       blocks = [translate_from_protobuf(B) || B <- Record#'WriteEvent'.blocks]
      };
translate_from_protobuf(#'FileBlock'{offset = Off, size = S, file_id = FID, storage_id = SID}) ->
    #file_block{offset = Off, size = S, file_id = FID, storage_id = SID};
translate_from_protobuf(#'HandshakeRequest'{token = Token, session_id = SessionId}) ->
    #handshake_request{auth = translate_from_protobuf(Token), session_id = SessionId};
translate_from_protobuf(#'MessageStream'{stream_id = StmId, sequence_number = SeqNum}) ->
    #message_stream{stream_id = StmId, sequence_number = SeqNum};
translate_from_protobuf(#'EndOfMessageStream'{}) ->
    #end_of_message_stream{};
translate_from_protobuf(#'Token'{value = Val}) ->
    #auth{macaroon = Val};
translate_from_protobuf(#'Ping'{data = Data}) ->
    #ping{data = Data};
translate_from_protobuf(#'GetProtocolVersion'{}) ->
    #get_protocol_version{};
translate_from_protobuf(#'FuseRequest'{fuse_request = {_, Record}}) ->
    #fuse_request{fuse_request = translate_from_protobuf(Record)};
translate_from_protobuf(#'GetFileAttr'{entry_type = 'PATH', entry = Path}) ->
    #get_file_attr{entry = {path, Path}};
translate_from_protobuf(#'GetFileAttr'{entry_type = 'UUID', entry = UUID}) ->
    #get_file_attr{entry = {uuid, UUID}};
translate_from_protobuf(#'GetFileChildren'{uuid = UUID, offset = Offset, size = Size}) ->
    #get_file_children{uuid = UUID, offset = Offset, size = Size};
translate_from_protobuf(#'CreateDir'{parent_uuid = ParentUUID, name = Name, mode = Mode}) ->
    #create_dir{parent_uuid = ParentUUID, name = Name, mode = Mode};
translate_from_protobuf(#'DeleteFile'{uuid = UUID}) ->
    #delete_file{uuid = UUID};
translate_from_protobuf(#'UpdateTimes'{uuid = UUID, atime = ATime, mtime = MTime,
                                       ctime = CTime}) ->
    #update_times{uuid = UUID, atime = ATime, mtime = MTime, ctime = CTime};
translate_from_protobuf(#'ChangeMode'{uuid = UUID, mode = Mode}) ->
    #change_mode{uuid = UUID, mode = Mode};
translate_from_protobuf(#'Rename'{uuid = UUID, target_path = TargetPath}) ->
    #rename{uuid = UUID, target_path = TargetPath};
translate_from_protobuf(#'GetNewFileLocation'{name = Name, parent_uuid = ParentUUID, mode = Mode, flags = Flags}) ->
    #get_new_file_location{name = Name, parent_uuid = ParentUUID, mode = Mode, flags = Flags};
translate_from_protobuf(#'GetFileLocation'{uuid = UUID, flags = Flags}) ->
    #get_file_location{uuid = UUID, flags = Flags};
translate_from_protobuf(#'GetHelperParams'{storage_id = SID, force_cluster_proxy = ForceCP}) ->
    #get_helper_params{storage_id = SID, force_cluster_proxy = ForceCP};
translate_from_protobuf(#'Truncate'{uuid = UUID, size = Size}) ->
    #truncate{uuid = UUID, size = Size};
translate_from_protobuf(#'Close'{uuid = UUID}) ->
    #close{uuid = UUID};
translate_from_protobuf(undefined) ->
    undefined.

%%--------------------------------------------------------------------
%% @doc
%% translate internal record to protobuf record
%% @end
%%--------------------------------------------------------------------
-spec translate_to_protobuf(tuple() | undefined) -> tuple() | undefined.
translate_to_protobuf(#status{code = Code, description = Desc}) ->
    {status, #'Status'{code = Code, description = Desc}};
translate_to_protobuf(#event_subscription_cancellation{id = Id}) ->
    {event_subscription, #'EventSubscription'{
                            event_subscription = {event_subscription_cancellation,
                                                  #'EventSubscriptionCancellation'{
                                                     id = Id
                                                    }
                                                 }
                           }};
translate_to_protobuf(#read_event_subscription{} = Sub) ->
    {event_subscription, #'EventSubscription'{
                            event_subscription = {read_event_subscription,
                                                  #'ReadEventSubscription'{
                                                     id = Sub#read_event_subscription.id,
                                                     counter_threshold = Sub#read_event_subscription.producer_counter_threshold,
                                                     time_threshold = Sub#read_event_subscription.producer_time_threshold,
                                                     size_threshold = Sub#read_event_subscription.producer_size_threshold
                                                    }
                                                 }
                           }};
translate_to_protobuf(#write_event_subscription{} = Sub) ->
    {event_subscription, #'EventSubscription'{
                            event_subscription = {write_event_subscription,
                                                  #'WriteEventSubscription'{
                                                     id = Sub#write_event_subscription.id,
                                                     counter_threshold = Sub#write_event_subscription.producer_counter_threshold,
                                                     time_threshold = Sub#write_event_subscription.producer_time_threshold,
                                                     size_threshold = Sub#write_event_subscription.producer_size_threshold
                                                    }
                                                 }}};
translate_to_protobuf(#handshake_response{session_id = Id}) ->
    {handshake_response, #'HandshakeResponse'{session_id = Id}};
translate_to_protobuf(#message_stream{stream_id = StmId, sequence_number = SeqNum}) ->
    #'MessageStream'{stream_id = StmId, sequence_number = SeqNum};
translate_to_protobuf(#message_stream_reset{}) ->
    {message_stream_reset, #'MessageStreamReset'{}};
translate_to_protobuf(#message_request{stream_id = StmId,
                                       lower_sequence_number = LowerSeqNum, upper_sequence_number = UpperSeqNum}) ->
    {message_request, #'MessageRequest'{stream_id = StmId,
                                        lower_sequence_number = LowerSeqNum, upper_sequence_number = UpperSeqNum}};
translate_to_protobuf(#message_acknowledgement{stream_id = StmId, sequence_number = SeqNum}) ->
    {message_acknowledgement,
     #'MessageAcknowledgement'{stream_id = StmId, sequence_number = SeqNum}};
translate_to_protobuf(#pong{data = Data}) ->
    {pong, #'Pong'{data = Data}};
translate_to_protobuf(#protocol_version{major = Major, minor = Minor}) ->
    {protocol_version, #'ProtocolVersion'{major = Major, minor = Minor}};
translate_to_protobuf(#fuse_response{status = Status, fuse_response = FuseResponse}) ->
    {fuse_response, #'FuseResponse'{
                       status = #'Status'{
                                   code = Status#status.code,
                                   description = Status#status.description
                                  },
                       fuse_response = translate_to_protobuf(FuseResponse)
                      }};
translate_to_protobuf(#child_link{uuid = UUID, name = Name}) ->
    #'ChildLink'{uuid = UUID, name = Name};
translate_to_protobuf(#file_attr{} = FileAttr) ->
    {file_attr, #'FileAttr'{
                   uuid = FileAttr#file_attr.uuid,
                   name = FileAttr#file_attr.name,
                   mode = FileAttr#file_attr.mode,
                   uid = FileAttr#file_attr.uid,
                   gid = FileAttr#file_attr.gid,
                   atime = FileAttr#file_attr.atime,
                   mtime = FileAttr#file_attr.mtime,
                   ctime = FileAttr#file_attr.ctime,
                   type = FileAttr#file_attr.type,
                   size = FileAttr#file_attr.size
                  }};
translate_to_protobuf(#file_children{child_links = FileEntries}) ->
    {file_children, #'FileChildren'{child_links = lists:map(fun(ChildLink) ->
                                                                    translate_to_protobuf(ChildLink)
                                                            end, FileEntries)}};
translate_to_protobuf(#helper_params{helper_name = HelperName, helper_args = HelpersArgs}) ->
    {helper_params, #'HelperParams'{helper_name = HelperName,
                                    helper_args = lists:map(fun(HelpersArg) ->
                                                                    translate_to_protobuf(HelpersArg)
                                                            end, HelpersArgs)}};
translate_to_protobuf(#helper_arg{key = Key, value = Value}) ->
    #'HelperArg'{key = Key, value = Value};
translate_to_protobuf(#file_location{} = Record) ->
    {file_location, #'FileLocation'{
                       uuid = Record#file_location.uuid,
                       provider_id = Record#file_location.provider_id,
                       storage_id = Record#file_location.storage_id,
                       file_id = Record#file_location.file_id,
                       blocks = lists:map(fun(Block) ->
                                                  translate_to_protobuf(Block)
                                          end, Record#file_location.blocks)
                      }};
translate_to_protobuf(#file_block{offset = Off, size = S, file_id = FID, storage_id = SID}) ->
    #'FileBlock'{offset = Off, size = S, file_id = FID, storage_id = SID};
translate_to_protobuf(undefined) ->
    undefined.

%%%===================================================================
%%% Internal functions
%%%===================================================================
