%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains list of communicates that can be processed.
%% @end
%% ===================================================================

-ifndef(MESSAGES_WHITE_LIST_HRL).
-define(MESSAGES_WHITE_LIST_HRL, 1).

%% white lists of messages that can be processed by oneprovider
-define(MessagesWhiteList, [
  clustermsg, answer, atom, channelregistration, channelclose, fusemessage, getfilelocation,
  getnewfilelocation, getfileattr, fileattr, filelocation, createfileack, filenotused, renewfilelocation,
  filelocationvalidity, getfilechildren, filechildren, createdir, deletefile, createlink, getlink,
  linkinfo, renamefile, changefileowner, changefilegroup, changefileperms, updatetimes, testchannel,
  testchannelanswer, handshakerequest, handshakerequest_envvariable, handshakerequest_certconfirmation, handshakeresponse, handshakeack,
  createstoragetestfilerequest, createstoragetestfileresponse, storagetestfilemodifiedrequest, storagetestfilemodifiedresponse,
  clientstorageinfo, clientstorageinfo_storageinfo, getstatfs, statfsinfo, eventfilterconfig, eventaggregatorconfig, eventtransformerconfig,
  eventstreamconfig, eventproducerconfig, eventmessage, changeremoteloglevel, logmessage, remotefilemangement, createfile,
  deletefileatstorage, truncatefile, readfile, filedata, writefile, writeinfo, changepermsatstorage,
  rtrequest, rtresponse, docupdated, message, spacemodified, spaceremoved, usermodified, userremoved, groupmodified, groupremoved
]).

%% white lists of atoms that located inside mssags
-define(AtomsWhiteList, [
  ping, event_producer_config_request, is_write_enabled, ack
]).

%% list of modules that can process messages
-define(VisibleModules, [
  central_logger, cluster_rengine, control_panel, dao, fslogic, gateway,
  rtransfer, rule_manager, dns_worker, remote_files_manager, dbsync
]).

%% list of messages decoders that can be used
-define(DecodersList, [
  communication_protocol, fuse_messages, logging, remote_file_management, rtcore, dbsync, gr_communication_protocol, gr_messages
]).

%% List of messages that needs FuseId to be present in connection state prior to process them.
%% If FuseId is not set and one of those messages arrive, cluster will immediately send error.
-define(SessionDependentMessages, [fusemessage, channelregistration, channelclose]).

-endif.
