%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains list of communiques that can be processed.
%% @end
%% ===================================================================

%% white lists defined as lists of pairs {user_type, white_list_for_user_type}
-define(MessagesWhiteList, [
  {standard_user, [fusemessage, remotefilemangement, channelregistration, channelclose, atom, handshakerequest, handshakeack]},
  {developer, [fusemessage, remotefilemangement, channelregistration, channelclose, atom, handshakerequest, handshakeack]}
]).
-define(AtomsWhiteList, [
  {standard_user, [ping, event]},
  {developer, [ping, event]}
]).


%% List of messages that needs FuseId to be present in connection state prior to process them.
%% If FuseId is not set and one of those messages arrive, cluster will immediately send error.
-define(SessionDependentMessages, [fusemessage, channelregistration, channelclose]).