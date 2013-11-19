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
  {standard_user, [fusemessage, remotefilemangement, atom]},
  {developer, [fusemessage, remotefilemangement, atom]}
]).
-define(AtomsWhiteList, [
  {standard_user, [ping]},
  {developer, [ping]}
]).