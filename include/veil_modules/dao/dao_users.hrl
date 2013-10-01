%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: dao_users header
%% @end
%% ===================================================================

-ifndef(DAO_USERS).
-define(DAO_USERS, 1).

%% This record defines a user and is handled as a database document
-record(user, {login = "", name = "", teams = undefined, email_list = [], dn_list = []}).

%% Declarations of lowest and highest user IDs. Those UIDs are used as #user record UUID. 
-define(LOWEST_USER_ID, 20000).
-define(HIGHEST_USER_ID, 65000).

-endif.