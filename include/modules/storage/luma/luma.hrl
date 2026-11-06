%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Generic header file containing definitions of macros used by modules
%%% associated with LUMA DB.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(LUMA_HRL).
-define(LUMA_HRL, 1).

%% Mapping user schemes
-define(ONEDATA_USER_SCHEME, <<"onedataUser">>).
-define(IDP_USER_SCHEME, <<"idpUser">>).

%% Mapping group schemes
-define(ONEDATA_GROUP_SCHEME, <<"onedataGroup">>).
-define(IDP_ENTITLEMENT_SCHEME, <<"idpEntitlement">>).

% Macros defining LUMA feed types
-define(AUTO_FEED, auto).
-define(LOCAL_FEED, local).
-define(EXTERNAL_FEED, external).

% Macros defining luma_db constraints.
%% These constraints are checked before performing operations on a few LUMA DB tables.
%% They are checked to ensure that operation (GET/PUT/DELETE) on given table
%% is relevant taking into consideration type of underlying storage.
-define(POSIX_STORAGE, posix_storage).
-define(IMPORTED_STORAGE, imported_storage).
-define(NON_IMPORTED_STORAGE, non_imported_storage).

% Macros defining options passed to luma_db:store function
-define(FORCE_OVERWRITE, force_overwrite).
-define(NO_OVERWRITE, no_overwrite).

-endif.