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

-endif.