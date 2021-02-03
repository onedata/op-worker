%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Common macros used in modules associated with offline access mechanism.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(OFFLINE_ACCESS_MANAGER_HRL).
-define(OFFLINE_ACCESS_MANAGER_HRL, 1).

% Variables controlling offline token renewal backoff
-define(MIN_OFFLINE_TOKEN_RENEWAL_INTERVAL_SEC, 2).
-define(OFFLINE_TOKEN_RENEWAL_BACKOFF_RATE, 2).
-define(MAX_OFFLINE_TOKEN_RENEWAL_INTERVAL_SEC, 180).  % 3 minutes

-endif.