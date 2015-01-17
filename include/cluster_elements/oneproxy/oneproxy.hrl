%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2013 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% oneproxy definitions.
%%% @end
%%%-------------------------------------------------------------------
-ifndef(ONEPROXY_HRL).
-define(ONEPROXY_HRL, 1).

-record(oneproxy_state, {timeout = timer:minutes(1), endpoint}).

%% oneproxy listeners
-define(ONEPROXY_DISPATCHER, oneproxy_dispatcher).
-define(ONEPROXY_REST, oneproxy_rest).

-define(DER_CERTS_DIR,      "der_certs").
-define(LOG_DEBUG_PREFIX,   "[ DEBUG ] ").
-define(LOG_INFO_PREFIX,    "[ INFO ] ").
-define(LOG_WARNING_PREFIX, "[ WARNING ] ").
-define(LOG_ERROR_PREFIX,   "[ ERROR ] ").


-endif.