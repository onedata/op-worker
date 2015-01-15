%%%-------------------------------------------------------------------
%%% @author lichon
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 15. Jan 2015 21:13
%%%-------------------------------------------------------------------

%% oneproxy listeners
-define(ONEPROXY_DISPATCHER, oneproxy_dispatcher).
-define(ONEPROXY_REST, oneproxy_rest).

-define(DER_CERTS_DIR,      "der_certs").
-define(LOG_DEBUG_PREFIX,   "[ DEBUG ] ").
-define(LOG_INFO_PREFIX,    "[ INFO ] ").
-define(LOG_WARNING_PREFIX, "[ WARNING ] ").
-define(LOG_ERROR_PREFIX,   "[ ERROR ] ").

-record(oneproxy_state, {timeout = timer:minutes(1), endpoint}).