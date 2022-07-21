%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This file contains definitions of macros used by audit log machinery.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(AUDIT_LOG_HRL).
-define(AUDIT_LOG_HRL, 1).


-define(SYSTEM_AUDIT_LOG_ENTRY_SOURCE, <<"system">>).
-define(USER_AUDIT_LOG_ENTRY_SOURCE, <<"user">>).


-define(DEBUG_AUDIT_LOG_SEVERITY, <<"debug">>).
-define(INFO_AUDIT_LOG_SEVERITY, <<"info">>).
-define(NOTICE_AUDIT_LOG_SEVERITY, <<"notice">>).
-define(WARNING_AUDIT_LOG_SEVERITY, <<"warning">>).
-define(ALERT_AUDIT_LOG_SEVERITY, <<"alert">>).
-define(ERROR_AUDIT_LOG_SEVERITY, <<"error">>).
-define(CRITICAL_AUDIT_LOG_SEVERITY, <<"critical">>).
-define(EMERGENCY_AUDIT_LOG_SEVERITY, <<"emergency">>).

-define(AUDIT_LOG_SEVERITY_LEVELS, [
    ?DEBUG_AUDIT_LOG_SEVERITY, ?INFO_AUDIT_LOG_SEVERITY, ?NOTICE_AUDIT_LOG_SEVERITY,
    ?WARNING_AUDIT_LOG_SEVERITY, ?ALERT_AUDIT_LOG_SEVERITY,
    ?ERROR_AUDIT_LOG_SEVERITY, ?CRITICAL_AUDIT_LOG_SEVERITY, ?EMERGENCY_AUDIT_LOG_SEVERITY
]).


-record(audit_log_append_request, {
    severity = ?INFO_AUDIT_LOG_SEVERITY :: audit_log:entry_severity(),
    source = ?SYSTEM_AUDIT_LOG_ENTRY_SOURCE :: audit_log:entry_source(),
    content :: json_utils:json_term()
}).


-endif.
