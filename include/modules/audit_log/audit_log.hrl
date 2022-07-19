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

-ifndef(AUDIT_LGO_HRL).
-define(AUDIT_LGO_HRL, 1).


-define(SYSTEM_ENTRY_SOURCE, <<"system">>).
-define(USER_ENTRY_SOURCE, <<"user">>).


-define(DEBUG_ENTRY_SEVERITY, <<"debug">>).
-define(INFO_ENTRY_SEVERITY, <<"info">>).
-define(NOTICE_ENTRY_SEVERITY, <<"notice">>).
-define(WARNING_ENTRY_SEVERITY, <<"warning">>).
-define(ALERT_ENTRY_SEVERITY, <<"alert">>).
-define(ERROR_ENTRY_SEVERITY, <<"error">>).
-define(CRITICAL_ENTRY_SEVERITY, <<"critical">>).
-define(EMERGENCY_ENTRY_SEVERITY, <<"emergency">>).

-define(ENTRY_SEVERITY_LEVELS, [
    ?DEBUG_ENTRY_SEVERITY, ?INFO_ENTRY_SEVERITY, ?NOTICE_ENTRY_SEVERITY,
    ?WARNING_ENTRY_SEVERITY, ?ALERT_ENTRY_SEVERITY,
    ?ERROR_ENTRY_SEVERITY, ?CRITICAL_ENTRY_SEVERITY, ?EMERGENCY_ENTRY_SEVERITY
]).


-record(audit_log_append_request, {
    severity = ?INFO_ENTRY_SEVERITY :: audit_log:entry_severity(),
    source = ?SYSTEM_ENTRY_SOURCE :: audit_log:entry_source(),
    content :: json_utils:json_term()
}).


-endif.