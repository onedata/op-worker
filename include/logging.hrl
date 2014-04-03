%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2713 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains convenient logging macros.
%% @end
%% ===================================================================

-ifndef(LOGGING_HRL).
-define(LOGGING_HRL, 1).

% List of available loglevels
-define(LOGLEVEL_LIST, [debug, info, notice, warning, error, critical, alert, emergency]).

%% ===================================================================
% Macros that should be used in code for logging
% xxx_stacktrace logs will automatically include stack trace, 
% provided this is enabled in env variable 'include_stacktrace'

% Compilation with skip_debug flag will remove all debug messages from code

-ifdef(skip_debug).
-define(debug(_Message), ok).
-define(debug(_Format, _Args), ok).
-define(debug_stacktrace(_Message), ok).
-define(debug_stacktrace(_Format, _Args), ok).
-endif.

-ifndef(skip_debug).
-define(debug(_Message), ?do_log(0, _Message, false)).
-define(debug(_Format, _Args), ?do_log(0, _Format, _Args, false)).
-define(debug_stacktrace(_Message), ?do_log(0, _Message, true)).
-define(debug_stacktrace(_Format, _Args), ?do_log(0, _Format, _Args, true)).
-endif.

-define(info(_Message), ?do_log(1, _Message, false)).
-define(info(_Format, _Args), ?do_log(1, _Format, _Args, false)).
-define(info_stacktrace(_Message), ?do_log(1, _Message, true)).
-define(info_stacktrace(_Format, _Args), ?do_log(1, _Format, _Args, true)).

-define(notice(_Message), ?do_log(2, _Message, false)).
-define(notice(_Format, _Args), ?do_log(2, _Format, _Args, false)).
-define(notice_stacktrace(_Message), ?do_log(2, _Message, true)).
-define(notice_stacktrace(_Format, _Args), ?do_log(2, _Format, _Args, true)).

-define(warning(_Message), ?do_log(3, _Message, false)).
-define(warning(_Format, _Args), ?do_log(3, _Format, _Args, false)).
-define(warning_stacktrace(_Message), ?do_log(3, _Message, true)).
-define(warning_stacktrace(_Format, _Args), ?do_log(3, _Format, _Args, true)).

-define(error(_Message), ?do_log(4, _Message, false)).
-define(error(_Format, _Args), ?do_log(4, _Format, _Args, false)).
-define(error_stacktrace(_Message), ?do_log(4, _Message, true)).
-define(error_stacktrace(_Format, _Args), ?do_log(4, _Format, _Args, true)).

-define(critical(_Message), ?do_log(5, _Message, false)).
-define(critical(_Format, _Args), ?do_log(5, _Format, _Args, false)).
-define(critical_stacktrace(_Message), ?do_log(5, _Message, true)).
-define(critical_stacktrace(_Format, _Args), ?do_log(5, _Format, _Args, true)).

-define(alert(_Message), ?do_log(6, _Message, false)).
-define(alert(_Format, _Args), ?do_log(6, _Format, _Args, false)).
-define(alert_stacktrace(_Message), ?do_log(6, _Message, true)).
-define(alert_stacktrace(_Format, _Args), ?do_log(6, _Format, _Args, true)).

-define(emergency(_Message), ?do_log(7, _Message, false)).
-define(emergency(_Format, _Args), ?do_log(7, _Format, _Args, false)).
-define(emergency_stacktrace(_Message), ?do_log(7, _Message, true)).
-define(emergency_stacktrace(_Format, _Args), ?do_log(7, _Format, _Args, true)).


%% ===================================================================
% Convienience macros for development purposes

% Prints a single variable
-define(dump(_Arg), io:format(user, "[DUMP] ~s: ~p~n~n", [??_Arg, _Arg])).

% Prints a list of variables
-define(dump_all(_ListOfVariables),
	lists:foreach(
		fun({_Name, _Value}) ->
			io:format(user, "[DUMP] ~s: ~p~n~n", [_Name, _Value])
		end, lists:zip(string:tokens(??_ListOfVariables, "[] ,"), _ListOfVariables))
).


%% ===================================================================
%% Macros used internally

-define(do_log(_LoglevelAsInt, _Message, _IncludeStackTrace),
    ?do_log(_LoglevelAsInt, _Message, [], _IncludeStackTrace)
).

-define(do_log(_LoglevelAsInt, _Format, _Args, _IncludeStackTrace),
    case logger:should_log(_LoglevelAsInt) of
        false -> ok;
        true -> logger:dispatch_log(_LoglevelAsInt, ?gather_metadata, _Format, _Args, _IncludeStackTrace)
    end
).

% Resolves current process's state and returns it as metadata proplist
% Must be called from original function where the log is,
% so that the process info makes sense
-define(gather_metadata,
    [{node, node()}, {pid, self()}, {line, ?LINE}] ++
        logger:parse_process_info(process_info(self(), current_function)) ++
        case get(user_id) of
            undefined -> [];
            _ -> [{dn, get(user_id)}]
        end
).

-endif.