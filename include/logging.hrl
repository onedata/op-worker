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

% List of available loglevels
-define(LOGLEVEL_LIST, [debug, info, notice, warning, error, critical, alert, emergency]).

%% ===================================================================
% Macros that should be used in code for logging
% xxx_stacktrace logs will automatically include stack trace, 
% provided this is enabled in env variable 'include_stacktrace'

% Compilation with skip_debug flag will remove all debug messages from code

-ifdef(skip_debug).
-define(debug(Message), ok).
-define(debug(Format, Args), ok).
-define(debug_stacktrace(Message), ok).
-define(debug_stacktrace(Format, Args), ok).
-endif.

-ifndef(skip_debug).
-define(debug(Message), ?do_log(0, Message, false)).
-define(debug(Format, Args), ?do_log(0, Format, Args, false)).
-define(debug_stacktrace(Message), ?do_log(0, Message, true)).
-define(debug_stacktrace(Format, Args), ?do_log(0, Format, Args, true)).
-endif.

-define(info(Message), ?do_log(1, Message, false)).
-define(info(Format, Args), ?do_log(1, Format, Args, false)).
-define(info_stacktrace(Message), ?do_log(1, Message, true)).
-define(info_stacktrace(Format, Args), ?do_log(1, Format, Args, true)).

-define(notice(Message), ?do_log(2, Message, false)).
-define(notice(Format, Args), ?do_log(2, Format, Args, false)).
-define(notice_stacktrace(Message), ?do_log(2, Message, true)).
-define(notice_stacktrace(Format, Args), ?do_log(2, Format, Args, true)).

-define(warning(Message), ?do_log(3, Message, false)).
-define(warning(Format, Args), ?do_log(3, Format, Args, false)).
-define(warning_stacktrace(Message), ?do_log(3, Message, true)).
-define(warning_stacktrace(Format, Args), ?do_log(3, Format, Args, true)).

-define(error(Message), ?do_log(4, Message, false)).
-define(error(Format, Args), ?do_log(4, Format, Args, false)).
-define(error_stacktrace(Message), ?do_log(4, Message, true)).
-define(error_stacktrace(Format, Args), ?do_log(4, Format, Args, true)).

-define(critical(Message), ?do_log(5, Message, false)).
-define(critical(Format, Args), ?do_log(5, Format, Args, false)).
-define(critical_stacktrace(Message), ?do_log(5, Message, true)).
-define(critical_stacktrace(Format, Args), ?do_log(5, Format, Args, true)).

-define(alert(Message), ?do_log(6, Message, false)).
-define(alert(Format, Args), ?do_log(6, Format, Args, false)).
-define(alert_stacktrace(Message), ?do_log(6, Message, true)).
-define(alert_stacktrace(Format, Args), ?do_log(6, Format, Args, true)).

-define(emergency(Message), ?do_log(7, Message, false)).
-define(emergency(Format, Args), ?do_log(7, Format, Args, false)).
-define(emergency_stacktrace(Message), ?do_log(7, Message, true)).
-define(emergency_stacktrace(Format, Args), ?do_log(7, Format, Args, true)).



%% ===================================================================
% Convienience macros for development purposes

% Prints a single variable
-define(dump(Arg), io:format("[DUMP] ~s: ~p~n~n", [??Arg, Arg])).

% Prints a list of variables
-define(dump_all(ListOfVariables), 
	lists:foreach(
		fun({Name, Value}) -> 
			io:format("[DUMP] ~s: ~p~n~n", [Name, Value]) 
		end, lists:zip(string:tokens(??ListOfVariables, "[] ,"), ListOfVariables))
).



%% ===================================================================
%% Macros used internally

-define(do_log(LoglevelAsInt, Message, IncludeStackTrace),
	?do_log(LoglevelAsInt, Message, [], IncludeStackTrace)
).

-define(do_log(LoglevelAsInt, Format, Args, IncludeStackTrace),	
	case logger:should_log(LoglevelAsInt) of
		false -> ok;
		true -> logger:dispatch_log(LoglevelAsInt, ?gather_metadata, Format, Args, IncludeStackTrace)
	end
).

% Resolves current process's state and returns it as metadata proplist
% Must be called from original function where the log is,
% so that the process info makes sense
-define(gather_metadata,
	[{node, node()}, {pid, self()}, {line, ?LINE}] ++ 
		logger:parse_process_info(process_info(self(), current_function))
).