%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'
%% @end
%% ===================================================================
%% @doc: This module handles log dispatching.
%% @end
%% ===================================================================

-module(logger).
-include("logging.hrl").
-export([should_log/1, dispatch_log/5, parse_process_info/1]).
-export([set_loglevel/1, set_console_loglevel/1, set_include_stacktrace/1]).
-export([get_current_loglevel/0, get_default_loglevel/0, get_console_loglevel/0, get_include_stacktrace/0]).
-export([loglevel_int_to_atom/1, loglevel_atom_to_int/1]).

%% should_log/1
%% ====================================================================
%% @doc Determines if logs with provided loglevel should be logged or discarded
%% @end 
-spec should_log(LoglevelAsInt :: integer()) -> boolean().
%% ====================================================================
should_log(LevelAsInt) ->
	case application:get_env(veil_cluster_node, current_loglevel) of
		{ok, Int} when LevelAsInt >= Int -> true;
		_ -> false
	end.  


%% dispatch_log/5
%% ====================================================================
%% @doc Logs the log locally (it will be intercepted by central_logging_backend and sent to central sink)
-spec dispatch_log(LoglevelAsInt :: integer(), Metadata :: [tuple()], Format :: string(),
	Args :: string(), IncludeStacktrace :: boolean()) -> ok | {error, lager_not_running}.
%% ====================================================================
dispatch_log(LoglevelAsInt, Metadata, Format, Args, IncludeStacktrace) ->
	Severity = loglevel_int_to_atom(LoglevelAsInt),
	lager:log(Severity, Metadata, compute_message(Format, Args, IncludeStacktrace)).


%% set_loglevel/1
%% ====================================================================
%% @doc Changes current global loglevel to desired. Argument can be loglevel as int or atom
%% 'default' atom can be used to set it back to default
%% @end 
-spec set_loglevel(Loglevel :: integer() | atom()) -> ok | {error, badarg}.
%% ====================================================================
set_loglevel(Loglevel) when is_atom(Loglevel) ->
	try
		LevelAsInt = case Loglevel of 
			default -> get_default_loglevel();
			Atom -> loglevel_atom_to_int(Atom)
		end,
		set_loglevel(LevelAsInt)
	catch _:_ ->
		{error, badarg}
	end;

set_loglevel(Loglevel) when is_integer(Loglevel) andalso (Loglevel >= 0) andalso (Loglevel =< 7)  ->
	application:set_env(veil_cluster_node, current_loglevel, Loglevel);

set_loglevel(_) ->
	{error, badarg}.


%% set_console_loglevel/1
%% ====================================================================
%% @doc Changes current console loglevel to desired. Argument can be loglevel as int or atom
%% 'default' atom can be used to set it back to default - default is what is defined in sys.config
%% @end 
-spec set_console_loglevel(Loglevel :: integer() | atom()) -> ok | {error, badarg}.
%% ====================================================================
set_console_loglevel(Loglevel) when is_integer(Loglevel) andalso (Loglevel >= 0) andalso (Loglevel =< 7)  ->
	set_console_loglevel(loglevel_int_to_atom(Loglevel));

set_console_loglevel(Loglevel) when is_atom(Loglevel) ->
	try
		LevelAsAtom = case Loglevel of 
			default -> 
				{ok, Proplist} = application:get_env(lager, handlers),
				proplists:get_value(lager_console_backend, Proplist);
			Atom -> 
				% Makes sure that the atom is recognizable as loglevel
				loglevel_int_to_atom(loglevel_atom_to_int(Atom))
		end,
		gen_event:call(lager_event, lager_console_backend, {set_loglevel, LevelAsAtom}),
		ok
	catch _:_ ->
		{error, badarg}
	end;

set_console_loglevel(_) ->
	{error, badarg}.



%% set_include_stacktrace/1
%% ====================================================================
%% @doc Changes include_stacktrace env to true or false
%% @end 
-spec set_include_stacktrace(boolean()) -> ok | {error, badarg}.
%% ====================================================================
set_include_stacktrace(Flag) when is_boolean(Flag) ->
	application:set_env(veil_cluster_node, include_stacktrace, Flag);

set_include_stacktrace(_) ->
	{error, badarg}.

	

%% get_current_loglevel/0
%% ====================================================================
%% @doc Returns current loglevel as set in application's env
%% @end 
-spec get_current_loglevel() -> integer().
%% ====================================================================
get_current_loglevel() ->
	{ok, Int} = application:get_env(veil_cluster_node, current_loglevel),
	Int.


%% get_default_loglevel/0
%% ====================================================================
%% @doc Returns default loglevel as set in application's env
%% @end 
-spec get_default_loglevel() -> integer().
%% ====================================================================
get_default_loglevel() ->
	{ok, Int} = application:get_env(veil_cluster_node, default_loglevel),
	Int.


%% get_console_loglevel/0
%% ====================================================================
%% @doc Returns current console loglevel
%% @end 
-spec get_console_loglevel() -> integer().
%% ====================================================================
get_console_loglevel() ->
	{mask, Mask} = gen_event:call(lager_event, lager_console_backend, get_loglevel),
	% lager_util:mask_to_levels(Mask) returns list of allowed log level, first of 
	% which is the lowest loglevel
	loglevel_atom_to_int(lists:nth(1, lager_util:mask_to_levels(Mask))).


%% get_include_stacktrace/0
%% ====================================================================
%% @doc Returns get_include_stacktrace env value
%% @end 
-spec get_include_stacktrace() -> boolean().
%% ====================================================================
get_include_stacktrace() ->
	{ok, Boolean} = application:get_env(veil_cluster_node, include_stacktrace),
	Boolean.


%% loglevel_int_to_atom/1
%% ====================================================================
%% @doc Returns loglevel name associated with loglevel number
-spec loglevel_int_to_atom(LoglevelAsInt :: integer()) -> atom().
%% ====================================================================
loglevel_int_to_atom(LoglevelAsInt) ->
	lists:nth(LoglevelAsInt + 1, [debug, info, notice, warning, error, critical, alert, emergency]).


%% loglevel_atom_to_int/1
%% ====================================================================
%% @doc Returns loglevel number associated with loglevel name
-spec loglevel_atom_to_int(LoglevelAsAtom :: atom()) -> integer().
%% ====================================================================
loglevel_atom_to_int(LoglevelAsAtom) ->
	case LoglevelAsAtom of
		debug -> 0;
		info -> 1;
		notice -> 2;
		warning -> 3;
		error -> 4; 
		critical -> 5; 
		alert -> 6;
		emergency -> 7
	end.


%% parse_process_info/1
%% ====================================================================
%% @doc Changes standard 'process_info' tuple into metadata proplist
-spec parse_process_info(ProcessInfo :: tuple()) -> [tuple()].
%% ====================================================================
parse_process_info({_, {Module, Function, Arity}}) ->
	[{module, Module}, {function, Function}, {arity, Arity}].



%% ===================================================
%% Internal functions

% Computes a log message string, possibly including stacktrace
compute_message(Format, Args, IncludeStackTrace) -> 
	{F, A} = case (IncludeStackTrace and get_include_stacktrace()) of
		false -> {Format, Args};
		true -> {Format ++ "~nStacktrace: ~p", Args ++ [erlang:get_stacktrace()]}
	end,
	lists:flatten(io_lib:format(F, A)).
