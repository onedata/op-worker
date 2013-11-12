%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of logger module, using eunit tests.
%% @end
%% ===================================================================
-module(logger_tests).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include("logging.hrl").

main_test_() ->
	{setup,
		fun() ->
			application:set_env(veil_cluster_node, current_loglevel, 2),
			application:set_env(veil_cluster_node, default_loglevel, 0) 
		end,
		fun(_) ->
			ok
		end,
		[
			{"should_log, set/get_loglevel, set/get_default_loglevel",
				fun() ->
					?assertEqual(logger:get_current_loglevel(), 2), 
					?assert(not logger:should_log(1)),
					?assert(not logger:should_log(0)),
					?assert(logger:should_log(5)),
					?assert(logger:should_log(2)),
					?assert(logger:should_log(7)),
					logger:set_loglevel(error), 
					?assertEqual(logger:get_current_loglevel(), 4),
					?assert(not logger:should_log(1)),
					?assert(not logger:should_log(0)),
					?assert(not logger:should_log(2)),
					?assert(logger:should_log(6)),
					?assert(logger:should_log(7)),
					logger:set_loglevel(default),
					?assertEqual(logger:get_default_loglevel(), logger:get_current_loglevel()),
					?assert(logger:should_log(1)),
					?assert(logger:should_log(4)),
					?assert(logger:should_log(5)),
					?assert(logger:should_log(6)),
					?assert(logger:should_log(3))
				end
			},

			{"parse_process_info",
				fun() ->
					Proplist = logger:parse_process_info({pid, {some_module, some_fun, some_arity}}),
					?assertEqual(Proplist, [{module, some_module}, {function, some_fun}, {arity, some_arity}]) 
				end
			},

			{"loglevel conversion",
				fun() ->
					?assertEqual(debug, logger:loglevel_int_to_atom(logger:loglevel_atom_to_int(debug))),
					?assertEqual(notice, logger:loglevel_int_to_atom(logger:loglevel_atom_to_int(notice))),
					?assertEqual(5, logger:loglevel_atom_to_int(logger:loglevel_int_to_atom(5))),
					?assertEqual(1, logger:loglevel_atom_to_int(logger:loglevel_int_to_atom(1)))
				end
			}
		]
	}.


lager_interfacing_test_() ->
	{setup, 
		fun() ->
			application:set_env(veil_cluster_node, current_loglevel, 0),
			meck:new(lager)
		end,
		fun(_) ->
			ok = meck:unload(lager)
		end,
		[
			{"dispatch_log, set/get_include_stacktrace, compute_message, logging macros",
				fun() -> 
					meck:expect(lager, log, 
						fun (debug, _, "debug message") -> ok;
							(info, _, "info message") -> ok;
							(warning, _, "warning message" ++ _Stacktrace) -> ok;
							(error, _, "error message") -> ok;
							(emergency, _, "emergency message") -> ok 
						end),

					logger:set_include_stacktrace(true),
					logger:dispatch_log(0, [], "debug ~s", ["message"], false),
					logger:dispatch_log(1, [], "info message", [], false),
					logger:dispatch_log(3, [], "warning message", [], true),
					logger:dispatch_log(4, [], "error ~s", ["message"], false),
					logger:set_include_stacktrace(false),
					logger:dispatch_log(7, [], "emergency ~s", ["message"], true), 
					?debug("debug message"),
					?debug("debug ~s", ["message"]),
					?info("info message"),
					?warning_stacktrace("warning message"),
					?error("error message"),
					?emergency("emergency message"),
					?assert(meck:validate(lager))
				end
			}
		]
	}.

-endif.