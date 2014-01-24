.. _logger:

logger
======

	:Authors: Lukasz Opiola
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This module handles log dispatching.

Function Index
~~~~~~~~~~~~~~~

	* :ref:`dispatch_log/5 <logger:dispatch_log/5>`
	* :ref:`get_console_loglevel/0 <logger:get_console_loglevel/0>`
	* :ref:`get_current_loglevel/0 <logger:get_current_loglevel/0>`
	* :ref:`get_default_loglevel/0 <logger:get_default_loglevel/0>`
	* :ref:`get_include_stacktrace/0 <logger:get_include_stacktrace/0>`
	* :ref:`loglevel_atom_to_int/1 <logger:loglevel_atom_to_int/1>`
	* :ref:`loglevel_int_to_atom/1 <logger:loglevel_int_to_atom/1>`
	* :ref:`parse_process_info/1 <logger:parse_process_info/1>`
	* :ref:`set_console_loglevel/1 <logger:set_console_loglevel/1>`
	* :ref:`set_include_stacktrace/1 <logger:set_include_stacktrace/1>`
	* :ref:`set_loglevel/1 <logger:set_loglevel/1>`
	* :ref:`should_log/1 <logger:should_log/1>`

Function Details
~~~~~~~~~~~~~~~~~

	.. _`logger:dispatch_log/5`:

	.. function:: dispatch_log(LoglevelAsInt :: integer(), Metadata :: [tuple()], Format :: string(), Args :: string(), IncludeStacktrace :: boolean()) -> ok | {error, lager_not_running}
		:noindex:

	Logs the log locally (it will be intercepted by central_logging_backend and sent to central sink)

	.. _`logger:get_console_loglevel/0`:

	.. function:: get_console_loglevel() -> integer()
		:noindex:

	Returns current console loglevel

	.. _`logger:get_current_loglevel/0`:

	.. function:: get_current_loglevel() -> integer()
		:noindex:

	Returns current loglevel as set in application's env

	.. _`logger:get_default_loglevel/0`:

	.. function:: get_default_loglevel() -> integer()
		:noindex:

	Returns default loglevel as set in application's env

	.. _`logger:get_include_stacktrace/0`:

	.. _`logger:loglevel_atom_to_int/1`:

	.. function:: loglevel_int_to_atom(LoglevelAsInt :: integer()) -> atom()
		:noindex:

	Returns loglevel name associated with loglevel number

	.. _`logger:loglevel_int_to_atom/1`:

	.. _`logger:parse_process_info/1`:

	.. function:: parse_process_info(ProcessInfo :: tuple()) -> [tuple()]
		:noindex:

	Changes standard 'process_info' tuple into metadata proplist

	.. _`logger:set_console_loglevel/1`:

	.. function:: set_console_loglevel(Loglevel :: integer() | atom()) -> ok | {error, badarg}
		:noindex:

	Changes current console loglevel to desired. Argument can be loglevel as int or atom 'default' atom can be used to set it back to default - default is what is defined in sys.config

	.. _`logger:set_include_stacktrace/1`:

	.. function:: set_include_stacktrace(boolean()) -> ok | {error, badarg}
		:noindex:

	Changes include_stacktrace env to true or false

	.. _`logger:set_loglevel/1`:

	.. function:: set_loglevel(Loglevel :: integer() | atom()) -> ok | {error, badarg}
		:noindex:

	Changes current global loglevel to desired. Argument can be loglevel as int or atom 'default' atom can be used to set it back to default

	.. _`logger:should_log/1`:

	.. function:: should_log(LoglevelAsInt :: integer()) -> boolean()
		:noindex:

	Determines if logs with provided loglevel should be logged or discarded

