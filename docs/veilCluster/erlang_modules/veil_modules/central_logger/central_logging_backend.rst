.. _central_logging_backend:

central_logging_backend
=======================

	:Authors: Lukasz Opiola
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This is a gen_event module (lager backend), responsible for intercepting logs and sending them to central sink via request_dispatcher.
	:Behaviours: `gen_event <http://www.erlang.org/doc/man/gen_event.html>`_

Function Index
~~~~~~~~~~~~~~~

	* :ref:`code_change/3 <central_logging_backend:code_change/3>`
	* :ref:`handle_call/2 <central_logging_backend:handle_call/2>`
	* :ref:`handle_event/2 <central_logging_backend:handle_event/2>`
	* :ref:`handle_info/2 <central_logging_backend:handle_info/2>`
	* :ref:`init/1 <central_logging_backend:init/1>`
	* :ref:`terminate/2 <central_logging_backend:terminate/2>`

Function Details
~~~~~~~~~~~~~~~~~

	.. _`central_logging_backend:code_change/3`:

	.. function:: code_change(OldVsn, State :: term(), Extra :: term()) -> Result
		:noindex:

	* **OldVsn:** Vsn | {down, Vsn}
	* **Result:** {ok, NewState :: term()} | {error, Reason :: term()}
	* **Vsn:** term()

	<a href="http://www.erlang.org/doc/man/gen_event.html#Module:code_change-3">gen_event:code_change/3</a>

	.. _`central_logging_backend:handle_call/2`:

	.. function:: handle_call(Request :: term(), State :: term()) -> Result
		:noindex:

	* **NewState:** term()
	* **Reply:** term()
	* **Result:** {ok, Reply, NewState}

	<a href="http://www.erlang.org/doc/man/gen_event.html#Module:handle_call-2">gen_event:handle_call/2</a>

	.. _`central_logging_backend:handle_event/2`:

	.. function:: handle_event(Request :: term(), State :: term()) -> Result
		:noindex:

	* **NewState:** term()
	* **Result:** {ok, NewState}

	<a href="http://www.erlang.org/doc/man/gen_event.html#Module:handle_event-2">gen_event:handle_event/2</a>

	.. _`central_logging_backend:handle_info/2`:

	.. function:: handle_info(Info :: term(), State :: term()) -> Result
		:noindex:

	* **NewState:** term()
	* **Result:** {ok, NewState}

	<a href="http://www.erlang.org/doc/man/gen_event.html#Module:handle_info-2">gen_event:handle_info/2</a>

	.. _`central_logging_backend:init/1`:

	.. function:: init(Args :: term()) -> Result
		:noindex:

	* **Result:** {ok, term()}

	gen_event callback init/1 <br /> Called after installing this handler into lager_event. Returns its loglevel ( {mask, 255} ) as Status.

	.. _`central_logging_backend:terminate/2`:

	.. function:: terminate(Reason, State :: term()) -> Any :: term()
		:noindex:

	* **Reason:** normal | shutdown | {shutdown, term()} | term()

	<a href="http://www.erlang.org/doc/man/gen_event.html#Module:terminate-2">gen_event:terminate/2</a>

