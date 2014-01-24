.. _central_logging_backend:

central_logging_backend
=======================

	:Authors: Lukasz Opiola
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This is a gen_event module (lager backend), responsible for intercepting logs and sending them to central sink via request_dispatcher.
	:Behaviours: `gen_event <http://www.erlang.org/doc/man/gen_event.html>`_

Function Index
~~~~~~~~~~~~~~~

	* :ref:`code_change/3 <central_logging_backend;code_change/3>`
	* :ref:`handle_call/2 <central_logging_backend;handle_call/2>`
	* :ref:`handle_event/2 <central_logging_backend;handle_event/2>`
	* :ref:`handle_info/2 <central_logging_backend;handle_info/2>`
	* :ref:`init/1 <central_logging_backend;init/1>`
	* :ref:`terminate/2 <central_logging_backend;terminate/2>`

Function Details
~~~~~~~~~~~~~~~~~

	.. erl:module:: central_logging_backend

	.. _`central_logging_backend;code_change/3`:

	.. erl:function:: code_change(OldVsn, State :: term(), Extra :: term()) -> Result

	* **OldVsn:** Vsn | {down, Vsn}
	* **Result:** {ok, NewState :: term()} | {error, Reason :: term()}
	* **Vsn:** term()

	 gen_event:code_change/3 

	.. _`central_logging_backend;handle_call/2`:

	.. erl:function:: handle_call(Request :: term(), State :: term()) -> Result

	* **NewState:** term()
	* **Reply:** term()
	* **Result:** {ok, Reply, NewState}

	 gen_event:handle_call/2 

	.. _`central_logging_backend;handle_event/2`:

	.. erl:function:: handle_event(Request :: term(), State :: term()) -> Result

	* **NewState:** term()
	* **Result:** {ok, NewState}

	 gen_event:handle_event/2 

	.. _`central_logging_backend;handle_info/2`:

	.. erl:function:: handle_info(Info :: term(), State :: term()) -> Result

	* **NewState:** term()
	* **Result:** {ok, NewState}

	 gen_event:handle_info/2 

	.. _`central_logging_backend;init/1`:

	.. erl:function:: init(Args :: term()) -> Result

	* **Result:** {ok, term()}

	gen_event callback init/1 Called after installing this handler into lager_event. Returns its loglevel ( {mask, 255} ) as Status.

	.. _`central_logging_backend;terminate/2`:

	.. erl:function:: terminate(Reason, State :: term()) -> Any :: term()

	* **Reason:** normal | shutdown | {shutdown, term()} | term()

	 gen_event:terminate/2 

