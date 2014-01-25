.. _cluster_manager:

cluster_manager
===============

	:Authors: Michal Wrzeszcz
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This module coordinates central cluster.
	:Behaviours: `gen_server <http://www.erlang.org/doc/man/gen_server.html>`_

Function Index
~~~~~~~~~~~~~~~

	* :ref:`code_change/3 <cluster_manager:code_change/3>`
	* :ref:`handle_call/3 <cluster_manager:handle_call/3>`
	* :ref:`handle_cast/2 <cluster_manager:handle_cast/2>`
	* :ref:`handle_info/2 <cluster_manager:handle_info/2>`
	* :ref:`init/1 <cluster_manager:init/1>`
	* :ref:`monitoring_loop/1 <cluster_manager:monitoring_loop/1>`
	* :ref:`monitoring_loop/2 <cluster_manager:monitoring_loop/2>`
	* :ref:`start_link/0 <cluster_manager:start_link/0>`
	* :ref:`start_link/1 <cluster_manager:start_link/1>`
	* :ref:`start_monitoring_loop/2 <cluster_manager:start_monitoring_loop/2>`
	* :ref:`stop/0 <cluster_manager:stop/0>`
	* :ref:`terminate/2 <cluster_manager:terminate/2>`

Function Details
~~~~~~~~~~~~~~~~~

	.. _`cluster_manager:code_change/3`:

	.. function:: code_change(OldVsn, State :: term(), Extra :: term()) -> Result
		:noindex:

	* **OldVsn:** Vsn | {down, Vsn}
	* **Result:** {ok, NewState :: term()} | {error, Reason :: term()}
	* **Vsn:** term()

	<a href="http://www.erlang.org/doc/man/gen_server.html#Module:code_change-3">gen_server:code_change/3</a>

	.. _`cluster_manager:handle_call/3`:

	.. function:: handle_call(Request :: term(), From :: {pid(), Tag :: term()}, State :: term()) -> Result
		:noindex:

	* **NewState:** term()
	* **Reason:** term()
	* **Reply:** term()
	* **Result:** {reply, Reply, NewState} | {reply, Reply, NewState, Timeout} | {reply, Reply, NewState, hibernate} | {noreply, NewState} | {noreply, NewState, Timeout} | {noreply, NewState, hibernate} | {stop, Reason, Reply, NewState} | {stop, Reason, NewState}
	* **Timeout:** non_neg_integer() | infinity

	<a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_call-3">gen_server:handle_call/3</a>

	.. _`cluster_manager:handle_cast/2`:

	.. function:: handle_cast(Request :: term(), State :: term()) -> Result
		:noindex:

	* **NewState:** term()
	* **Result:** {noreply, NewState} | {noreply, NewState, Timeout} | {noreply, NewState, hibernate} | {stop, Reason :: term(), NewState}
	* **Timeout:** non_neg_integer() | infinity

	<a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_cast-2">gen_server:handle_cast/2</a>

	.. _`cluster_manager:handle_info/2`:

	.. function:: handle_info(Info :: timeout | term(), State :: term()) -> Result
		:noindex:

	* **NewState:** term()
	* **Result:** {noreply, NewState} | {noreply, NewState, Timeout} | {noreply, NewState, hibernate} | {stop, Reason :: term(), NewState}
	* **Timeout:** non_neg_integer() | infinity

	<a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_info-2">gen_server:handle_info/2</a>

	.. _`cluster_manager:init/1`:

	.. function:: init(Args :: term()) -> Result
		:noindex:

	* **Result:** {ok, State} | {ok, State, Timeout} | {ok, State, hibernate} | {stop, Reason :: term()} | ignore
	* **State:** term()
	* **Timeout:** non_neg_integer() | infinity

	<a href="http://www.erlang.org/doc/man/gen_server.html#Module:init-1">gen_server:init/1</a>

	.. _`cluster_manager:monitoring_loop/1`:

	.. function:: monitoring_loop(Flag) -> ok
		:noindex:

	* **Flag:** on | off

	Loop that monitors if nodes are alive.

	.. _`cluster_manager:monitoring_loop/2`:

	.. function:: monitoring_loop(Flag, Nodes) -> ok
		:noindex:

	* **Flag:** on | off
	* **Nodes:** list()

	Beginning of loop that monitors if nodes are alive.

	.. _`cluster_manager:start_link/0`:

	.. function:: start_link() -> Result
		:noindex:

	* **Error:** {already_started, Pid} | term()
	* **Pid:** pid()
	* **Result:** {ok,Pid} | ignore | {error,Error}

	Starts cluster manager

	.. _`cluster_manager:start_link/1`:

	.. function:: start_link(Mode) -> Result
		:noindex:

	* **Error:** {already_started,Pid} | term()
	* **Mode:** test | normal
	* **Pid:** pid()
	* **Result:** {ok,Pid} | ignore | {error,Error}

	Starts cluster manager

	.. _`cluster_manager:start_monitoring_loop/2`:

	.. function:: start_monitoring_loop(Flag, Nodes) -> ok
		:noindex:

	* **Flag:** on | off
	* **Nodes:** list()

	Starts loop that monitors if nodes are alive.

	.. _`cluster_manager:stop/0`:

	.. function:: stop() -> ok
		:noindex:

	Stops the server

	.. _`cluster_manager:terminate/2`:

	.. function:: terminate(Reason, State :: term()) -> Any :: term()
		:noindex:

	* **Reason:** normal | shutdown | {shutdown, term()} | term()

	<a href="http://www.erlang.org/doc/man/gen_server.html#Module:terminate-2">gen_server:terminate/2</a>

