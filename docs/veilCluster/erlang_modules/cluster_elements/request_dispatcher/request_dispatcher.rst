.. _request_dispatcher:

request_dispatcher
==================

	:Authors: Michal Wrzeszcz
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This module forwards client's requests to appropriate worker_hosts.
	:Behaviours: `gen_server <http://www.erlang.org/doc/man/gen_server.html>`_

Function Index
~~~~~~~~~~~~~~~

	* :ref:`code_change/3 <request_dispatcher:code_change/3>`
	* :ref:`handle_call/3 <request_dispatcher:handle_call/3>`
	* :ref:`handle_cast/2 <request_dispatcher:handle_cast/2>`
	* :ref:`handle_info/2 <request_dispatcher:handle_info/2>`
	* :ref:`init/1 <request_dispatcher:init/1>`
	* :ref:`send_to_fuse/3 <request_dispatcher:send_to_fuse/3>`
	* :ref:`start_link/0 <request_dispatcher:start_link/0>`
	* :ref:`stop/0 <request_dispatcher:stop/0>`
	* :ref:`terminate/2 <request_dispatcher:terminate/2>`

Function Details
~~~~~~~~~~~~~~~~~

	.. _`request_dispatcher:code_change/3`:

	.. function:: code_change(OldVsn, State :: term(), Extra :: term()) -> Result
		:noindex:

	* **OldVsn:** Vsn | {down, Vsn}
	* **Result:** {ok, NewState :: term()} | {error, Reason :: term()}
	* **Vsn:** term()

	 gen_server:code_change/3 

	.. _`request_dispatcher:handle_call/3`:

	.. function:: handle_call(Request :: term(), From :: {pid(), Tag :: term()}, State :: term()) -> Result
		:noindex:

	* **NewState:** term()
	* **Reason:** term()
	* **Reply:** term()
	* **Result:** {reply, Reply, NewState} | {reply, Reply, NewState, Timeout} | {reply, Reply, NewState, hibernate} | {noreply, NewState} | {noreply, NewState, Timeout} | {noreply, NewState, hibernate} | {stop, Reason, Reply, NewState} | {stop, Reason, NewState}
	* **Timeout:** non_neg_integer() | infinity

	 gen_server:handle_call/3 

	.. _`request_dispatcher:handle_cast/2`:

	.. function:: handle_cast(Request :: term(), State :: term()) -> Result
		:noindex:

	* **NewState:** term()
	* **Result:** {noreply, NewState} | {noreply, NewState, Timeout} | {noreply, NewState, hibernate} | {stop, Reason :: term(), NewState}
	* **Timeout:** non_neg_integer() | infinity

	 gen_server:handle_cast/2 

	.. _`request_dispatcher:handle_info/2`:

	.. function:: handle_info(Info :: timeout | term(), State :: term()) -> Result
		:noindex:

	* **NewState:** term()
	* **Result:** {noreply, NewState} | {noreply, NewState, Timeout} | {noreply, NewState, hibernate} | {stop, Reason :: term(), NewState}
	* **Timeout:** non_neg_integer() | infinity

	 gen_server:handle_info/2 

	.. _`request_dispatcher:init/1`:

	.. function:: init(Args :: term()) -> Result
		:noindex:

	* **Result:** {ok, State} | {ok, State, Timeout} | {ok, State, hibernate} | {stop, Reason :: term()} | ignore
	* **State:** term()
	* **Timeout:** non_neg_integer() | infinity

	 gen_server:init/1 

	.. _`request_dispatcher:send_to_fuse/3`:

	.. function:: send_to_fuse(FuseId :: string(), Message :: term(), MessageDecoder :: string()) -> Result
		:noindex:

	* **Result:** callback_node_not_found | node_manager_error | dispatcher_error | ok | term()

	Sends message to fuse

	.. _`request_dispatcher:start_link/0`:

	.. function:: start_link() -> Result
		:noindex:

	* **Error:** {already_started,Pid} | term()
	* **Pid:** pid()
	* **Result:** {ok,Pid} | ignore | {error,Error}

	Starts the server

	.. _`request_dispatcher:stop/0`:

	.. function:: stop() -> ok
		:noindex:

	Stops the server

	.. _`request_dispatcher:terminate/2`:

	.. function:: terminate(Reason, State :: term()) -> Any :: term()
		:noindex:

	* **Reason:** normal | shutdown | {shutdown, term()} | term()

	 gen_server:terminate/2 

