.. _node_manager:

node_manager
============

	:Authors: Michal Wrzeszcz
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This module is a gen_server that coordinates the life cycle of node. It starts/stops appropriate services (according to node type) and communicates with ccm (if node works as worker). Node can be ccm or worker. However, worker_hosts can be also started at ccm nodes.
	:Behaviours: `gen_server <http://www.erlang.org/doc/man/gen_server.html>`_

Function Index
~~~~~~~~~~~~~~~

	* :ref:`addCallback/3 <node_manager:addCallback/3>`
	* :ref:`check_vsn/0 <node_manager:check_vsn/0>`
	* :ref:`code_change/3 <node_manager:code_change/3>`
	* :ref:`delete_callback/3 <node_manager:delete_callback/3>`
	* :ref:`get_callback/2 <node_manager:get_callback/2>`
	* :ref:`handle_call/3 <node_manager:handle_call/3>`
	* :ref:`handle_cast/2 <node_manager:handle_cast/2>`
	* :ref:`handle_info/2 <node_manager:handle_info/2>`
	* :ref:`init/1 <node_manager:init/1>`
	* :ref:`start_link/1 <node_manager:start_link/1>`
	* :ref:`stop/0 <node_manager:stop/0>`
	* :ref:`terminate/2 <node_manager:terminate/2>`

Function Details
~~~~~~~~~~~~~~~~~

	.. _`node_manager:addCallback/3`:

	.. function:: addCallback(State :: term(), FuseId :: string(), Pid :: pid()) -> NewState
		:noindex:

	* **NewState:** list()

	Adds callback to fuse.

	.. _`node_manager:check_vsn/0`:

	.. function:: check_vsn() -> Result
		:noindex:

	* **Result:** term()

	Checks application version

	.. _`node_manager:code_change/3`:

	.. function:: code_change(OldVsn, State :: term(), Extra :: term()) -> Result
		:noindex:

	* **OldVsn:** Vsn | {down, Vsn}
	* **Result:** {ok, NewState :: term()} | {error, Reason :: term()}
	* **Vsn:** term()

	<a href="http://www.erlang.org/doc/man/gen_server.html#Module:code_change-3">gen_server:code_change/3</a>

	.. _`node_manager:delete_callback/3`:

	.. function:: delete_callback(State :: term(), FuseId :: string(), Pid :: pid()) -> Result
		:noindex:

	* **NewState:** term()
	* **Result:** {NewState, fuse_not_found | fuse_deleted | pid_not_found | pid_deleted}

	Deletes callback

	.. _`node_manager:get_callback/2`:

	.. function:: get_callback(State :: term(), FuseId :: string()) -> Result
		:noindex:

	* **Result:** non | pid()

	Gets callback to fuse (if there are more than one callback it chooses one).

	.. _`node_manager:handle_call/3`:

	.. function:: handle_call(Request :: term(), From :: {pid(), Tag :: term()}, State :: term()) -> Result
		:noindex:

	* **NewState:** term()
	* **Reason:** term()
	* **Reply:** term()
	* **Result:** {reply, Reply, NewState} | {reply, Reply, NewState, Timeout} | {reply, Reply, NewState, hibernate} | {noreply, NewState} | {noreply, NewState, Timeout} | {noreply, NewState, hibernate} | {stop, Reason, Reply, NewState} | {stop, Reason, NewState}
	* **Timeout:** non_neg_integer() | infinity

	<a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_call-3">gen_server:handle_call/3</a>

	.. _`node_manager:handle_cast/2`:

	.. function:: handle_cast(Request :: term(), State :: term()) -> Result
		:noindex:

	* **NewState:** term()
	* **Result:** {noreply, NewState} | {noreply, NewState, Timeout} | {noreply, NewState, hibernate} | {stop, Reason :: term(), NewState}
	* **Timeout:** non_neg_integer() | infinity

	<a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_cast-2">gen_server:handle_cast/2</a>

	.. _`node_manager:handle_info/2`:

	.. function:: handle_info(Info :: timeout | term(), State :: term()) -> Result
		:noindex:

	* **NewState:** term()
	* **Result:** {noreply, NewState} | {noreply, NewState, Timeout} | {noreply, NewState, hibernate} | {stop, Reason :: term(), NewState}
	* **Timeout:** non_neg_integer() | infinity

	<a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_info-2">gen_server:handle_info/2</a>

	.. _`node_manager:init/1`:

	.. function:: init(Args :: term()) -> Result
		:noindex:

	* **Result:** {ok, State} | {ok, State, Timeout} | {ok, State, hibernate} | {stop, Reason :: term()} | ignore
	* **State:** term()
	* **Timeout:** non_neg_integer() | infinity

	<a href="http://www.erlang.org/doc/man/gen_server.html#Module:init-1">gen_server:init/1</a>

	.. _`node_manager:start_link/1`:

	.. function:: start_link(Type) -> Result
		:noindex:

	* **Error:** {already_started,Pid} | term()
	* **Pid:** pid()
	* **Result:** {ok,Pid} | ignore | {error,Error}
	* **Type:** test_worker | worker | ccm

	Starts the server

	.. _`node_manager:stop/0`:

	.. function:: stop() -> ok
		:noindex:

	Stops the server

	.. _`node_manager:terminate/2`:

	.. function:: terminate(Reason, State :: term()) -> Any :: term()
		:noindex:

	* **Reason:** normal | shutdown | {shutdown, term()} | term()

	<a href="http://www.erlang.org/doc/man/gen_server.html#Module:terminate-2">gen_server:terminate/2</a>

