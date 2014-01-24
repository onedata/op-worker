.. _cluster_manager:

cluster_manager
===============

	:Authors: Michal Wrzeszcz
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This module coordinates central cluster.
	:Behaviours: `gen_server <http://www.erlang.org/doc/man/gen_server.html>`_

Function Index
~~~~~~~~~~~~~~~

	* :ref:`calculate_load/2 <cluster_manager;calculate_load/2>`
	* :ref:`calculate_node_load/2 <cluster_manager;calculate_node_load/2>`
	* :ref:`calculate_worker_load/1 <cluster_manager;calculate_worker_load/1>`
	* :ref:`code_change/3 <cluster_manager;code_change/3>`
	* :ref:`handle_call/3 <cluster_manager;handle_call/3>`
	* :ref:`handle_cast/2 <cluster_manager;handle_cast/2>`
	* :ref:`handle_info/2 <cluster_manager;handle_info/2>`
	* :ref:`init/1 <cluster_manager;init/1>`
	* :ref:`monitoring_loop/1 <cluster_manager;monitoring_loop/1>`
	* :ref:`monitoring_loop/2 <cluster_manager;monitoring_loop/2>`
	* :ref:`start_link/0 <cluster_manager;start_link/0>`
	* :ref:`start_link/1 <cluster_manager;start_link/1>`
	* :ref:`start_monitoring_loop/2 <cluster_manager;start_monitoring_loop/2>`
	* :ref:`stop/0 <cluster_manager;stop/0>`
	* :ref:`terminate/2 <cluster_manager;terminate/2>`
	* :ref:`update_dispatcher_state/6 <cluster_manager;update_dispatcher_state/6>`
	* :ref:`update_dns_state/3 <cluster_manager;update_dns_state/3>`

Function Details
~~~~~~~~~~~~~~~~~

	.. erl:module:: cluster_manager

	.. _`cluster_manager;calculate_load/2`:

	.. erl:function:: calculate_load(NodesLoad :: list(), WorkersLoad :: list()) -> Result

	* **Result:** list()

	Merges nodes' and workers' loads to more useful form

	.. _`cluster_manager;calculate_node_load/2`:

	.. erl:function:: calculate_node_load(Nodes :: list(), Period :: atom()) -> Result

	* **Result:** list()

	Calculates load of all nodes in cluster

	.. _`cluster_manager;calculate_worker_load/1`:

	.. erl:function:: calculate_worker_load(Workers :: list()) -> Result

	* **Result:** list()

	Calculates load of all workers in cluster

	.. _`cluster_manager;code_change/3`:

	.. erl:function:: code_change(OldVsn, State :: term(), Extra :: term()) -> Result

	* **OldVsn:** Vsn | {down, Vsn}
	* **Result:** {ok, NewState :: term()} | {error, Reason :: term()}
	* **Vsn:** term()

	 gen_server:code_change/3 

	.. _`cluster_manager;handle_call/3`:

	.. erl:function:: handle_call(Request :: term(), From :: {pid(), Tag :: term()}, State :: term()) -> Result

	* **NewState:** term()
	* **Reason:** term()
	* **Reply:** term()
	* **Result:** {reply, Reply, NewState} | {reply, Reply, NewState, Timeout} | {reply, Reply, NewState, hibernate} | {noreply, NewState} | {noreply, NewState, Timeout} | {noreply, NewState, hibernate} | {stop, Reason, Reply, NewState} | {stop, Reason, NewState}
	* **Timeout:** non_neg_integer() | infinity

	 gen_server:handle_call/3 

	.. _`cluster_manager;handle_cast/2`:

	.. erl:function:: handle_cast(Request :: term(), State :: term()) -> Result

	* **NewState:** term()
	* **Result:** {noreply, NewState} | {noreply, NewState, Timeout} | {noreply, NewState, hibernate} | {stop, Reason :: term(), NewState}
	* **Timeout:** non_neg_integer() | infinity

	 gen_server:handle_cast/2 

	.. _`cluster_manager;handle_info/2`:

	.. erl:function:: handle_info(Info :: timeout | term(), State :: term()) -> Result

	* **NewState:** term()
	* **Result:** {noreply, NewState} | {noreply, NewState, Timeout} | {noreply, NewState, hibernate} | {stop, Reason :: term(), NewState}
	* **Timeout:** non_neg_integer() | infinity

	 gen_server:handle_info/2 

	.. _`cluster_manager;init/1`:

	.. erl:function:: init(Args :: term()) -> Result

	* **Result:** {ok, State} | {ok, State, Timeout} | {ok, State, hibernate} | {stop, Reason :: term()} | ignore
	* **State:** term()
	* **Timeout:** non_neg_integer() | infinity

	 gen_server:init/1 

	.. _`cluster_manager;monitoring_loop/1`:

	.. erl:function:: monitoring_loop(Flag) -> ok

	* **Flag:** on | off

	Loop that monitors if nodes are alive.

	.. _`cluster_manager;monitoring_loop/2`:

	.. erl:function:: monitoring_loop(Flag, Nodes) -> ok

	* **Flag:** on | off
	* **Nodes:** list()

	Beginning of loop that monitors if nodes are alive.

	.. _`cluster_manager;start_link/0`:

	.. erl:function:: start_link() -> Result

	* **Error:** {already_started, Pid} | term()
	* **Pid:** pid()
	* **Result:** {ok,Pid} | ignore | {error,Error}

	Starts cluster manager

	.. _`cluster_manager;start_link/1`:

	.. erl:function:: start_link(Mode) -> Result

	* **Error:** {already_started,Pid} | term()
	* **Mode:** test | normal
	* **Pid:** pid()
	* **Result:** {ok,Pid} | ignore | {error,Error}

	Starts cluster manager

	.. _`cluster_manager;start_monitoring_loop/2`:

	.. erl:function:: start_monitoring_loop(Flag, Nodes) -> ok

	* **Flag:** on | off
	* **Nodes:** list()

	Starts loop that monitors if nodes are alive.

	.. _`cluster_manager;stop/0`:

	.. erl:function:: stop() -> ok

	Stops the server

	.. _`cluster_manager;terminate/2`:

	.. erl:function:: terminate(Reason, State :: term()) -> Any :: term()

	* **Reason:** normal | shutdown | {shutdown, term()} | term()

	 gen_server:terminate/2 

	.. _`cluster_manager;update_dispatcher_state/6`:

	.. erl:function:: update_dispatcher_state(WorkersList, DispatcherMaps, Nodes, NewStateNum, Loads, AvgLoad) -> ok

	* **AvgLoad:** integer()
	* **DispatcherMaps:** list()
	* **Loads:** list()
	* **NewStateNum:** integer()
	* **Nodes:** list()
	* **WorkersList:** list()

	Updates dispatchers' states.

	.. _`cluster_manager;update_dns_state/3`:

