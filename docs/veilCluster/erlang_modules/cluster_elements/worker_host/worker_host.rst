.. _worker_host:

worker_host
===========

	:Authors: Michal Wrzeszcz
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This module hosts all VeilFS modules (fslogic, cluster_rengin etc.). It makes it easy to manage modules and provides some basic functionality for its plug-ins (VeilFS modules) e.g. requests management.
	:Behaviours: `gen_server <http://www.erlang.org/doc/man/gen_server.html>`_

Function Index
~~~~~~~~~~~~~~~

	* :ref:`code_change/3 <worker_host;code_change/3>`
	* :ref:`generate_sub_proc_list/1 <worker_host;generate_sub_proc_list/1>`
	* :ref:`generate_sub_proc_list/5 <worker_host;generate_sub_proc_list/5>`
	* :ref:`handle_call/3 <worker_host;handle_call/3>`
	* :ref:`handle_cast/2 <worker_host;handle_cast/2>`
	* :ref:`handle_info/2 <worker_host;handle_info/2>`
	* :ref:`init/1 <worker_host;init/1>`
	* :ref:`start_link/3 <worker_host;start_link/3>`
	* :ref:`start_sub_proc/5 <worker_host;start_sub_proc/5>`
	* :ref:`stop/1 <worker_host;stop/1>`
	* :ref:`stop_all_sub_proc/1 <worker_host;stop_all_sub_proc/1>`
	* :ref:`terminate/2 <worker_host;terminate/2>`

Function Details
~~~~~~~~~~~~~~~~~

	.. erl:module:: worker_host

	.. _`worker_host;code_change/3`:

	.. erl:function:: code_change(OldVsn, State :: term(), Extra :: term()) -> Result

	* **OldVsn:** Vsn | {down, Vsn}
	* **Result:** {ok, NewState :: term()} | {error, Reason :: term()}
	* **Vsn:** term()

	 gen_server:code_change/3 

	.. _`worker_host;generate_sub_proc_list/1`:

	.. _`worker_host;generate_sub_proc_list/5`:

	.. erl:function:: generate_sub_proc_list(Name :: atom(), MaxDepth :: integer(), MaxWidth :: integer(), ProcFun :: term(), MapFun :: term()) -> Result

	* **Result:** list()

	Generates the list that describes sub procs.

	.. _`worker_host;handle_call/3`:

	.. erl:function:: handle_call(Request :: term(), From :: {pid(), Tag :: term()}, State :: term()) -> Result

	* **NewState:** term()
	* **Reason:** term()
	* **Reply:** term()
	* **Result:** {reply, Reply, NewState} | {reply, Reply, NewState, Timeout} | {reply, Reply, NewState, hibernate} | {noreply, NewState} | {noreply, NewState, Timeout} | {noreply, NewState, hibernate} | {stop, Reason, Reply, NewState} | {stop, Reason, NewState}
	* **Timeout:** non_neg_integer() | infinity

	 gen_server:handle_call/3 

	.. _`worker_host;handle_cast/2`:

	.. erl:function:: handle_cast(Request :: term(), State :: term()) -> Result

	* **NewState:** term()
	* **Result:** {noreply, NewState} | {noreply, NewState, Timeout} | {noreply, NewState, hibernate} | {stop, Reason :: term(), NewState}
	* **Timeout:** non_neg_integer() | infinity

	 gen_server:handle_cast/2 

	.. _`worker_host;handle_info/2`:

	.. erl:function:: handle_info(Info :: timeout | term(), State :: term()) -> Result

	* **NewState:** term()
	* **Result:** {noreply, NewState} | {noreply, NewState, Timeout} | {noreply, NewState, hibernate} | {stop, Reason :: term(), NewState}
	* **Timeout:** non_neg_integer() | infinity

	 gen_server:handle_info/2 

	.. _`worker_host;init/1`:

	.. erl:function:: init(Args :: term()) -> Result

	* **Result:** {ok, State} | {ok, State, Timeout} | {ok, State, hibernate} | {stop, Reason :: term()} | ignore
	* **State:** term()
	* **Timeout:** non_neg_integer() | infinity

	 gen_server:init/1 

	.. _`worker_host;start_link/3`:

	.. erl:function:: start_link(PlugIn, PlugInArgs, LoadMemorySize) -> Result

	* **Error:** {already_started,Pid} | term()
	* **LoadMemorySize:** integer()
	* **Pid:** pid()
	* **PlugIn:** atom()
	* **PlugInArgs:** any()
	* **Result:** {ok,Pid} | ignore | {error,Error}

	Starts host with apropriate plug-in

	.. _`worker_host;start_sub_proc/5`:

	.. erl:function:: start_sub_proc(Name :: atom(), MaxDepth :: integer(), MaxWidth :: integer(), ProcFun :: term(), MapFun :: term()) -> Result

	* **Result:** pid()

	Starts sub proc

	.. _`worker_host;stop/1`:

	.. erl:function:: stop(PlugIn) -> ok

	* **PlugIn:** atom()

	Stops the server

	.. _`worker_host;stop_all_sub_proc/1`:

	.. erl:function:: stop_all_sub_proc(SubProcs :: list()) -> ok

	Stops all sub procs

	.. _`worker_host;terminate/2`:

	.. erl:function:: terminate(Reason, State :: term()) -> Any :: term()

	* **Reason:** normal | shutdown | {shutdown, term()} | term()

	 gen_server:terminate/2 

