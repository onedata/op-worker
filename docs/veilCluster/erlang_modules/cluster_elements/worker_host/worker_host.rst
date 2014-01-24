.. _worker_host:

worker_host
===========

	:Authors: Michal Wrzeszcz
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This module hosts all VeilFS modules (fslogic, cluster_rengin etc.). It makes it easy to manage modules and provides some basic functionality for its plug-ins (VeilFS modules) e.g. requests management.
	:Behaviours: `gen_server <http://www.erlang.org/doc/man/gen_server.html>`_

Function Index
~~~~~~~~~~~~~~~

	* :ref:`code_change/3 <worker_host:code_change/3>`
	* :ref:`generate_sub_proc_list/1 <worker_host:generate_sub_proc_list/1>`
	* :ref:`generate_sub_proc_list/5 <worker_host:generate_sub_proc_list/5>`
	* :ref:`handle_call/3 <worker_host:handle_call/3>`
	* :ref:`handle_cast/2 <worker_host:handle_cast/2>`
	* :ref:`handle_info/2 <worker_host:handle_info/2>`
	* :ref:`init/1 <worker_host:init/1>`
	* :ref:`start_link/3 <worker_host:start_link/3>`
	* :ref:`start_sub_proc/5 <worker_host:start_sub_proc/5>`
	* :ref:`stop/1 <worker_host:stop/1>`
	* :ref:`stop_all_sub_proc/1 <worker_host:stop_all_sub_proc/1>`
	* :ref:`terminate/2 <worker_host:terminate/2>`

Function Details
~~~~~~~~~~~~~~~~~

	.. _`worker_host:code_change/3`:

	.. function:: code_change(OldVsn, State :: term(), Extra :: term()) -> Result
		:noindex:

	* **OldVsn:** Vsn | {down, Vsn}
	* **Result:** {ok, NewState :: term()} | {error, Reason :: term()}
	* **Vsn:** term()

	<a href="http://www.erlang.org/doc/man/gen_server.html#Module:code_change-3">gen_server:code_change/3</a>

	.. _`worker_host:generate_sub_proc_list/1`:

	.. _`worker_host:generate_sub_proc_list/5`:

	.. function:: generate_sub_proc_list(Name :: atom(), MaxDepth :: integer(), MaxWidth :: integer(), ProcFun :: term(), MapFun :: term()) -> Result
		:noindex:

	* **Result:** list()

	Generates the list that describes sub procs.

	.. _`worker_host:handle_call/3`:

	.. function:: handle_call(Request :: term(), From :: {pid(), Tag :: term()}, State :: term()) -> Result
		:noindex:

	* **NewState:** term()
	* **Reason:** term()
	* **Reply:** term()
	* **Result:** {reply, Reply, NewState} | {reply, Reply, NewState, Timeout} | {reply, Reply, NewState, hibernate} | {noreply, NewState} | {noreply, NewState, Timeout} | {noreply, NewState, hibernate} | {stop, Reason, Reply, NewState} | {stop, Reason, NewState}
	* **Timeout:** non_neg_integer() | infinity

	<a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_call-3">gen_server:handle_call/3</a>

	.. _`worker_host:handle_cast/2`:

	.. function:: handle_cast(Request :: term(), State :: term()) -> Result
		:noindex:

	* **NewState:** term()
	* **Result:** {noreply, NewState} | {noreply, NewState, Timeout} | {noreply, NewState, hibernate} | {stop, Reason :: term(), NewState}
	* **Timeout:** non_neg_integer() | infinity

	<a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_cast-2">gen_server:handle_cast/2</a>

	.. _`worker_host:handle_info/2`:

	.. function:: handle_info(Info :: timeout | term(), State :: term()) -> Result
		:noindex:

	* **NewState:** term()
	* **Result:** {noreply, NewState} | {noreply, NewState, Timeout} | {noreply, NewState, hibernate} | {stop, Reason :: term(), NewState}
	* **Timeout:** non_neg_integer() | infinity

	<a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_info-2">gen_server:handle_info/2</a>

	.. _`worker_host:init/1`:

	.. function:: init(Args :: term()) -> Result
		:noindex:

	* **Result:** {ok, State} | {ok, State, Timeout} | {ok, State, hibernate} | {stop, Reason :: term()} | ignore
	* **State:** term()
	* **Timeout:** non_neg_integer() | infinity

	<a href="http://www.erlang.org/doc/man/gen_server.html#Module:init-1">gen_server:init/1</a>

	.. _`worker_host:start_link/3`:

	.. function:: start_link(PlugIn, PlugInArgs, LoadMemorySize) -> Result
		:noindex:

	* **Error:** {already_started,Pid} | term()
	* **LoadMemorySize:** integer()
	* **Pid:** pid()
	* **PlugIn:** atom()
	* **PlugInArgs:** any()
	* **Result:** {ok,Pid} | ignore | {error,Error}

	Starts host with apropriate plug-in

	.. _`worker_host:start_sub_proc/5`:

	.. function:: start_sub_proc(Name :: atom(), MaxDepth :: integer(), MaxWidth :: integer(), ProcFun :: term(), MapFun :: term()) -> Result
		:noindex:

	* **Result:** pid()

	Starts sub proc

	.. _`worker_host:stop/1`:

	.. function:: stop(PlugIn) -> ok
		:noindex:

	* **PlugIn:** atom()

	Stops the server

	.. _`worker_host:stop_all_sub_proc/1`:

	.. function:: stop_all_sub_proc(SubProcs :: list()) -> ok
		:noindex:

	Stops all sub procs

	.. _`worker_host:terminate/2`:

	.. function:: terminate(Reason, State :: term()) -> Any :: term()
		:noindex:

	* **Reason:** normal | shutdown | {shutdown, term()} | term()

	<a href="http://www.erlang.org/doc/man/gen_server.html#Module:terminate-2">gen_server:terminate/2</a>

