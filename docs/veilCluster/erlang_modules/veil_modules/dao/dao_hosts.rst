.. _dao_hosts:

dao_hosts
=========

	:Authors: Rafal Slota
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This module manages hosts and connections to VeilFS DB

Function Index
~~~~~~~~~~~~~~~

	* :ref:`ban/1 <dao_hosts;ban/1>`
	* :ref:`ban/2 <dao_hosts;ban/2>`
	* :ref:`call/2 <dao_hosts;call/2>`
	* :ref:`call/3 <dao_hosts;call/3>`
	* :ref:`delete/1 <dao_hosts;delete/1>`
	* :ref:`insert/1 <dao_hosts;insert/1>`
	* :ref:`reactivate/1 <dao_hosts;reactivate/1>`
	* :ref:`store_exec/2 <dao_hosts;store_exec/2>`

Function Details
~~~~~~~~~~~~~~~~~

	.. erl:module:: dao_hosts

	.. _`dao_hosts;ban/1`:

	.. erl:function:: ban(Host :: atom()) -> ok | {error, timeout}

	Bans db host name (lowers its priority while selecting host in get_host/0) Ban will be cleaned after DEFAULT_BAN_TIME ms or while store refresh

	.. _`dao_hosts;ban/2`:

	.. erl:function:: ban(Host :: atom(), BanTime :: integer()) -> ok | {error, no_host} | {error, timeout}

	Bans db host name (lowers its priority while selecting host in get_host/0) Ban will be cleaned after BanTime ms or while store refresh If given Host is already banned, nothing happens and will return 'ok' If given Host wasn't inserted, returns {error, no_host}

	.. _`dao_hosts;call/2`:

	.. erl:function:: call(Method :: atom(), Args :: [term()]) -> term() | {error, rpc_retry_limit_exceeded}

	Calls fabric:Method with Args on random db host. Random host will be assigned to the calling process and will be used with subsequent calls

	.. _`dao_hosts;call/3`:

	.. erl:function:: call(Module :: atom(), Method :: atom(), Args :: [Arg :: term()]) -> term() | Error

	* **Error:** {error, rpc_retry_limit_exceeded} | {error, term()}

	Same as call/2, but with custom Module

	.. _`dao_hosts;delete/1`:

	.. erl:function:: delete(Host :: atom()) -> ok | {error, timeout}

	Deletes db host name from store (host pool)

	.. _`dao_hosts;insert/1`:

	.. erl:function:: insert(Host :: atom()) -> ok | {error, timeout}

	Inserts db host name into store (host pool)

	.. _`dao_hosts;reactivate/1`:

	.. _`dao_hosts;store_exec/2`:

	.. erl:function:: store_exec(sequential, Msg :: term()) -> ok | {error, Error :: term()}

	Executes Msg. Caller must ensure that this method won't be used concurrently. Currently this method is used as part of internal module implementation, although it has to be exported because it's called by gen_server (which ensures it's sequential call).

