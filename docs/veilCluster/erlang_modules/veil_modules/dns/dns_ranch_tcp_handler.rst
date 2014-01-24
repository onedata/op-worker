.. _dns_ranch_tcp_handler:

dns_ranch_tcp_handler
=====================

	:Authors: Bartosz Polnik
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This module is responsible for handling tcp aspects of dns protocol.
	:Behaviours: `ranch_protocol <https://github.com/extend/ranch/blob/master/manual/ranch_protocol.md>`_

Function Index
~~~~~~~~~~~~~~~

	* :ref:`loop/5 <dns_ranch_tcp_handler;loop/5>`
	* :ref:`start_link/4 <dns_ranch_tcp_handler;start_link/4>`

Function Details
~~~~~~~~~~~~~~~~~

	.. erl:module:: dns_ranch_tcp_handler

	.. _`dns_ranch_tcp_handler;loop/5`:

	.. erl:function:: loop(Socket, Transport, ResponseTTL, TCPIdleTime, DispatcherTimeout) -> ok

	* **DispatcherTimeout:** non_neg_integer()
	* **ResponseTTL:** non_neg_integer()
	* **Socket:** inet:socket()
	* **TCPIdleTime:** non_neg_integer()
	* **Transport:** term()

	Main handler loop.

	.. _`dns_ranch_tcp_handler;start_link/4`:

	.. erl:function:: start_link(Ref :: term(), Socket :: term(), Transport :: term(), Opts :: term()) -> Result

	* **Pid:** pid()
	* **Result:** {ok, Pid}

	Starts handler.

