.. _dns_udp_handler:

dns_udp_handler
===============

	:Authors: Bartosz Polnik
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This module is responsible for handling udp aspects of dns protocol.

Function Index
~~~~~~~~~~~~~~~

	* :ref:`loop/3 <dns_udp_handler;loop/3>`
	* :ref:`start_link/3 <dns_udp_handler;start_link/3>`

Function Details
~~~~~~~~~~~~~~~~~

	.. erl:module:: dns_udp_handler

	.. _`dns_udp_handler;loop/3`:

	.. erl:function:: loop(Socket, ResponseTTL, DispatcherTimeout) -> no_return()

	* **DispatcherTimeout:** non_neg_integer()
	* **ResponseTTL:** non_neg_integer()
	* **Socket:** inet:socket()

	Loop maintaining state.

	.. _`dns_udp_handler;start_link/3`:

	.. erl:function:: start_link(Port, ResponseTTLInSecs, DispatcherTimeout) -> {ok, pid()}

	* **DispatcherTimeout:** non_neg_integer()
	* **Port:** non_neg_integer()
	* **ResponseTTLInSecs:** non_neg_integer()

	Creates process to handle udp socket.

