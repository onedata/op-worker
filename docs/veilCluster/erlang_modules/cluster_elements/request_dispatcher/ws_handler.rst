.. _ws_handler:

ws_handler
==========

	:Authors: Michal Wrzeszcz
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This module forwards requests from socket to dispatcher.

Function Index
~~~~~~~~~~~~~~~

	* :ref:`init/3 <ws_handler:init/3>`
	* :ref:`websocket_handle/3 <ws_handler:websocket_handle/3>`
	* :ref:`websocket_info/3 <ws_handler:websocket_info/3>`
	* :ref:`websocket_init/3 <ws_handler:websocket_init/3>`
	* :ref:`websocket_terminate/3 <ws_handler:websocket_terminate/3>`

Function Details
~~~~~~~~~~~~~~~~~

	.. _`ws_handler:init/3`:

	.. function:: init(Proto :: term(), Req :: term(), Opts :: term()) -> {upgrade, protocol, cowboy_websocket}
		:noindex:

	Switches protocol to WebSocket

	.. _`ws_handler:websocket_handle/3`:

	.. function:: websocket_handle({Type :: atom(), Data :: term()}, Req, State) -> {reply, {Type :: atom(), Data :: term()}, Req, State} | {ok, Req, State} | {shutdown, Req, State}
		:noindex:

	* **Req:** term()
	* **State:** #hander_state{}

	Cowboy's webscoket_handle callback. Binary data was received on socket. <br/> For more information please refer Cowboy's user manual.

	.. _`ws_handler:websocket_info/3`:

	.. function:: websocket_info(Msg :: term(), Req, State) -> {reply, {Type :: atom(), Data :: term()}, Req, State} | {ok, Req, State} | {shutdown, Req, State}
		:noindex:

	* **Req:** term()
	* **State:** #hander_state{}

	Cowboy's webscoket_info callback. Erlang message received. <br/> For more information please refer Cowboy's user manual.

	.. _`ws_handler:websocket_init/3`:

	.. function:: websocket_init(TransportName :: atom(), Req :: term(), Opts :: list()) -> {ok, Req :: term(), State :: term()} | {shutdown, Req :: term()}
		:noindex:

	Cowboy's webscoket_init callback. Initialize connection, proceed with TLS-GSI authentication. <br/> If GSI validation fails, connection will be closed. <br/> Currently validation is handled by Globus NIF library loaded on erlang slave nodes.

	.. _`ws_handler:websocket_terminate/3`:

	.. function:: websocket_terminate(Reason :: term(), Req, State) -> ok
		:noindex:

	* **Req:** term()
	* **State:** #hander_state{}

	Cowboy's webscoket_info callback. Connection was closed. <br/> For more information please refer Cowboy's user manual.

