.. _ws_handler:

ws_handler
==========

	:Authors: Michal Wrzeszcz
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This module forwards requests from socket to dispatcher.

Function Index
~~~~~~~~~~~~~~~

	* :ref:`checkMessage/2 <ws_handler;checkMessage/2>`
	* :ref:`decode_protocol_buffer/2 <ws_handler;decode_protocol_buffer/2>`
	* :ref:`encode_answer/2 <ws_handler;encode_answer/2>`
	* :ref:`encode_answer/5 <ws_handler;encode_answer/5>`
	* :ref:`init/3 <ws_handler;init/3>`
	* :ref:`websocket_handle/3 <ws_handler;websocket_handle/3>`
	* :ref:`websocket_info/3 <ws_handler;websocket_info/3>`
	* :ref:`websocket_init/3 <ws_handler;websocket_init/3>`
	* :ref:`websocket_terminate/3 <ws_handler;websocket_terminate/3>`

Function Details
~~~~~~~~~~~~~~~~~

	.. erl:module:: ws_handler

	.. _`ws_handler;checkMessage/2`:

	.. erl:function:: checkMessage(Msg :: term(), DN :: string()) -> Result

	* **Result:** boolean()

	Checks if message can be processed by cluster.

	.. _`ws_handler;decode_protocol_buffer/2`:

	.. erl:function:: decode_protocol_buffer(MsgBytes :: binary(), DN :: string()) -> Result

	* **Answer_type:** string()
	* **ModuleName:** atom()
	* **Msg:** term()
	* **MsgId:** integer()
	* **Result:** {Synch, ModuleName, Msg, MsgId, Answer_type}
	* **Synch:** boolean()

	Decodes the message using protocol buffers records_translator.

	.. _`ws_handler;encode_answer/2`:

	.. erl:function:: encode_answer(Main_Answer :: atom(), MsgId :: integer()) -> Result

	* **Result:** binary()

	Encodes answer using protocol buffers records_translator.

	.. _`ws_handler;encode_answer/5`:

	.. erl:function:: encode_answer(Main_Answer :: atom(), MsgId :: integer(), AnswerType :: string(), Answer_decoder_name :: string(), Worker_Answer :: term()) -> Result

	* **Result:** binary()

	Encodes answer using protocol buffers records_translator.

	.. _`ws_handler;init/3`:

	.. erl:function:: init(Proto :: term(), Req :: term(), Opts :: term()) -> {upgrade, protocol, cowboy_websocket}

	Switches protocol to WebSocket

	.. _`ws_handler;websocket_handle/3`:

	.. erl:function:: websocket_handle({Type :: atom(), Data :: term()}, Req, State) -> {reply, {Type :: atom(), Data :: term()}, Req, State} | {ok, Req, State} | {shutdown, Req, State}

	* **Req:** term()
	* **State:** #hander_state{}

	Cowboy's webscoket_handle callback. Binary data was received on socket. For more information please refer Cowboy's user manual.

	.. _`ws_handler;websocket_info/3`:

	.. erl:function:: websocket_info(Msg :: term(), Req, State) -> {reply, {Type :: atom(), Data :: term()}, Req, State} | {ok, Req, State} | {shutdown, Req, State}

	* **Req:** term()
	* **State:** #hander_state{}

	Cowboy's webscoket_info callback. Erlang message received. For more information please refer Cowboy's user manual.

	.. _`ws_handler;websocket_init/3`:

	.. erl:function:: websocket_init(TransportName :: atom(), Req :: term(), Opts :: list()) -> {ok, Req :: term(), State :: term()} | {shutdown, Req :: term()}

	Cowboy's webscoket_init callback. Initialize connection, proceed with TLS-GSI authentication. If GSI validation fails, connection will be closed. Currently validation is handled by Globus NIF library loaded on erlang slave nodes.

	.. _`ws_handler;websocket_terminate/3`:

	.. erl:function:: websocket_terminate(Reason :: term(), Req, State) -> ok

	* **Req:** term()
	* **State:** #hander_state{}

	Cowboy's webscoket_info callback. Connection was closed. For more information please refer Cowboy's user manual.

