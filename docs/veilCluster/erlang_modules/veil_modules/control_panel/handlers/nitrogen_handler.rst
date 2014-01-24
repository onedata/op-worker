.. _nitrogen_handler:

nitrogen_handler
================

	:Authors: Lukasz Opiola
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This is the callback module for cowboy to handle requests by passing them to the nitrogen engine

Function Index
~~~~~~~~~~~~~~~

	* :ref:`handle/2 <nitrogen_handler;handle/2>`
	* :ref:`init/3 <nitrogen_handler;init/3>`
	* :ref:`terminate/3 <nitrogen_handler;terminate/3>`

Function Details
~~~~~~~~~~~~~~~~~

	.. erl:module:: nitrogen_handler

	.. _`nitrogen_handler;handle/2`:

	.. erl:function:: handle(Request, Options) -> Result

	* **Options:** term()
	* **Request:** term()
	* **Response:** term()
	* **Result:** {ok, Response, Options}

	Handles a request producing a response with use of Nitrogen engine or a file stream response

	.. _`nitrogen_handler;init/3`:

	.. erl:function:: init(Protocol, Request :: term(), Options :: term()) -> Result

	* **Protocol:** {Transport :: term(), http}
	* **Result:** {ok, Request :: term(), #state{}}

	Initializes a request-response procedure

	.. _`nitrogen_handler;terminate/3`:

	.. erl:function:: terminate(Reason, Request, State) -> Result

	* **Reason:** term()
	* **Request:** term()
	* **Result:** ok
	* **State:** term()

	Cowboy handler callback

