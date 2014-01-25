.. _dns_utils:

dns_utils
=========

	:Authors: Bartosz Polnik
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This module is responsible for creating dns response for given dns request. Currently only supported are queries of type a and class in.

Function Index
~~~~~~~~~~~~~~~

	* :ref:`generate_answer/5 <dns_utils:generate_answer/5>`

Function Details
~~~~~~~~~~~~~~~~~

	.. _`dns_utils:generate_answer/5`:

	.. function:: generate_answer(Packet, Dispatcher, DispatcherTimeout, ResponseTTL, Protocol) -> Result
		:noindex:

	* **Dispatcher:** term()
	* **DispatcherTimeout:** non_neg_integer()
	* **Packet:** binary()
	* **Protocol:** udp | tcp
	* **ResponseTTL:** non_neg_integer()
	* **Result:** {ok, binary()} | {error, term()}

	Generates binary dns response for given binary dns request, non protocol agnostic.

