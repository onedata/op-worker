.. _rest_utils:

rest_utils
==========

	:Authors: Lukasz Opiola
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This module provides convinience functions designed for REST handling modules.

Function Index
~~~~~~~~~~~~~~~

	* :ref:`decode_from_json/1 <rest_utils:decode_from_json/1>`
	* :ref:`encode_to_json/1 <rest_utils:encode_to_json/1>`
	* :ref:`map/2 <rest_utils:map/2>`
	* :ref:`unmap/3 <rest_utils:unmap/3>`

Function Details
~~~~~~~~~~~~~~~~~

	.. _`rest_utils:decode_from_json/1`:

	.. function:: decode_from_json(term()) -> binary()
		:noindex:

	Convinience function that convert JSON binary to an erlang term.

	.. _`rest_utils:encode_to_json/1`:

	.. function:: encode_to_json(term()) -> binary()
		:noindex:

	Convinience function that convert an erlang term to JSON, producing binary result. Possible terms, can be nested: {struct, Props} - Props is a structure as a proplist, e.g.: [{id, 13}, {message, "mess"}] {Props} - alias for above {array, Array} - Array is a list, e.g.: [13, "mess"]

	.. _`rest_utils:map/2`:

	.. function:: map(record(), [atom()]) -> [tuple()]
		:noindex:

	Converts a record to JSON conversion-ready tuple list. For this input: RecordToMap = #some_record{id=123, message="mess"} Fields = [id, message] The function will produce: [{id, 123},{message, "mess"}] NOTE: rest_utils.hrl contains convienience macro that will include this function in code with Fields listed for given record name

	.. _`rest_utils:unmap/3`:

	.. function:: unmap([tuple()], record(), [atom()]) -> [tuple()]
		:noindex:

	Converts a tuple list resulting from JSON to erlang translation into an erlang record. Reverse process to map/2. For this input: Proplist = [{id, 123},{message, "mess"}] RecordTuple = #some_record{} Fields = [id, message] The function will produce: #some_record{id=123, message="mess"} NOTE: rest_utils.hrl contains convienience macro that will include this function in code with Fields listed for given record name

