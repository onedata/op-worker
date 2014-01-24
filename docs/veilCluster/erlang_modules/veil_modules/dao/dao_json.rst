.. _dao_json:

dao_json
========

	:Authors: Rafal Slota
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: BigCouch document management module (very simple - BigCouch specific - JSON creator)

Function Index
~~~~~~~~~~~~~~~

	* :ref:`get_field/2 <dao_json;get_field/2>`
	* :ref:`get_fields/1 <dao_json;get_fields/1>`
	* :ref:`mk_bin/1 <dao_json;mk_bin/1>`
	* :ref:`mk_doc/1 <dao_json;mk_doc/1>`
	* :ref:`mk_field/3 <dao_json;mk_field/3>`
	* :ref:`mk_fields/3 <dao_json;mk_fields/3>`
	* :ref:`mk_obj/0 <dao_json;mk_obj/0>`
	* :ref:`mk_str/1 <dao_json;mk_str/1>`
	* :ref:`reverse_fields/1 <dao_json;reverse_fields/1>`
	* :ref:`rm_field/2 <dao_json;rm_field/2>`
	* :ref:`rm_fields/2 <dao_json;rm_fields/2>`

Function Details
~~~~~~~~~~~~~~~~~

	.. erl:module:: dao_json

	.. _`dao_json;get_field/2`:

	.. erl:function:: get_field(DocOrObj, Name :: string()) -> any() | {error, not_found} | {error, invalid_object}

	* **DocOrObj:** #doc{} | json_object()

	Returns field's value from given document or JSON object

	.. _`dao_json;get_fields/1`:

	.. _`dao_json;mk_bin/1`:

	.. erl:function:: mk_bin(Term :: term()) -> binary()

	Converts given term to binary form used by BigCouch/CouchDB

	.. _`dao_json;mk_doc/1`:

	.. erl:function:: mk_doc(Id :: string()) -> #doc{}

	Returns new BigCouch document with given Id

	.. _`dao_json;mk_field/3`:

	.. erl:function:: mk_field(DocOrObj, Name :: string(), Value :: term()) -> DocOrObj

	* **DocOrObj:** #doc{} | json_object()

	Inserts new field into given document or JSON object

	.. _`dao_json;mk_fields/3`:

	.. erl:function:: mk_fields(DocOrObj, [Names :: string()], [Values :: term()]) -> DocOrObj

	* **DocOrObj:** #doc{} | json_object()

	Inserts new fields into given document or JSON object

	.. _`dao_json;mk_obj/0`:

	.. erl:function:: mk_obj() -> {[]}

	Returns empty json object structure used by BigCouch/CouchDB

	.. _`dao_json;mk_str/1`:

	.. erl:function:: mk_str(Str :: string() | atom()) -> binary()

	Converts given string to binary form used by BigCouch/CouchDB

	.. _`dao_json;reverse_fields/1`:

	.. erl:function:: reverse_fields(DocOrObj) -> DocOrObj

	* **DocOrObj:** #doc{} | json_object()

	Reverses fields in given document or JSON object

	.. _`dao_json;rm_field/2`:

	.. erl:function:: rm_field(DocOrObj, Name :: string()) -> DocOrObj

	* **DocOrObj:** #doc{} | json_object()

	Removes field from given document or JSON object

	.. _`dao_json;rm_fields/2`:

	.. erl:function:: rm_fields(DocOrObj, [Name :: string()]) -> DocOrObj

	* **DocOrObj:** #doc{} | json_object()

	Removes fields from given document or JSON object

