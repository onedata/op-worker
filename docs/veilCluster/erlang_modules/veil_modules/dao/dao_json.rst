.. _dao_json:

dao_json
========

	:Authors: Rafal Slota
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: BigCouch document management module (very simple - BigCouch specific - JSON creator)

Function Index
~~~~~~~~~~~~~~~

	* :ref:`get_field/2 <dao_json:get_field/2>`
	* :ref:`get_fields/1 <dao_json:get_fields/1>`
	* :ref:`mk_bin/1 <dao_json:mk_bin/1>`
	* :ref:`mk_doc/1 <dao_json:mk_doc/1>`
	* :ref:`mk_field/3 <dao_json:mk_field/3>`
	* :ref:`mk_fields/3 <dao_json:mk_fields/3>`
	* :ref:`mk_obj/0 <dao_json:mk_obj/0>`
	* :ref:`mk_str/1 <dao_json:mk_str/1>`
	* :ref:`reverse_fields/1 <dao_json:reverse_fields/1>`
	* :ref:`rm_field/2 <dao_json:rm_field/2>`
	* :ref:`rm_fields/2 <dao_json:rm_fields/2>`

Function Details
~~~~~~~~~~~~~~~~~

	.. _`dao_json:get_field/2`:

	.. function:: get_field(DocOrObj, Name :: string()) -> any() | {error, not_found} | {error, invalid_object}
		:noindex:

	* **DocOrObj:** #doc{} | json_object()

	Returns field's value from given document or JSON object

	.. _`dao_json:get_fields/1`:

	.. _`dao_json:mk_bin/1`:

	.. function:: mk_bin(Term :: term()) -> binary()
		:noindex:

	Converts given term to binary form used by BigCouch/CouchDB

	.. _`dao_json:mk_doc/1`:

	.. function:: mk_doc(Id :: string()) -> #doc{}
		:noindex:

	Returns new BigCouch document with given Id

	.. _`dao_json:mk_field/3`:

	.. function:: mk_field(DocOrObj, Name :: string(), Value :: term()) -> DocOrObj
		:noindex:

	* **DocOrObj:** #doc{} | json_object()

	Inserts new field into given document or JSON object

	.. _`dao_json:mk_fields/3`:

	.. function:: mk_fields(DocOrObj, [Names :: string()], [Values :: term()]) -> DocOrObj
		:noindex:

	* **DocOrObj:** #doc{} | json_object()

	Inserts new fields into given document or JSON object

	.. _`dao_json:mk_obj/0`:

	.. function:: mk_obj() -> {[]}
		:noindex:

	Returns empty json object structure used by BigCouch/CouchDB

	.. _`dao_json:mk_str/1`:

	.. function:: mk_str(Str :: string() | atom()) -> binary()
		:noindex:

	Converts given string to binary form used by BigCouch/CouchDB

	.. _`dao_json:reverse_fields/1`:

	.. function:: reverse_fields(DocOrObj) -> DocOrObj
		:noindex:

	* **DocOrObj:** #doc{} | json_object()

	Reverses fields in given document or JSON object

	.. _`dao_json:rm_field/2`:

	.. function:: rm_field(DocOrObj, Name :: string()) -> DocOrObj
		:noindex:

	* **DocOrObj:** #doc{} | json_object()

	Removes field from given document or JSON object

	.. _`dao_json:rm_fields/2`:

	.. function:: rm_fields(DocOrObj, [Name :: string()]) -> DocOrObj
		:noindex:

	* **DocOrObj:** #doc{} | json_object()

	Removes fields from given document or JSON object

