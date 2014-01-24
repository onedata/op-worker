.. _records_translator:

records_translator
==================

	:Authors: Michal Wrzeszcz
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This module is able to do additional translation of record decoded using protocol buffer e.g. it can change record "atom" to Erlang atom type.

Function Index
~~~~~~~~~~~~~~~

	* :ref:`translate/2 <records_translator:translate/2>`
	* :ref:`translate_to_record/1 <records_translator:translate_to_record/1>`

Function Details
~~~~~~~~~~~~~~~~~

	.. _`records_translator:translate/2`:

	.. function:: translate(Record :: tuple(), DecoderName :: string()) -> Result
		:noindex:

	* **Result:** term()

	Translates record to simpler terms if possible.

	.. _`records_translator:translate_to_record/1`:

	.. function:: translate_to_record(Value :: term()) -> Result
		:noindex:

	* **Result:** tuple() | term()

	Translates term to record if possible.

