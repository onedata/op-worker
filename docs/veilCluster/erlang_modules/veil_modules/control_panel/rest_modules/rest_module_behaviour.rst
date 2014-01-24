.. _rest_module_behaviour:

rest_module_behaviour
=====================

	:Authors: Lukasz Opiola
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This behaviour should be implemented by modules handling REST requests. It ensures the presence of required callbacks.

Function Index
~~~~~~~~~~~~~~~

	* :ref:`behaviour_info/1 <rest_module_behaviour;behaviour_info/1>`

Function Details
~~~~~~~~~~~~~~~~~

	.. erl:module:: rest_module_behaviour

	.. _`rest_module_behaviour;behaviour_info/1`:

	.. erl:function:: behaviour_info(Arg) -> Result

	* **Arg:** callbacks | Other
	* **Fun_def:** tuple()
	* **Other:** any()
	* **Result:** [Fun_def] | undefined

	Defines the behaviour (lists the callbacks and their arity)

