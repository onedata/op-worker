.. _fslogic_utils:

fslogic_utils
=============

	:Authors: Rafal Slota
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This module exports utility tools for fslogic

Function Index
~~~~~~~~~~~~~~~

	* :ref:`basename/1 <fslogic_utils:basename/1>`
	* :ref:`create_children_list/1 <fslogic_utils:create_children_list/1>`
	* :ref:`create_children_list/2 <fslogic_utils:create_children_list/2>`
	* :ref:`get_parent_and_name_from_path/2 <fslogic_utils:get_parent_and_name_from_path/2>`
	* :ref:`strip_path_leaf/1 <fslogic_utils:strip_path_leaf/1>`
	* :ref:`time/0 <fslogic_utils:time/0>`
	* :ref:`update_meta_attr/3 <fslogic_utils:update_meta_attr/3>`

Function Details
~~~~~~~~~~~~~~~~~

	.. _`fslogic_utils:basename/1`:

	.. function:: basename(Path :: string()) -> string()
		:noindex:

	Gives file basename from given path

	.. _`fslogic_utils:create_children_list/1`:

	.. function:: create_children_list(Files :: list()) -> Result
		:noindex:

	* **Result:** term()

	Creates list of children logical names on the basis of list with veil_documents that describe children.

	.. _`fslogic_utils:create_children_list/2`:

	.. function:: create_children_list(Files :: list(), TmpAns :: list()) -> Result
		:noindex:

	* **Result:** term()

	Creates list of children logical names on the basis of list with veil_documents that describe children.

	.. _`fslogic_utils:get_parent_and_name_from_path/2`:

	.. function:: get_parent_and_name_from_path(Path :: string(), ProtocolVersion :: term()) -> Result
		:noindex:

	* **Result:** tuple()

	Gets parent uuid and file name on the basis of absolute path.

	.. _`fslogic_utils:strip_path_leaf/1`:

	.. function:: strip_path_leaf(Path :: string()) -> string()
		:noindex:

	Strips file name from path

	.. _`fslogic_utils:time/0`:

	.. function:: time() -> Result :: integer()
		:noindex:

	Returns time in seconds.

	.. _`fslogic_utils:update_meta_attr/3`:

	.. function:: update_meta_attr(File :: #file{}, Attr, Value :: term()) -> Result :: #file{}
		:noindex:

	* **Attr:** atime | mtime | ctime | size | times

	Updates file_meta record associated with given #file record. <br/> Attr agument decides which field has to be updated with Value. <br/> There is one exception to this rule: if Attr == 'times', Value has to be tuple <br/> with fallowing format: {ATimeValue, MTimeValue, CTimeValue} or {ATimeValue, MTimeValue}. <br/> If there is no #file_meta record associated with given #file, #file_meta will be created and whole function call will be blocking. <br/> Otherwise the method call will be asynchronous. <br/> Returns given as argument #file record unchanged, unless #file_meta had to be created. <br/> In this case returned #file record will have #file.meta_doc field updated and shall be saved to DB after this call.

