.. _veilhelpers_nif:

veilhelpers_nif
===============

	:Authors: Rafal Slota
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This module wraps storage helper's methods using NIF driver

Function Index
~~~~~~~~~~~~~~~

	* :ref:`access/4 <veilhelpers_nif;access/4>`
	* :ref:`chmod/4 <veilhelpers_nif;chmod/4>`
	* :ref:`chown/5 <veilhelpers_nif;chown/5>`
	* :ref:`chown_name/5 <veilhelpers_nif;chown_name/5>`
	* :ref:`fsync/5 <veilhelpers_nif;fsync/5>`
	* :ref:`get_flag/3 <veilhelpers_nif;get_flag/3>`
	* :ref:`getattr/3 <veilhelpers_nif;getattr/3>`
	* :ref:`is_dir/3 <veilhelpers_nif;is_dir/3>`
	* :ref:`is_reg/3 <veilhelpers_nif;is_reg/3>`
	* :ref:`mkdir/4 <veilhelpers_nif;mkdir/4>`
	* :ref:`mknod/5 <veilhelpers_nif;mknod/5>`
	* :ref:`open/4 <veilhelpers_nif;open/4>`
	* :ref:`read/5 <veilhelpers_nif;read/5>`
	* :ref:`read/6 <veilhelpers_nif;read/6>`
	* :ref:`release/4 <veilhelpers_nif;release/4>`
	* :ref:`rename/4 <veilhelpers_nif;rename/4>`
	* :ref:`rmdir/3 <veilhelpers_nif;rmdir/3>`
	* :ref:`start/1 <veilhelpers_nif;start/1>`
	* :ref:`statfs/3 <veilhelpers_nif;statfs/3>`
	* :ref:`truncate/4 <veilhelpers_nif;truncate/4>`
	* :ref:`unlink/3 <veilhelpers_nif;unlink/3>`
	* :ref:`write/5 <veilhelpers_nif;write/5>`
	* :ref:`write/6 <veilhelpers_nif;write/6>`

Function Details
~~~~~~~~~~~~~~~~~

	.. erl:module:: veilhelpers_nif

	.. _`veilhelpers_nif;access/4`:

	.. erl:function:: access(_sh_name :: string(), _sh_args :: [string()], _path :: string(), _mask :: integer()) -> ErrorCode :: integer() | {error, 'NIF_not_loaded'}

	First 2 arguments of this method should come from #storage_helper_info{} record. Those two arguments decide which Storage Helper shall be used for this operation. ErrorCode return value shall be 0 if operation was succesfull, otherwise negated POSIX error code will be returned. access/4 checks if the calling process has specified by _mask premissions to file with given _path. Most storage helpers will always return 0 (success), therefore this method can be used only to check if calling process does NOT have permissions to the file.

	.. _`veilhelpers_nif;chmod/4`:

	.. erl:function:: chmod(_sh_name :: string(), _sh_args :: [string()], _path :: string(), _mode :: integer()) -> ErrorCode :: integer() | {error, 'NIF_not_loaded'}

	First 2 arguments of this method should come from #storage_helper_info{} record. Those two arguments decide which Storage Helper shall be used for this operation. ErrorCode return value shall be 0 if operation was succesfull, otherwise negated POSIX error code will be returned. chmod/4 changes file's _mode.

	.. _`veilhelpers_nif;chown/5`:

	.. erl:function:: chown(_sh_name :: string(), _sh_args :: [string()], _path :: string(), _uid :: integer(), _gid :: integer()) -> ErrorCode :: integer() | {error, 'NIF_not_loaded'}

	First 2 arguments of this method should come from #storage_helper_info{} record. Those two arguments decide which Storage Helper shall be used for this operation. ErrorCode return value shall be 0 if operation was succesfull, otherwise negated POSIX error code will be returned. chown/5 changes file's uid and gid

	.. _`veilhelpers_nif;chown_name/5`:

	.. erl:function:: chown_name(_sh_name :: string(), _sh_args :: [string()], _path :: string(), _uname :: string(), _gname :: string()) -> ErrorCode :: integer() | {error, 'NIF_not_loaded'}

	First 2 arguments of this method should come from #storage_helper_info{} record. Those two arguments decide which Storage Helper shall be used for this operation. ErrorCode return value shall be 0 if operation was succesfull, otherwise negated POSIX error code will be returned. chown/5 changes file's uid and gid

	.. _`veilhelpers_nif;fsync/5`:

	.. erl:function:: fsync(_sh_name :: string(), _sh_args :: [string()], _path :: string(), _isdatasync :: integer(), _fi :: #st_fuse_file_info{}) -> ErrorCode :: integer() | {error, 'NIF_not_loaded'}

	First 2 arguments of this method should come from #storage_helper_info{} record. Those two arguments decide which Storage Helper shall be used for this operation. ErrorCode return value shall be 0 if operation was succesfull, otherwise negated POSIX error code will be returned. 

	.. _`veilhelpers_nif;get_flag/3`:

	.. _`veilhelpers_nif;getattr/3`:

	.. erl:function:: getattr(_sh_name :: string(), _sh_args :: [string()], _path :: string()) -> {ErrorCode :: integer(), Stats :: #st_stat{}} | {error, 'NIF_not_loaded'}

	First 2 arguments of this method should come from #storage_helper_info{} record. Those two arguments decide which Storage Helper shall be used for this operation. ErrorCode return value shall be 0 if operation was succesfull, otherwise negated POSIX error code will be returned. gettattr/3 returns #st_stat{} record for given file _path. Note that if ErrorCode does not equal 0, fields of #st_stat{} are undefined and shall be ignored.

	.. _`veilhelpers_nif;is_dir/3`:

	.. _`veilhelpers_nif;is_reg/3`:

	.. _`veilhelpers_nif;mkdir/4`:

	.. erl:function:: mkdir(_sh_name :: string(), _sh_args :: [string()], _path :: string(), _mode :: integer()) -> ErrorCode :: integer() | {error, 'NIF_not_loaded'}

	First 2 arguments of this method should come from #storage_helper_info{} record. Those two arguments decide which Storage Helper shall be used for this operation. ErrorCode return value shall be 0 if operation was succesfull, otherwise negated POSIX error code will be returned. mkdir creates directory with given _path and _mode (permissions).

	.. _`veilhelpers_nif;mknod/5`:

	.. erl:function:: mknod(_sh_name :: string(), _sh_args :: [string()], _path :: string(), _mode :: integer(), _rdev :: integer()) -> ErrorCode :: integer() | {error, 'NIF_not_loaded'}

	First 2 arguments of this method should come from #storage_helper_info{} record. Those two arguments decide which Storage Helper shall be used for this operation. ErrorCode return value shall be 0 if operation was succesfull, otherwise negated POSIX error code will be returned. mknod/5 can and shall be used in order to create file (not directory). _mode and _rdev arguments are the same as in mknod syscall.

	.. _`veilhelpers_nif;open/4`:

	.. erl:function:: open(_sh_name :: string(), _sh_args :: [string()], _path :: string(), _fi :: #st_fuse_file_info{}) -> {ErrorCode :: integer(), FFI :: #st_fuse_file_info{}} | {error, 'NIF_not_loaded'}

	First 2 arguments of this method should come from #storage_helper_info{} record. Those two arguments decide which Storage Helper shall be used for this operation. ErrorCode return value shall be 0 if operation was succesfull, otherwise negated POSIX error code will be returned. open/4 opens file. _fi aregument is an #st_fuse_file_info record that shall contain open flags. Same record will be returnd with 'fd' field set (file descriptor), therefore record returned by 'open' shall be passed to next read/write/release calls.

	.. _`veilhelpers_nif;read/5`:

	.. _`veilhelpers_nif;read/6`:

	.. erl:function:: read(_sh_name :: string(), _sh_args :: [string()], _path :: string(), _size :: integer(), _offset :: integer(), _fi :: #st_fuse_file_info{}) -> {ErrorCode :: integer(), Data :: binary()} | {error, 'NIF_not_loaded'}

	First 2 arguments of this method should come from #storage_helper_info{} record. Those two arguments decide which Storage Helper shall be used for this operation. ErrorCode return value equals to bytes read count if operation was succesfull, otherwise negated POSIX error code will be returned. read/6 reads _size bytes (starting with _offset) from given file. If the _fi arguemnt is given with valid file descriptor ('fd' field) the 'fd' will be used to access file. Otherwise read/6 will open file for you.

	.. _`veilhelpers_nif;release/4`:

	.. erl:function:: release(_sh_name :: string(), _sh_args :: [string()], _path :: string(), _fi :: #st_fuse_file_info{}) -> ErrorCode :: integer() | {error, 'NIF_not_loaded'}

	First 2 arguments of this method should come from #storage_helper_info{} record. Those two arguments decide which Storage Helper shall be used for this operation. ErrorCode return value shall be 0 if operation was succesfull, otherwise negated POSIX error code will be returned. release/4 closes file that was previously opened with open/4.

	.. _`veilhelpers_nif;rename/4`:

	.. erl:function:: rename(_sh_name :: string(), _sh_args :: [string()], _from :: string(), _to :: string()) -> ErrorCode :: integer() | {error, 'NIF_not_loaded'}

	First 2 arguments of this method should come from #storage_helper_info{} record. Those two arguments decide which Storage Helper shall be used for this operation. ErrorCode return value shall be 0 if operation was succesfull, otherwise negated POSIX error code will be returned. rename/4 shall be used to rename/move file from _from path to _to path.

	.. _`veilhelpers_nif;rmdir/3`:

	.. erl:function:: rmdir(_sh_name :: string(), _sh_args :: [string()], _path :: string()) -> ErrorCode :: integer() | {error, 'NIF_not_loaded'}

	First 2 arguments of this method should come from #storage_helper_info{} record. Those two arguments decide which Storage Helper shall be used for this operation. ErrorCode return value shall be 0 if operation was succesfull, otherwise negated POSIX error code will be returned. rmdir removes directory with given _path.

	.. _`veilhelpers_nif;start/1`:

	.. erl:function:: start(Prefix :: string()) -> ok | {error, Reason :: term()}

	This method loads NIF library into erlang VM. This should be used once before using any other method in this module.

	.. _`veilhelpers_nif;statfs/3`:

	.. erl:function:: statfs(_sh_name :: string(), _sh_args :: [string()], _path :: string()) -> {ErrorCode :: integer(), #st_statvfs{}} | {error, 'NIF_not_loaded'}

	First 2 arguments of this method should come from #storage_helper_info{} record. Those two arguments decide which Storage Helper shall be used for this operation. ErrorCode return value shall be 0 if operation was succesfull, otherwise negated POSIX error code will be returned. statfs/3 returns #st_statvfs record for given _path. See statfs syscall for more details.

	.. _`veilhelpers_nif;truncate/4`:

	.. erl:function:: truncate(_sh_name :: string(), _sh_args :: [string()], _path :: string(), _size :: integer()) -> ErrorCode :: integer() | {error, 'NIF_not_loaded'}

	First 2 arguments of this method should come from #storage_helper_info{} record. Those two arguments decide which Storage Helper shall be used for this operation. ErrorCode return value shall be 0 if operation was succesfull, otherwise negated POSIX error code will be returned. truncate/4 changes file size to _size.

	.. _`veilhelpers_nif;unlink/3`:

	.. erl:function:: unlink(_sh_name :: string(), _sh_args :: [string()], _path :: string()) -> ErrorCode :: integer() | {error, 'NIF_not_loaded'}

	First 2 arguments of this method should come from #storage_helper_info{} record. Those two arguments decide which Storage Helper shall be used for this operation. ErrorCode return value shall be 0 if operation was succesfull, otherwise negated POSIX error code will be returned. unlink/3 removes given file (not directory).

	.. _`veilhelpers_nif;write/5`:

	.. _`veilhelpers_nif;write/6`:

	.. erl:function:: write(_sh_name :: string(), _sh_args :: [string()], _path :: string(), _buf :: binary(), _offset :: integer(), _fi :: #st_fuse_file_info{}) -> ErrorCode :: integer() | {error, 'NIF_not_loaded'}

	First 2 arguments of this method should come from #storage_helper_info{} record. Those two arguments decide which Storage Helper shall be used for this operation. ErrorCode return value equals to bytes writen count if operation was succesfull, otherwise negated POSIX error code will be returned. write/6 writes _buf binary data to given file starting with _offset. _fi argument has the same meaning as in read/6.

