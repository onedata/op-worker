%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module wraps storage helper's methods using NIF driver
%% @end
%% ===================================================================

-module(veilhelpers_nif).

-include("veil_modules/fslogic/fslogic.hrl").

-export([start/1, getattr/3, access/4, mknod/5, unlink/3, rename/4, chmod/4, chown/5, truncate/4, 
         open/4, read/6, write/6, read/5, write/5, statfs/3, release/4, fsync/5, mkdir/4, rmdir/3]).
%% TODO zaimplementować natsępujące funkcje
-export([is_reg/1, is_dir/1, get_flag/1]).
%% ===================================================================
%% API
%% ===================================================================

%% TODO this 2 functions should be implemented
is_reg(_St_mode) ->
  true.

is_dir(_St_mode) ->
  true.

get_flag(Flag) ->
  case Flag of
    o_rdonly -> 0;
    o_wronly -> 1;
    o_rdwr -> 2
  end.

%% start/1
%% ====================================================================
%% @doc This method loads NIF library into erlang VM. This should be used <br/>
%%      once before using any other method in this module.
%% @end
-spec start(Prefix :: string()) -> ok | {error, Reason :: term()}.
%% ====================================================================
start(Prefix) ->
    erlang:load_nif(filename:join(Prefix, "c_lib/veilhelpers_drv"), 0).


%% getattr/3
%% ====================================================================
%% @doc First 2 arguments of this method should come from #storage_helper_info{} record. <br/>
%%      Those two arguments decide which Storage Helper shall be used for this operation. <br/>
%%      ErrorCode return value shall be 0 if operation was succesfull, otherwise negated POSIX error code will be returned. <br/>
%%      gettattr/3 returns #st_stat{} record for given file _path. Note that if ErrorCode does not equal 0, fields of #st_stat{} are undefined and shall be ignored.
%% @end
-spec getattr(_sh_name :: string(), _sh_args :: [string()], _path :: string()) -> {ErrorCode :: integer(), Stats :: #st_stat{}} | {error, 'NIF_not_loaded'}.
%% ====================================================================
getattr(_sh_name, _sh_args, _path) -> 
    {error, 'NIF_not_loaded'}.


%% access/4
%% ====================================================================
%% @doc First 2 arguments of this method should come from #storage_helper_info{} record. <br/>
%%      Those two arguments decide which Storage Helper shall be used for this operation. <br/>
%%      ErrorCode return value shall be 0 if operation was succesfull, otherwise negated POSIX error code will be returned. <br/>
%%      access/4 checks if the calling process has specified by _mask premissions to file with given _path. Most storage helpers <br/>
%%      will always return 0 (success), therefore this method can be used only to check if calling process does NOT have permissions to the file. 
%% @end
-spec access(_sh_name :: string(), _sh_args :: [string()], _path :: string(), _mask :: integer()) -> ErrorCode :: integer() | {error, 'NIF_not_loaded'}.
%% ====================================================================
access(_sh_name, _sh_args, _path, _mask) -> 
    {error, 'NIF_not_loaded'}.


%% mknod/5
%% ====================================================================
%% @doc First 2 arguments of this method should come from #storage_helper_info{} record. <br/>
%%      Those two arguments decide which Storage Helper shall be used for this operation. <br/>
%%      ErrorCode return value shall be 0 if operation was succesfull, otherwise negated POSIX error code will be returned. <br/>
%%      mknod/5 can and shall be used in order to create file (not directory). _mode and _rdev arguments are the same as in mknod syscall.
%% @end
-spec mknod(_sh_name :: string(), _sh_args :: [string()], _path :: string(), _mode :: integer(), _rdev :: integer()) -> ErrorCode :: integer() | {error, 'NIF_not_loaded'}.
%% ====================================================================
mknod(_sh_name, _sh_args, _path, _mode, _rdev) -> 
    {error, 'NIF_not_loaded'}.

%% unlink/3
%% ====================================================================
%% @doc First 2 arguments of this method should come from #storage_helper_info{} record. <br/>
%%      Those two arguments decide which Storage Helper shall be used for this operation. <br/>
%%      ErrorCode return value shall be 0 if operation was succesfull, otherwise negated POSIX error code will be returned. <br/>
%%      unlink/3 removes given file (not directory).
%% @end
-spec unlink(_sh_name :: string(), _sh_args :: [string()], _path :: string()) -> ErrorCode :: integer() | {error, 'NIF_not_loaded'}.
%% ====================================================================
unlink(_sh_name, _sh_args, _path) -> 
    {error, 'NIF_not_loaded'}.


%% rename/4
%% ====================================================================
%% @doc First 2 arguments of this method should come from #storage_helper_info{} record. <br/>
%%      Those two arguments decide which Storage Helper shall be used for this operation. <br/>
%%      ErrorCode return value shall be 0 if operation was succesfull, otherwise negated POSIX error code will be returned. <br/>
%%      rename/4 shall be used to rename/move file from _from path to _to path.
%% @end
-spec rename(_sh_name :: string(), _sh_args :: [string()], _from :: string(), _to :: string()) -> ErrorCode :: integer() | {error, 'NIF_not_loaded'}.
%% ====================================================================
rename(_sh_name, _sh_args, _from, _to) -> 
    {error, 'NIF_not_loaded'}.

%% chmod/4
%% ====================================================================
%% @doc First 2 arguments of this method should come from #storage_helper_info{} record. <br/>
%%      Those two arguments decide which Storage Helper shall be used for this operation. <br/>
%%      ErrorCode return value shall be 0 if operation was succesfull, otherwise negated POSIX error code will be returned. <br/>
%%      chmod/4 changes file's _mode.
%% @end
-spec chmod(_sh_name :: string(), _sh_args :: [string()], _path :: string(), _mode :: integer()) -> ErrorCode :: integer() | {error, 'NIF_not_loaded'}.
%% ====================================================================
chmod(_sh_name, _sh_args, _path, _mode) -> 
    {error, 'NIF_not_loaded'}.


%% chown/5
%% ====================================================================
%% @doc First 2 arguments of this method should come from #storage_helper_info{} record. <br/>
%%      Those two arguments decide which Storage Helper shall be used for this operation. <br/>
%%      ErrorCode return value shall be 0 if operation was succesfull, otherwise negated POSIX error code will be returned. <br/>
%%      chown/5 changes file's uid and gid
%% @end
-spec chown(_sh_name :: string(), _sh_args :: [string()], _path :: string(), _uid :: integer(), _gid :: integer()) -> ErrorCode :: integer() | {error, 'NIF_not_loaded'}.
%% ====================================================================
chown(_sh_name, _sh_args, _path, _uid, _gid) -> 
    {error, 'NIF_not_loaded'}.


%% truncate/4
%% ====================================================================
%% @doc First 2 arguments of this method should come from #storage_helper_info{} record. <br/>
%%      Those two arguments decide which Storage Helper shall be used for this operation. <br/>
%%      ErrorCode return value shall be 0 if operation was succesfull, otherwise negated POSIX error code will be returned. <br/>
%%      truncate/4 changes file size to _size.
%% @end
-spec truncate(_sh_name :: string(), _sh_args :: [string()], _path :: string(), _size :: integer()) -> ErrorCode :: integer() | {error, 'NIF_not_loaded'}.
%% ====================================================================
truncate(_sh_name, _sh_args, _path, _size) -> 
    {error, 'NIF_not_loaded'}.


%% open/4
%% ====================================================================
%% @doc First 2 arguments of this method should come from #storage_helper_info{} record. <br/>
%%      Those two arguments decide which Storage Helper shall be used for this operation. <br/>
%%      ErrorCode return value shall be 0 if operation was succesfull, otherwise negated POSIX error code will be returned. <br/>
%%      open/4 opens file. _fi aregument is an #st_fuse_file_info record that shall contain open flags. Same record will be returnd <br/>
%%      with 'fd' field set (file descriptor), therefore record returned by 'open' shall be passed to next read/write/release calls. 
%% @end
-spec open(_sh_name :: string(), _sh_args :: [string()], _path :: string(), _fi :: #st_fuse_file_info{}) -> {ErrorCode :: integer(), FFI :: #st_fuse_file_info{}} | {error, 'NIF_not_loaded'}.
%% ====================================================================
open(_sh_name, _sh_args, _path, _fi) -> 
    {error, 'NIF_not_loaded'}.


%% read/6
%% ====================================================================
%% @doc First 2 arguments of this method should come from #storage_helper_info{} record. <br/>
%%      Those two arguments decide which Storage Helper shall be used for this operation. <br/>
%%      ErrorCode return value equals to bytes read count if operation was succesfull, otherwise negated POSIX error code will be returned. <br/>
%%      read/6 reads _size bytes (starting with _offset) from given file. If the _fi arguemnt is given with valid file descriptor ('fd' field) <br/>
%%      the 'fd' will be used to access file. Otherwise read/6 will open file for you.
%% @end
-spec read(_sh_name :: string(), _sh_args :: [string()], _path :: string(), _size :: integer(), _offset :: integer(), _fi :: #st_fuse_file_info{}) -> 
    {ErrorCode :: integer(), Data  :: binary()} | {error, 'NIF_not_loaded'}.
%% ====================================================================
read(_sh_name, _sh_args, _path, _size, _offset, _fi) -> 
    {error, 'NIF_not_loaded'}.
read(_sh_name, _sh_args, _path, _size, _offset) ->
    read(_sh_name, _sh_args, _path, _size, _offset, #st_fuse_file_info{}).

%% write/6
%% ====================================================================
%% @doc First 2 arguments of this method should come from #storage_helper_info{} record. <br/>
%%      Those two arguments decide which Storage Helper shall be used for this operation. <br/>
%%      ErrorCode return value equals to bytes writen count if operation was succesfull, otherwise negated POSIX error code will be returned. <br/>
%%      write/6 writes _buf binary data to given file starting with _offset. _fi argument has the same meaning as in read/6. 
%% @end
-spec write(_sh_name :: string(), _sh_args :: [string()], _path :: string(), _buf :: binary(), _offset :: integer(), _fi :: #st_fuse_file_info{}) -> 
    ErrorCode :: integer() | {error, 'NIF_not_loaded'}.
%% ====================================================================
write(_sh_name, _sh_args, _path, _buf, _offset, _fi) -> 
    {error, 'NIF_not_loaded'}.
write(_sh_name, _sh_args, _path, _buf, _offset) ->
    write(_sh_name, _sh_args, _path, _buf, _offset, #st_fuse_file_info{}).


%% statfs/3
%% ====================================================================
%% @doc First 2 arguments of this method should come from #storage_helper_info{} record. <br/>
%%      Those two arguments decide which Storage Helper shall be used for this operation. <br/>
%%      ErrorCode return value shall be 0 if operation was succesfull, otherwise negated POSIX error code will be returned. <br/>
%%      statfs/3 returns #st_statvfs record for given _path. See statfs syscall for more details.
%% @end
-spec statfs(_sh_name :: string(), _sh_args :: [string()], _path :: string()) -> {ErrorCode :: integer(), #st_statvfs{}} | {error, 'NIF_not_loaded'}.
%% ====================================================================
statfs(_sh_name, _sh_args, _path) -> 
    {error, 'NIF_not_loaded'}.


%% release/4
%% ====================================================================
%% @doc First 2 arguments of this method should come from #storage_helper_info{} record. <br/>
%%      Those two arguments decide which Storage Helper shall be used for this operation. <br/>
%%      ErrorCode return value shall be 0 if operation was succesfull, otherwise negated POSIX error code will be returned. <br/>
%%      release/4 closes file that was previously opened with open/4. 
%% @end
-spec release(_sh_name :: string(), _sh_args :: [string()], _path :: string(), _fi :: #st_fuse_file_info{}) -> ErrorCode :: integer() | {error, 'NIF_not_loaded'}.
%% ====================================================================
release(_sh_name, _sh_args, _path, _fi) -> 
    {error, 'NIF_not_loaded'}.


%% fsync/5
%% ====================================================================
%% @doc First 2 arguments of this method should come from #storage_helper_info{} record. <br/>
%%      Those two arguments decide which Storage Helper shall be used for this operation. <br/>
%%      ErrorCode return value shall be 0 if operation was succesfull, otherwise negated POSIX error code will be returned. <br/>
%%      
%% @end
-spec fsync(_sh_name :: string(), _sh_args :: [string()], _path :: string(), _isdatasync :: integer(), _fi :: #st_fuse_file_info{}) -> ErrorCode :: integer() | {error, 'NIF_not_loaded'}.
%% ====================================================================
fsync(_sh_name, _sh_args, _path, _isdatasync, _fi) -> 
    {error, 'NIF_not_loaded'}.


%% mkdir/4
%% ====================================================================
%% @doc First 2 arguments of this method should come from #storage_helper_info{} record. <br/>
%%      Those two arguments decide which Storage Helper shall be used for this operation. <br/>
%%      ErrorCode return value shall be 0 if operation was succesfull, otherwise negated POSIX error code will be returned. <br/>
%%      mkdir creates directory with given _path and _mode (permissions).
%% @end
-spec mkdir(_sh_name :: string(), _sh_args :: [string()], _path :: string(), _mode :: integer()) -> ErrorCode :: integer() | {error, 'NIF_not_loaded'}.
%% ====================================================================
mkdir(_sh_name, _sh_args, _path, _mode) -> 
    {error, 'NIF_not_loaded'}.


%% rmdir/3
%% ====================================================================
%% @doc First 2 arguments of this method should come from #storage_helper_info{} record. <br/>
%%      Those two arguments decide which Storage Helper shall be used for this operation. <br/>
%%      ErrorCode return value shall be 0 if operation was succesfull, otherwise negated POSIX error code will be returned. <br/>
%%      rmdir removes directory with given _path.
%% @end
-spec rmdir(_sh_name :: string(), _sh_args :: [string()], _path :: string()) -> ErrorCode :: integer() | {error, 'NIF_not_loaded'}.
%% ====================================================================
rmdir(_sh_name, _sh_args, _path) -> 
    {error, 'NIF_not_loaded'}.

