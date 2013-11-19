%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module provides high level file system operations that
%% operates directly on storage.
%% @end
%% ===================================================================

-module(storage_files_manager).

-include("registered_names.hrl").
-include("communication_protocol_pb.hrl").
-include("fuse_messages_pb.hrl").
-include("veil_modules/fslogic/fslogic.hrl").
-include("veil_modules/dao/dao_vfs.hrl").
-include("veil_modules/dao/dao_share.hrl").
-include("cluster_elements/request_dispatcher/gsi_handler.hrl").
-include_lib("veil_modules/dao/dao_types.hrl").

-define(NewFileStorageMode, 8#644).
-define(S_IFREG, 8#100000).

%% ====================================================================
%% API
%% ====================================================================
%% Physical files organization management (to better organize files on storage;
%% the user does not see results of these operations)
-export([mkdir/0, mv/0, delete_dir/0]).
%% Physical files access (used to create temporary copies for remote files)
-export([read/4, write/4, write/3, create/2, truncate/3, delete/2, ls/0]).

%% ====================================================================
%% API functions
%% ====================================================================

%% ====================================================================
%% Physical files organization management (to better organize files on storage;
%% the user does not see results of these operations)
%% ====================================================================

%% mkdir/0
%% ====================================================================
%% @doc Creates dir on storage
%% @end
-spec mkdir() -> {error, not_implemented_yet}.
%% ====================================================================
mkdir() ->
  {error, not_implemented_yet}.

%% mv/0
%% ====================================================================
%% @doc Moves file on storage
%% @end
-spec mv() -> {error, not_implemented_yet}.
%% ====================================================================
mv() ->
  {error, not_implemented_yet}.

%% delete_dir/0
%% ====================================================================
%% @doc Deletes dir on storage
%% @end
-spec delete_dir() -> {error, not_implemented_yet}.
%% ====================================================================
delete_dir() ->
  {error, not_implemented_yet}.

%% ====================================================================
%% Physical files access (used to create temporary copies for remote files)
%% ====================================================================

%% read/4
%% ====================================================================
%% @doc Reads file (operates only on storage). First it checks file
%% attributes (file type and file size). If everything is ok,
%% it reads data from file.
%% @end
-spec read(Storage_helper_info :: record(), File :: string(), Offset :: integer(), Size :: integer()) -> Result when
  Result :: {ok, Bytes} | {ErrorGeneral, ErrorDetail},
  Bytes :: binary(),
  ErrorGeneral :: atom(),
  ErrorDetail :: term().
%% ====================================================================
read(Storage_helper_info, File, Offset, Size) ->
  {ErrorCode, Stat} = veilhelpers:exec(getattr, Storage_helper_info, [File]),
  case ErrorCode of
    0 ->
      case veilhelpers:exec(is_reg, [Stat#st_stat.st_mode]) of
        true ->
          FSize = Stat#st_stat.st_size,
          case FSize < Offset of
            false ->
              Flag = veilhelpers:exec(get_flag, [o_rdonly]),
              {ErrorCode2, FFI} = veilhelpers:exec(open, Storage_helper_info, [File, #st_fuse_file_info{flags = Flag}]),
              case ErrorCode2 of
                0 ->
                  Size2 = case Offset + Size > FSize of
                            true -> FSize - Offset;
                            false -> Size
                          end,
                  {ReadAns, Bytes} = read_bytes(Storage_helper_info, File, Offset, Size2, FFI),

                  ErrorCode3 = veilhelpers:exec(release, Storage_helper_info, [File, FFI]),
                  case ErrorCode3 of
                    0 -> {ReadAns, Bytes};
                    {error, 'NIF_not_loaded'} -> ErrorCode3;
                    _ -> {wrong_release_return_code, ErrorCode3}
                  end;
                error -> {ErrorCode, FFI};
                _ -> {wrong_open_return_code, ErrorCode2}
              end;
            true  -> {error, file_too_small}
          end;
        false -> {error, not_regular_file}
      end;
    error -> {ErrorCode, Stat};
    _ -> {wrong_getatt_return_code, ErrorCode}
  end.

%% write/4
%% ====================================================================
%% @doc Writes data to file (operates only on storage). First it checks file
%% attributes (file type and file size). If everything is ok,
%% it reads data from file.
%% @end
-spec write(Storage_helper_info :: record(), File :: string(), Offset :: integer(), Buf :: binary()) -> Result when
  Result :: BytesWritten | {ErrorGeneral, ErrorDetail},
  BytesWritten :: integer(),
  ErrorGeneral :: atom(),
  ErrorDetail :: term().
%% ====================================================================
write(Storage_helper_info, File, Offset, Buf) ->
  {ErrorCode, Stat} = veilhelpers:exec(getattr, Storage_helper_info, [File]),
  case ErrorCode of
    0 ->
      case veilhelpers:exec(is_reg, [Stat#st_stat.st_mode]) of
        true ->
          FSize = Stat#st_stat.st_size,
          case FSize < Offset of
            false ->
              Flag = veilhelpers:exec(get_flag, [o_wronly]),
              {ErrorCode2, FFI} = veilhelpers:exec(open, Storage_helper_info, [File, #st_fuse_file_info{flags = Flag}]),
              case ErrorCode2 of
                0 ->
                  BytesWritten = write_bytes(Storage_helper_info, File, Offset, Buf, FFI),

                  ErrorCode3 = veilhelpers:exec(release, Storage_helper_info, [File, FFI]),
                  case ErrorCode3 of
                    0 -> BytesWritten;
                    {error, 'NIF_not_loaded'} -> ErrorCode3;
                    _ -> {wrong_release_return_code, ErrorCode3}
                  end;
                error -> {ErrorCode, FFI};
                _ -> {wrong_open_return_code, ErrorCode2}
              end;
            true  -> {error, file_too_small}
          end;
        false -> {error, not_regular_file}
      end;
    error -> {ErrorCode, Stat};
    _ -> {wrong_getatt_return_code, ErrorCode}
  end.

%% write/3
%% ====================================================================
%% @doc Appends data to the end of file (operates only on storage).
%% First it checks file attributes (file type and file size).
%% If everything is ok, it reads data from file.
%% @end
-spec write(Storage_helper_info :: record(), File :: string(), Buf :: binary()) -> Result when
  Result :: BytesWritten | {ErrorGeneral, ErrorDetail},
  BytesWritten :: integer(),
  ErrorGeneral :: atom(),
  ErrorDetail :: term().
%% ====================================================================
write(Storage_helper_info, File, Buf) ->
  {ErrorCode, Stat} = veilhelpers:exec(getattr, Storage_helper_info, [File]),
  case ErrorCode of
    0 ->
      case veilhelpers:exec(is_reg, [Stat#st_stat.st_mode]) of
        true ->
          Offset = Stat#st_stat.st_size,
          Flag = veilhelpers:exec(get_flag, [o_wronly]),
          {ErrorCode2, FFI} = veilhelpers:exec(open, Storage_helper_info, [File, #st_fuse_file_info{flags = Flag}]),
          case ErrorCode2 of
            0 ->
              BytesWritten = write_bytes(Storage_helper_info, File, Offset, Buf, FFI),

              ErrorCode3 = veilhelpers:exec(release, Storage_helper_info, [File, FFI]),
              case ErrorCode3 of
                0 -> BytesWritten;
                {error, 'NIF_not_loaded'} -> ErrorCode3;
                _ -> {wrong_release_return_code, ErrorCode3}
              end;
            error -> {ErrorCode, FFI};
            _ -> {wrong_open_return_code, ErrorCode2}
          end;
        false -> {error, not_regular_file}
      end;
    error -> {ErrorCode, Stat};
    _ -> {wrong_getatt_return_code, ErrorCode}
  end.

%% create/2
%% ====================================================================
%% @doc Creates file (operates only on storage). First it checks if file
%% exists. If not, it creates file.
%% @end
-spec create(Storage_helper_info :: record(), File :: string()) -> Result when
  Result :: ok | {ErrorGeneral, ErrorDetail},
  ErrorGeneral :: atom(),
  ErrorDetail :: term().
%% ====================================================================
create(Storage_helper_info, File) ->
  {ErrorCode, Stat} = veilhelpers:exec(getattr, Storage_helper_info, [File]),
  case ErrorCode of
    0 -> {error, file_exists};
    error -> {ErrorCode, Stat};
    _ ->
      ErrorCode2 = veilhelpers:exec(mknod, Storage_helper_info, [File, ?NewFileStorageMode bor ?S_IFREG, 0]),
      case ErrorCode2 of
        0 ->
          ErrorCode3 = veilhelpers:exec(truncate, Storage_helper_info, [File, 0]),
          case ErrorCode3 of
            0 -> ok;
            {error, 'NIF_not_loaded'} -> ErrorCode3;
            _ -> {wrong_truncate_return_code, ErrorCode3}
          end;
        {error, 'NIF_not_loaded'} -> ErrorCode2;
        _ ->
          lager:error("Can note create file %p, code: %p, helper info: %p, mode: %p%n", [File, ErrorCode2, Storage_helper_info, ?NewFileStorageMode bor ?S_IFREG]),
          {wrong_mknod_return_code, ErrorCode2}
      end
  end.

%% truncate/2
%% ====================================================================
%% @doc Truncates file (operates only on storage). First it checks if file
%% exists and is regular file. If everything is ok, it truncates file.
%% @end
-spec truncate(Storage_helper_info :: record(), File :: string(), Size :: integer()) -> Result when
  Result :: ok | {ErrorGeneral, ErrorDetail},
  ErrorGeneral :: atom(),
  ErrorDetail :: term().
%% ====================================================================
truncate(Storage_helper_info, File, Size) ->
  {ErrorCode, Stat} = veilhelpers:exec(getattr, Storage_helper_info, [File]),
  case ErrorCode of
    0 ->
      case veilhelpers:exec(is_reg, [Stat#st_stat.st_mode]) of
        true ->
          ErrorCode2 = veilhelpers:exec(truncate, Storage_helper_info, [File, Size]),
          case ErrorCode2 of
            0 -> ok;
            {error, 'NIF_not_loaded'} -> ErrorCode2;
            _ -> {wrong_truncate_return_code, ErrorCode2}
          end;
        false -> {error, not_regular_file}
      end;
    error -> {ErrorCode, Stat};
    _ -> {wrong_getatt_return_code, ErrorCode}
  end.

%% delete/2
%% ====================================================================
%% @doc Deletes file (operates only on storage). First it checks if file
%% exists and is regular file. If everything is ok, it deletes file.
%% @end
-spec delete(Storage_helper_info :: record(), File :: string()) -> Result when
  Result :: ok | {ErrorGeneral, ErrorDetail},
  ErrorGeneral :: atom(),
  ErrorDetail :: term().
%% ====================================================================
delete(Storage_helper_info, File) ->
  {ErrorCode, Stat} = veilhelpers:exec(getattr, Storage_helper_info, [File]),
  case ErrorCode of
    0 ->
      case veilhelpers:exec(is_reg, [Stat#st_stat.st_mode]) of
        true ->
          ErrorCode2 = veilhelpers:exec(unlink, Storage_helper_info, [File]),
          case ErrorCode2 of
            0 -> ok;
            {error, 'NIF_not_loaded'} -> ErrorCode2;
            _ -> {wrong_unlink_return_code, ErrorCode2}
          end;
        false -> {error, not_regular_file}
      end;
    error -> {ErrorCode, Stat};
    _ -> {wrong_getatt_return_code, ErrorCode}
  end.

%% ls/0
%% ====================================================================
%% @doc Lists files in directory on storage
%% @end
-spec ls() -> {error, not_implemented_yet}.
%% ====================================================================
ls() ->
  %% czy taka funkcja jest nam do czegoś potrzebna - w końcu znane będą pliki z bazy jak i kopie tymczasowe?
  {error, not_implemented_yet}.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% read_bytes/5
%% ====================================================================
%% @doc Reads file (operates only on storage).It contains loop that reads
%% data until all requested data is read (storage may not be able to provide
%% all requested data at once).
%% @end
-spec read_bytes(Storage_helper_info :: record(), File :: string(), Offset :: integer(), Size :: integer(), FFI :: #st_fuse_file_info{}) -> Result when
  Result :: {ok, Bytes} | {ErrorGeneral, ErrorDetail},
  Bytes :: binary(),
  ErrorGeneral :: atom(),
  ErrorDetail :: term().
%% ====================================================================
read_bytes(_Storage_helper_info, _File, _Offset, 0, _FFI) ->
  {ok, <<>>};

read_bytes(Storage_helper_info, File, Offset, Size, FFI) ->
  {ErrorCode, Bytes} = veilhelpers:exec(read, Storage_helper_info, [File, Size, Offset, FFI]),
  case ErrorCode of
    BytesNum when is_integer(BytesNum), BytesNum > 0 ->
      {TmpErrorCode, TmpBytes} = read_bytes(Storage_helper_info, File, Offset + BytesNum, Size - BytesNum, FFI),
      case TmpErrorCode of
        ok -> {ok, <<Bytes/binary, TmpBytes/binary>>};
        _ -> {TmpErrorCode, TmpBytes}
      end;
    error -> {ErrorCode, Bytes};
    _ -> {error, {wrong_read_return_code, ErrorCode}}
  end.

%% write_bytes/5
%% ====================================================================
%% @doc Writes data to file (operates only on storage). It contains loop
%% that writes data until all data is written (storage may not be able to
%% save all data at once).
%% @end
-spec write_bytes(Storage_helper_info :: record(), File :: string(), Offset :: integer(), Buf :: binary(), FFI :: #st_fuse_file_info{}) -> Result when
  Result :: BytesWritten | {ErrorGeneral, ErrorDetail},
  BytesWritten :: integer(),
  ErrorGeneral :: atom(),
  ErrorDetail :: term().
%% ====================================================================
write_bytes(_Storage_helper_info, _File, _Offset, <<>>, _FFI) ->
  0;

write_bytes(Storage_helper_info, File, Offset, Buf, FFI) ->
  ErrorCode = veilhelpers:exec(write, Storage_helper_info, [File, Buf, Offset, FFI]),
  case ErrorCode of
    BytesNum when is_integer(BytesNum), BytesNum > 0 ->
      <<_:BytesNum/binary, NewBuf/binary>> = Buf,
      TmpErrorCode = write_bytes(Storage_helper_info, File, Offset + BytesNum, NewBuf, FFI),
      case TmpErrorCode of
        BytesNum2 when is_integer(BytesNum2) -> BytesNum2 + BytesNum;
        _ -> TmpErrorCode
      end;
    {error, 'NIF_not_loaded'} -> ErrorCode;
    _ ->
      lager:error("Write bytes error - wrong code: ~p", [ErrorCode]),
      {error, {wrong_write_return_code, ErrorCode}}
  end.