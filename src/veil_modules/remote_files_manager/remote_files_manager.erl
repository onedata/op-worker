%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module implements worker_plugin_behaviour to provide
%% functionality of remote files manager.
%% @end
%% ===================================================================

-module(remote_files_manager).
-behaviour(worker_plugin_behaviour).

-include("remote_file_management_pb.hrl").
-include("communication_protocol_pb.hrl").
-include("veil_modules/fslogic/fslogic.hrl").
-include("veil_modules/dao/dao.hrl").

%% ====================================================================
%% API
%% ====================================================================
-export([init/1, handle/2, cleanup/0]).

%% ====================================================================
%% Test API
%% ====================================================================
-ifdef(TEST).
-export([get_storage_and_id/1]).
-endif.

%% ====================================================================
%% API functions
%% ====================================================================

%% init/1
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback init/1
-spec init(Args :: term()) -> list().
%% ====================================================================
init(_Args) ->
  Pid = self(),
  {ok, Interval} = application:get_env(veil_cluster_node, fslogic_cleaning_period),
  erlang:send_after(Interval * 1000, Pid, {timer, {asynch, 1, {delete_old_descriptors, Pid}}}),
  [].

%% handle/2
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback handle/1. <br/>
%% Processes standard worker requests (e.g. ping) and requests from FUSE.
%% @end
-spec handle(ProtocolVersion :: term(), Request :: term()) -> Result when
  Result :: term().
%% ====================================================================
handle(_ProtocolVersion, ping) ->
  pong;

handle(_ProtocolVersion, get_version) ->
  node_manager:check_vsn();

handle(ProtocolVersion, Record) when is_record(Record, remotefilemangement) ->
  handle_message(ProtocolVersion, Record#remotefilemangement.input);

handle(_ProtocolVersion, _Msg) ->
  ok.

%% cleanup/0
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback cleanup/0
-spec cleanup() -> ok.
%% ====================================================================
cleanup() ->
  ok.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% handle_message/2
%% ====================================================================
%% @doc Processes requests from Cluster Proxy.
%% @end
-spec handle_message(ProtocolVersion :: term(), Record :: tuple()) -> Result when
  Result :: term().
%% ====================================================================
handle_message(ProtocolVersion, Record) when is_record(Record, createfile) ->
  FileId = Record#createfile.file_id,
  SH_And_ID = get_helper_and_id(FileId, ProtocolVersion),
  case SH_And_ID of
    {Storage_helper_info, File} ->
      {PermsStatus, Perms} = storage_files_manager:check_perms(File, Storage_helper_info, read),
      case PermsStatus of
        ok ->
          case Perms of
            true ->
              TmpAns = storage_files_manager:create(Storage_helper_info, File),
              case TmpAns of
                ok -> #atom{value = ?VOK};
                Other ->
                  lager:warning("storage_files_manager:create error: ~p, shi: ~p, file: ~p", [Other, Storage_helper_info, File]),
                  #atom{value = ?VEREMOTEIO}
              end;
            false ->
              #atom{value = ?VEPERM}
          end;
        _ ->
          lager:warning("createfile error: can not check permissions, shi: ~p, file: ~p, error: ~p", [Storage_helper_info, File, {PermsStatus, Perms}]),
          #atom{value = ?VEREMOTEIO}
      end;
    _ -> #atom{value = ?VEREMOTEIO}
  end;

handle_message(ProtocolVersion, Record) when is_record(Record, deletefileatstorage) ->
  FileId = Record#deletefileatstorage.file_id,
  SH_And_ID = get_helper_and_id(FileId, ProtocolVersion),
  case SH_And_ID of
    {Storage_helper_info, File} ->
      {PermsStatus, Perms} = storage_files_manager:check_perms(File, Storage_helper_info, perms),
      case PermsStatus of
        ok ->
          case Perms of
            true ->
              TmpAns = storage_files_manager:delete(Storage_helper_info, File),
              case TmpAns of
                ok -> #atom{value = ?VOK};
                Other ->
                  lager:warning("storage_files_manager:delete error: ~p, shi: ~p, file: ~p", [Other, Storage_helper_info, File]),
                  #atom{value = ?VEREMOTEIO}
              end;
            false ->
              #atom{value = ?VEPERM}
          end;
        _ ->
          lager:warning("delete error: can not check permissions, shi: ~p, file: ~p", [Storage_helper_info, File]),
          #atom{value = ?VEREMOTEIO}
      end;
    _ -> #atom{value = ?VEREMOTEIO}
  end;

handle_message(ProtocolVersion, Record) when is_record(Record, truncatefile) ->
  FileId = Record#truncatefile.file_id,
  Length = Record#truncatefile.length,
  SH_And_ID = get_helper_and_id(FileId, ProtocolVersion),
  case SH_And_ID of
    {Storage_helper_info, File} ->
      {PermsStatus, Perms} = storage_files_manager:check_perms(File, Storage_helper_info),
      case PermsStatus of
        ok ->
          case Perms of
            true ->
              TmpAns = storage_files_manager:truncate(Storage_helper_info, File, Length),
              case TmpAns of
                ok -> #atom{value = ?VOK};
                Other ->
                  lager:warning("storage_files_manager:truncate error: ~p, shi: ~p, file: ~p", [Other, Storage_helper_info, File]),
                  #atom{value = ?VEREMOTEIO}
              end;
            false ->
              #atom{value = ?VEPERM}
          end;
        _ ->
          lager:warning("truncatefile error: can not check permissions, shi: ~p, file: ~p", [Storage_helper_info, File]),
          #atom{value = ?VEREMOTEIO}
      end;
    _ -> #atom{value = ?VEREMOTEIO}
  end;

handle_message(ProtocolVersion, Record) when is_record(Record, readfile) ->
  FileId = Record#readfile.file_id,
  Size = Record#readfile.size,
  Offset = Record#readfile.offset,
  SH_And_ID = get_helper_and_id(FileId, ProtocolVersion),
  case SH_And_ID of
    {Storage_helper_info, File} ->
      {PermsStatus, Perms} = storage_files_manager:check_perms(File, Storage_helper_info, read),
      case PermsStatus of
        ok ->
          case Perms of
            true ->
              TmpAns = storage_files_manager:read(Storage_helper_info, File, Offset, Size),
              case TmpAns of
                {ok, Bytes} -> #filedata{answer_status = ?VOK, data = Bytes};
                Other ->
                  lager:warning("storage_files_manager:read error: ~p, shi: ~p, file: ~p", [Other, Storage_helper_info, File]),
                  #filedata{answer_status = ?VEREMOTEIO}
              end;
            false ->
              #filedata{answer_status = ?VEPERM}
          end;
        _ ->
          lager:warning("readfile error: can not check permissions, shi: ~p, file: ~p", [Storage_helper_info, File]),
          #filedata{answer_status = ?VEREMOTEIO}
      end;
    _ -> #filedata{answer_status = ?VEREMOTEIO}
  end;

handle_message(ProtocolVersion, Record) when is_record(Record, writefile) ->
  FileId = Record#writefile.file_id,
  Bytes = Record#writefile.data,
  Offset = Record#writefile.offset,
  SH_And_ID = get_helper_and_id(FileId, ProtocolVersion),
  case SH_And_ID of
    {Storage_helper_info, File} ->
      {PermsStatus, Perms} = storage_files_manager:check_perms(File, Storage_helper_info),
      case PermsStatus of
        ok ->
          case Perms of
            true ->
              TmpAns = storage_files_manager:write(Storage_helper_info, File, Offset, Bytes),
              case TmpAns of
                BytesNum when is_integer(BytesNum) -> #writeinfo{answer_status = ?VOK, bytes_written = BytesNum};
                Other ->
                  lager:warning("storage_files_manager:write error: ~p, shi: ~p, file: ~p", [Other, Storage_helper_info, File]),
                  #writeinfo{answer_status = ?VEREMOTEIO}
              end;
            false ->
              #writeinfo{answer_status = ?VEPERM}
          end;
        _ ->
          lager:warning("writefile error: can not check permissions, shi: ~p, file: ~p", [Storage_helper_info, File]),
          #writeinfo{answer_status = ?VEREMOTEIO}
      end;
    _ -> #writeinfo{answer_status = ?VEREMOTEIO}
  end;

handle_message(ProtocolVersion, Record) when is_record(Record, changepermsatstorage) ->
  FileId = Record#changepermsatstorage.file_id,
  Perms = Record#changepermsatstorage.perms,
  SH_And_ID = get_helper_and_id(FileId, ProtocolVersion),
  case SH_And_ID of
    {Storage_helper_info, File} ->
      {PermsStatus, PermsCheck} = storage_files_manager:check_perms(File, Storage_helper_info, perms),
      case PermsStatus of
        ok ->
          case PermsCheck of
            true ->
              TmpAns = storage_files_manager:chmod(Storage_helper_info, File, Perms),
              case TmpAns of
                ok -> #atom{value = ?VOK};
                Other ->
                  lager:warning("storage_files_manager:chmod error: ~p, shi: ~p, file: ~p", [Other, Storage_helper_info, File]),
                  #atom{value = ?VEREMOTEIO}
              end;
            false ->
              #atom{value = ?VEPERM}
          end;
        _ ->
          lager:warning("changepermsatstorage error: can not check permissions, shi: ~p, file: ~p", [Storage_helper_info, File]),
          #atom{value = ?VEREMOTEIO}
      end;
    _ -> #atom{value = ?VEREMOTEIO}
  end.

get_storage_and_id(Combined) ->
  Pos = string:str(Combined, ?REMOTE_HELPER_SEPARATOR),
  case Pos of
    0 -> error;
    _ ->
      try
        Storage = list_to_integer(string:substr(Combined, 1, Pos - 1)),
        File = string:substr(Combined, Pos + length(?REMOTE_HELPER_SEPARATOR)),
        {Storage, File}
      catch
        _:_ -> error
      end
  end.

%% get_helper_and_id/2
%% ====================================================================
%% @doc Gets storage helper info and new file id on the basis of file id from request.
%% @end
-spec get_helper_and_id(Combined :: string(), ProtocolVersion :: term()) -> Result when
  Result :: {SHI, FileId} | error,
  SHI :: term(),
  FileId :: string().
%% ====================================================================
get_helper_and_id(Combined, ProtocolVersion) ->
  Storage_And_ID = get_storage_and_id(Combined),
  case Storage_And_ID of
    error ->
      lager:warning("Can not get storage and id from file_id: ~p", [Combined]),
      error;
    _ ->
      {Storage, File} = Storage_And_ID,
      case dao_lib:apply(dao_vfs, get_storage, [{id, Storage}], ProtocolVersion) of
        {ok, #veil_document{record = StorageRecord}} ->
          SHI = fslogic_storage:get_sh_for_fuse(?CLUSTER_FUSE_ID, StorageRecord),
          lager:debug("SHI and info for remote operation: ~p", [{SHI, File}]),
          {SHI, File};
        Other ->
          lager:warning("Can not get storage from id: ~p", [Other]),
          error
      end
  end.
