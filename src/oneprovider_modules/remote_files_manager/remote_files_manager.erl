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
-include("registered_names.hrl").
-include("oneprovider_modules/fslogic/fslogic.hrl").
-include("oneprovider_modules/dao/dao.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/global_registry/gr_providers.hrl").

%% ====================================================================
%% API
%% ====================================================================
-export([init/1, handle/2, cleanup/0]).

%% ====================================================================
%% Test API
%% ====================================================================
-ifdef(TEST).
-export([get_storage_and_id/1, verify_file_name/1]).
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
  {ok, Interval} = application:get_env(?APP_Name, fslogic_cleaning_period),
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

handle(_ProtocolVersion, healthcheck) ->
	ok;

handle(_ProtocolVersion, get_version) ->
  node_manager:check_vsn();

handle(ProtocolVersion, #remotefilemangement{input = RequestBody, space_id = SpaceId}) ->
    ?debug("RFM space ctx ~p", [SpaceId]),
    RequestType = element(1, RequestBody),

    fslogic_context:set_protocol_version(ProtocolVersion),
    fslogic:fslogic_runner(fun(ReqBody) -> maybe_handle_message(ReqBody, SpaceId) end, RequestType, RequestBody, remote_files_manager_errors);

handle(_ProtocolVersion, _Msg) ->
  ?warning("Wrong request: ~p", [_Msg]),
  ok.

%% cleanup/0
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback cleanup/0
-spec cleanup() -> ok.
%% ====================================================================
cleanup() ->
  ok.


%% maybe_handle_message/1
%% ====================================================================
%% @doc Tries to handle message locally (i.e. handle_message/1) or delegate request to 'provider_proxy' module.
%% @end
-spec maybe_handle_message(RequestBody :: tuple(), SpaceId :: binary()) -> Result :: term().
%% ====================================================================
maybe_handle_message(RequestBody, SpaceId) ->
    {ok, #space_info{providers = Providers}} = fslogic_objects:get_space({uuid, SpaceId}),
    Self = cluster_manager_lib:get_provider_id(),
    case lists:member(Self, Providers) of
        true ->
            handle_message(RequestBody);
        false ->
            [RerouteToProvider | _] = Providers,
            {ok, #provider_details{urls = URLs}} = gr_providers:get_details(provider, RerouteToProvider),
            ?info("Reroute to: ~p", [URLs]),
            try
                provider_proxy:reroute_pull_message(RerouteToProvider, fslogic_context:get_gr_auth(),
                    fslogic_context:get_fuse_id(), #remotefilemangement{space_id = SpaceId, input = RequestBody, message_type = atom_to_list(element(1, RequestBody))})
            catch
                Type:Reason ->
                    ?error_stacktrace("Unable to process remote files manager request to provider ~p due to: ~p", [RerouteToProvider, {Type, Reason}]),
                    throw({unable_to_reroute_message, Reason})
            end
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% handle_message/1
%% ====================================================================
%% @doc Processes requests from Cluster Proxy.
%% @end
-spec handle_message(Record :: tuple()) -> Result when
  Result :: term().
%% ====================================================================

handle_message(Record) when is_record(Record, getattr) ->
    FileId = Record#getattr.file_id,
    {Storage_helper_info, File} = get_helper_and_id(FileId, fslogic_context:get_protocol_version()),
    {ok, #st_stat{} = Stat} = storage_files_manager:getattr(Storage_helper_info, File),
    #storageattibutes{
        answer = ?VOK,
        atime = Stat#st_stat.st_atime,
        blksize = Stat#st_stat.st_blksize,
        blocks = Stat#st_stat.st_blocks,
        ctime = Stat#st_stat.st_ctime,
        dev = Stat#st_stat.st_dev,
        gid = Stat#st_stat.st_gid,
        ino = Stat#st_stat.st_ino,
        mode = Stat#st_stat.st_mode,
        mtime = Stat#st_stat.st_mtime,
        nlink = Stat#st_stat.st_nlink,
        rdev = Stat#st_stat.st_rdev,
        size = Stat#st_stat.st_size,
        uid = Stat#st_stat.st_uid
    };

handle_message(Record) when is_record(Record, createfile) ->
    FileId = Record#createfile.file_id,
    Mode = Record#createfile.mode,
    SH_And_ID = get_helper_and_id(FileId, fslogic_context:get_protocol_version()),
    case SH_And_ID of
    {Storage_helper_info, File} ->
        TmpAns = storage_files_manager:create(Storage_helper_info, File,Mode),
            case TmpAns of
                ok -> #atom{value = ?VOK};
                {_, ErrorCode} when is_integer(ErrorCode) ->
                    throw(fslogic_errors:posix_to_oneerror(ErrorCode));
                Other ->
                    ?warning("create error: ~p, shi: ~p, file: ~p", [Other, Storage_helper_info, File]),
                    #atom{value = ?VEREMOTEIO}
            end;
        _ -> #atom{value = ?VEREMOTEIO}
    end;

handle_message(Record) when is_record(Record, deletefileatstorage) ->
  FileId = Record#deletefileatstorage.file_id,
  SH_And_ID = get_helper_and_id(FileId, fslogic_context:get_protocol_version()),
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
                  {_, ErrorCode} when is_integer(ErrorCode) ->
                      throw(fslogic_errors:posix_to_oneerror(ErrorCode));
                Other ->
                  ?warning("storage_files_manager:delete error: ~p, shi: ~p, file: ~p", [Other, Storage_helper_info, File]),
                  #atom{value = ?VEREMOTEIO}
              end;
            false ->
              #atom{value = ?VEACCES}
          end;
        _ ->
          ?warning("delete error: can not check permissions, shi: ~p, file: ~p", [Storage_helper_info, File]),
          #atom{value = ?VEREMOTEIO}
      end;
    _ -> #atom{value = ?VEREMOTEIO}
  end;

handle_message(Record) when is_record(Record, truncatefile) ->
    FileId = Record#truncatefile.file_id,
  Length = Record#truncatefile.length,
  SH_And_ID = get_helper_and_id(FileId, fslogic_context:get_protocol_version()),
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
                  {_, ErrorCode} when is_integer(ErrorCode) ->
                      throw(fslogic_errors:posix_to_oneerror(ErrorCode));
                Other ->
                  ?warning("storage_files_manager:truncate error: ~p, shi: ~p, file: ~p", [Other, Storage_helper_info, File]),
                  #atom{value = ?VEREMOTEIO}
              end;
            false ->
              #atom{value = ?VEACCES}
          end;
        _ ->
          ?warning("truncatefile error: can not check permissions, shi: ~p, file: ~p", [Storage_helper_info, File]),
          #atom{value = ?VEREMOTEIO}
      end;
    _ -> #atom{value = ?VEREMOTEIO}
  end;

handle_message(Record) when is_record(Record, readfile) ->
    FileId = Record#readfile.file_id,
  Size = Record#readfile.size,
  Offset = Record#readfile.offset,
  SH_And_ID = get_helper_and_id(FileId, fslogic_context:get_protocol_version()),
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
                  {_, ErrorCode} when is_integer(ErrorCode) ->
                      throw(fslogic_errors:posix_to_oneerror(ErrorCode));
                Other ->
                  ?warning("storage_files_manager:read error: ~p, shi: ~p, file: ~p", [Other, Storage_helper_info, File]),
                  #filedata{answer_status = ?VEREMOTEIO}
              end;
            false ->
              #filedata{answer_status = ?VEACCES}
          end;
        _ ->
          ?warning("readfile error: can not check permissions, shi: ~p, file: ~p", [Storage_helper_info, File]),
          #filedata{answer_status = ?VEREMOTEIO}
      end;
    _ -> #filedata{answer_status = ?VEREMOTEIO}
  end;

handle_message(Record) when is_record(Record, writefile) ->
  FileId = Record#writefile.file_id,
  Bytes = Record#writefile.data,
  Offset = Record#writefile.offset,
  SH_And_ID = get_helper_and_id(FileId, fslogic_context:get_protocol_version()),
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
                  {_, ErrorCode} when is_integer(ErrorCode) ->
                      throw(fslogic_errors:posix_to_oneerror(ErrorCode));
                Other ->
                  ?warning("storage_files_manager:write error: ~p, shi: ~p, file: ~p", [Other, Storage_helper_info, File]),
                  #writeinfo{answer_status = ?VEREMOTEIO}
              end;
            false ->
              #writeinfo{answer_status = ?VEACCES}
          end;
        _ ->
          ?warning("writefile error: can not check permissions, shi: ~p, file: ~p", [Storage_helper_info, File]),
          #writeinfo{answer_status = ?VEREMOTEIO}
      end;
    _ -> #writeinfo{answer_status = ?VEREMOTEIO}
  end;

handle_message(Record) when is_record(Record, changepermsatstorage) ->
  FileId = Record#changepermsatstorage.file_id,
  Perms = Record#changepermsatstorage.perms,
  SH_And_ID = get_helper_and_id(FileId, fslogic_context:get_protocol_version()),
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
                  {_, ErrorCode} when is_integer(ErrorCode) ->
                      throw(fslogic_errors:posix_to_oneerror(ErrorCode));
                Other ->
                  ?warning("storage_files_manager:chmod error: ~p, shi: ~p, file: ~p", [Other, Storage_helper_info, File]),
                  #atom{value = ?VEREMOTEIO}
              end;
            false ->
              #atom{value = ?VEACCES}
          end;
        _ ->
          ?warning("changepermsatstorage error: can not check permissions, shi: ~p, file: ~p", [Storage_helper_info, File]),
          #atom{value = ?VEREMOTEIO}
      end;
    _ -> #atom{value = ?VEREMOTEIO}
  end.

get_storage_and_id([$/ | Combined]) ->
    get_storage_and_id(Combined);
get_storage_and_id(Combined) ->
  [StorageStr | PathTokens] = filename:split(Combined),
  Storage = list_to_integer(StorageStr),
  File = filename:join(PathTokens),
  case verify_file_name(File) of
    {error, _} -> error;
    {ok, VerifiedFile} -> {Storage, VerifiedFile}
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
      ?warning("Can not get storage and id from file_id: ~p", [Combined]),
      error;
    _ ->
      {Storage, File} = Storage_And_ID,
      case dao_lib:apply(dao_vfs, get_storage, [{id, Storage}], ProtocolVersion) of
        {ok, #db_document{record = StorageRecord}} ->
          SHI = fslogic_storage:get_sh_for_fuse(?CLUSTER_FUSE_ID, StorageRecord),
          ?debug("SHI and info for remote operation: ~p", [{SHI, File}]),
          {SHI, File};
        Other ->
          ?warning("Can not get storage from id: ~p", [Other]),
          error
      end
  end.

%% Verify filename
%% (skip single dot in filename, return error when double dot in filename, return filename otherwies)
verify_file_name(FileName) ->
  Tokens = string:tokens(FileName, "/"),
  case lists:any(fun(X) -> X =:= ".." end, Tokens) of
    true -> {error, wrong_filename};
    _ -> {ok, string:join(Tokens, "/")}
  end.
