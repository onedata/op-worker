%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module contains functions useful during files operations
%% testing.
%% @end
%% ===================================================================

-module(files_tester).

-include("veil_modules/dao/dao.hrl").
-include("veil_modules/fslogic/fslogic.hrl").

-define(SH, "DirectIO").

%% ====================================================================
%% API
%% ====================================================================
-export([file_exists/1, read_file/2, get_file_location/1]).
-export([file_exists_storage/1, read_file_storage/2]).

%% ====================================================================
%% API functions
%% ====================================================================

%% file_exists/1
%% ====================================================================
%% @doc Checks if file exists on the basis of logical name.
-spec file_exists(LogicalName :: string()) -> Result when
  Result ::  Answer | {error, Reason},
  Answer :: true | file_not_exists_in_db | file_not_exists_at_storage,
  Reason :: term().
%% ====================================================================
file_exists(LogicalName) ->
  File = dao_lib:apply(dao_vfs, get_file, [LogicalName], 0),
  case File of
    {ok, #veil_document{record = #file{location = Location}}} ->
      case dao_lib:apply(dao_vfs, get_storage, [{uuid, Location#file_location.storage_id}], 0) of
        {ok, #veil_document{record = Storage}} ->
          SHI = fslogic_storage:get_sh_for_fuse(?CLUSTER_FUSE_ID, Storage),
          #storage_helper_info{name = SHName, init_args = SHArgs} = SHI,
          case SHName of
            ?SH ->
              case file_exists_storage(SHArgs ++ "/" ++ Location#file_location.file_id) of
                true -> true;
                false -> file_not_exists_at_storage;
                Error -> {storage_error, Error}
              end;
            _ -> {wrong_storage_helper, SHName}
          end;
        Other -> {get_storage_error, Other}
      end;
    {error, file_not_found} ->
      file_not_exists_in_db;
    {error, Reason} ->
      {get_file_error, Reason}
  end.

%% read_file/2
%% ====================================================================
%% @doc Gets data from file on the basis of logical name.
-spec read_file(LogicalName :: string(), BytesNum :: integer()) -> Result when
  Result ::  {ok, Data} | {error, Reason},
  Data :: string(),
  Reason :: term().
%% ====================================================================
read_file(LogicalName, BytesNum) ->
  File = dao_lib:apply(dao_vfs, get_file, [LogicalName], 0),
  case File of
    {ok, #veil_document{record = #file{location = Location}}} ->
      case dao_lib:apply(dao_vfs, get_storage, [{uuid, Location#file_location.storage_id}], 0) of
        {ok, #veil_document{record = Storage}} ->
          SHI = fslogic_storage:get_sh_for_fuse(?CLUSTER_FUSE_ID, Storage),
          #storage_helper_info{name = SHName, init_args = SHArgs} = SHI,
          case SHName of
            ?SH ->
              case read_file_storage(SHArgs ++ "/" ++ Location#file_location.file_id, BytesNum) of
                {ok, Data} -> {ok, Data};
                Error -> {storage_error, Error}
              end;
            _ -> {wrong_storage_helper, SHName}
          end;
        Other -> {get_storage_error, Other}
      end;
    {error, file_not_found} ->
      {error, file_not_exists_in_db};
    {error, Reason} ->
      {get_file_error, Reason}
  end.

%% file_exists_storage/1
%% ====================================================================
%% @doc Checks if file exists on storage.
-spec file_exists_storage(File :: string()) -> Result when
  Result ::  Answer | {error, Reason},
  Answer :: boolean(),
  Reason :: term().
%% ====================================================================
file_exists_storage(File) ->
  OpenAns = file:open(File, read),
  case OpenAns of
    {ok, IoDevice} ->
      file:close(IoDevice),
      true;
    {error, enoent} -> false;
    _ -> OpenAns
  end.

%% read_file_storage/2
%% ====================================================================
%% @doc Gets data from file.
-spec read_file_storage(LogicalName :: string(), BytesNum :: integer()) -> Result when
  Result ::  {ok, Data} | {error, Reason},
  Data :: string(),
  Reason :: term().
%% ====================================================================
read_file_storage(File, BytesNum) ->
  OpenAns = file:open(File, read),
  case OpenAns of
    {ok, IoDevice} ->
      ReadAns = file:read(IoDevice, BytesNum),
      file:close(IoDevice),
      ReadAns;
    _ -> OpenAns
  end.

%% get_file_location/1
%% ====================================================================
%% @doc Gets file location on the basis of logical name.
-spec get_file_location(LogicalName :: string()) -> Result when
  Result ::  {ok, Location} | {error, Reason},
  Location :: string(),
  Reason :: term().
%% ====================================================================
get_file_location(LogicalName) ->
  File = dao_lib:apply(dao_vfs, get_file, [LogicalName], 0),
  case File of
    {ok, #veil_document{record = #file{location = Location}}} ->
      case dao_lib:apply(dao_vfs, get_storage, [{uuid, Location#file_location.storage_id}], 0) of
        {ok, #veil_document{record = Storage}} ->
          SHI = fslogic_storage:get_sh_for_fuse(?CLUSTER_FUSE_ID, Storage),
          #storage_helper_info{name = SHName, init_args = SHArgs} = SHI,
          case SHName of
            ?SH -> {ok, SHArgs ++ "/" ++ Location#file_location.file_id};
            _ -> {wrong_storage_helper, SHName}
          end;
        Other -> {get_storage_error, Other}
      end;
    {error, file_not_found} ->
      file_not_exists_in_db;
    {error, Reason} ->
      {get_file_error, Reason}
  end.