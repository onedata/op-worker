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

-include("oneprovider_modules/dao/dao.hrl").
-include("oneprovider_modules/fslogic/fslogic.hrl").
-include_lib("dao/include/common.hrl").

-define(SH, "DirectIO").

%% ====================================================================
%% API
%% ====================================================================
-export([file_exists/1, read_file/2, get_file_location/1, get_permissions/1, get_owner/1]).
-export([file_exists_storage/1, read_file_storage/2, delete/1, delete_dir/1]).

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
    {ok, #db_document{record = #file{}} = FileDoc} ->
      Location = fslogic_file:get_file_local_location(FileDoc),
      case dao_lib:apply(dao_vfs, get_storage, [{uuid, Location#file_location.storage_uuid}], 0) of
        {ok, #db_document{record = Storage}} ->
          SHI = fslogic_storage:get_sh_for_fuse(?CLUSTER_FUSE_ID, Storage),
          #storage_helper_info{name = SHName, init_args = SHArgs} = SHI,
          case SHName of
            ?SH ->
              case file_exists_storage(SHArgs ++ "/" ++ Location#file_location.storage_file_id) of
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
    {ok, #db_document{record = #file{}} = FileDoc} ->
      Location = fslogic_file:get_file_local_location(FileDoc),
      case dao_lib:apply(dao_vfs, get_storage, [{uuid, Location#file_location.storage_uuid}], 0) of
        {ok, #db_document{record = Storage}} ->
          SHI = fslogic_storage:get_sh_for_fuse(?CLUSTER_FUSE_ID, Storage),
          #storage_helper_info{name = SHName, init_args = SHArgs} = SHI,
          case SHName of
            ?SH ->
              case read_file_storage(SHArgs ++ "/" ++ Location#file_location.storage_file_id, BytesNum) of
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
  Check = filelib:is_file(File),
  case Check of
    true ->
      Check2 = filelib:is_dir(File),
      case Check2 of
        true -> dir;
        false -> true
      end;
    _ -> Check
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

%% delete/1
%% ====================================================================
%% @doc Deletes file
-spec delete(File :: string()) -> Result when
  Result ::  ok | {error, Reason},
  Reason :: term().
%% ====================================================================
delete(File) ->
  file:delete(File).

%% delete_dir/1
%% ====================================================================
%% @doc Deletes dir
-spec delete_dir(File :: string()) -> Result when
  Result ::  ok | {error, Reason},
  Reason :: term().
%% ====================================================================
delete_dir(File) ->
  file:del_dir(File).


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
    {ok, #db_document{record = #file{}} = FileDoc} ->
      Location = fslogic_file:get_file_local_location(FileDoc),
      case dao_lib:apply(dao_vfs, get_storage, [{uuid, Location#file_location.storage_uuid}], 0) of
        {ok, #db_document{record = Storage}} ->
          SHI = fslogic_storage:get_sh_for_fuse(?CLUSTER_FUSE_ID, Storage),
          #storage_helper_info{name = SHName, init_args = SHArgs} = SHI,
          case SHName of
            ?SH -> {ok, lists:nth(1, SHArgs) ++ "/" ++ Location#file_location.storage_file_id};
            _ -> {wrong_storage_helper, SHName}
          end;
        Other -> {get_storage_error, Other}
      end;
    {error, file_not_found} ->
      file_not_exists_in_db;
    {error, Reason} ->
      {get_file_error, Reason}
  end.

%% get_permissions/1
%% ====================================================================
%% @doc Returns file's permissions
-spec get_permissions(File :: string()) -> Result when
  Result ::  {ok, Permissions} | {error, Reason},
  Permissions :: integer(),
  Reason :: term().
%% ====================================================================
get_permissions(File) ->
  {Status, Info} = file:read_file_info(File),
  case Status of
    ok ->
      %% -include_lib("kernel/include/file.hrl"). can not be used because of records' names conflict
      {ok, lists:nth(8,tuple_to_list(Info))};
    _ -> {Status, Info}
  end.

%% get_owner/1
%% ====================================================================
%% @doc Returns file's owner
-spec get_owner(File :: string()) -> Result when
  Result ::  {ok, User, Group} | {error, Reason},
  User :: integer(),
  Group :: integer(),
  Reason :: term().
%% ====================================================================
get_owner(File) ->
  {Status, Info} = file:read_file_info(File),
  case Status of
    ok ->
      %% -include_lib("kernel/include/file.hrl"). can not be used because of records' names conflict
      {ok, lists:nth(13,tuple_to_list(Info)), lists:nth(14,tuple_to_list(Info))};
    _ -> {Status, Info}
  end.
