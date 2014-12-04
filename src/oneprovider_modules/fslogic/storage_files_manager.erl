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
-include("oneprovider_modules/fslogic/fslogic.hrl").
-include("oneprovider_modules/dao/dao_vfs.hrl").
-include("oneprovider_modules/dao/dao_share.hrl").
-include("cluster_elements/request_dispatcher/gsi_handler.hrl").
-include_lib("oneprovider_modules/dao/dao_types.hrl").
-include_lib("ctool/include/logging.hrl").

-define(S_IFREG, 8#100000).

%% ====================================================================
%% API
%% ====================================================================
%% Physical files organization management (to better organize files on storage;
%% the user does not see results of these operations)
-export([mkdir/2, mkdir/3, mv/3, delete_dir/2, chmod/3, chown/4, link/3]).

%% Physical files access (used to create temporary copies for remote files)
-export([getattr/2, read/4, write/4, create/2, create/3, truncate/3, delete/2, ls/0]).

%% Helper functions
-export([check_perms/2, check_perms/3]).

%% ====================================================================
%% Test API
%% ====================================================================
%% eunit
-ifdef(TEST).
-export([get_cached_value/3]).
-endif.

%% ====================================================================
%% API functions
%% ====================================================================

%% ====================================================================
%% Physical files organization management (to better organize files on storage;
%% the user does not see results of these operations)
%% ====================================================================


%% link/3
%% ====================================================================
%% @doc Creates hard-link on storage
%% @end
-spec link(Storage_helper_info :: record(), FileId :: string(), LinkId :: string()) -> Result when
    Result :: ok | {ErrorGeneral, ErrorDetail},
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
link(Storage_helper_info, FileId, LinkId) ->
    case helpers:exec(link, Storage_helper_info, [FileId, LinkId]) of
        0 -> ok;
        ErrorCode ->
            {error, fslogic_errors:posix_to_oneerror(ErrorCode)}
    end.

%% getattr/2
%% ====================================================================
%% @doc Gets attributes of the file.
%% @end
-spec getattr(Storage_helper_info :: record(), FileId :: string()) -> Result when
    Result :: {ok, #st_stat{}} | {error, Reason :: fslogic_error()}.
%% ====================================================================
getattr(Storage_helper_info, FileId) ->
    case helpers:exec(getattr, Storage_helper_info, [FileId]) of
        {0, #st_stat{} = Attrs} ->
            {ok, Attrs};
        {ErrorCode, _} ->
            {error, fslogic_errors:posix_to_oneerror(ErrorCode)}
    end.

%% mkdir/2
%% ====================================================================
%% @doc Creates dir on storage
%% @end
-spec mkdir(Storage_helper_info :: record(), Dir :: string()) -> Result when
    Result :: ok | {ErrorGeneral, ErrorDetail},
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
mkdir(Storage_helper_info, Dir) ->
    {ok, Mode} = application:get_env(?APP_Name, new_dir_storage_mode),
    mkdir(Storage_helper_info, Dir, Mode).
mkdir(Storage_helper_info, Dir, Mode) ->
    {ErrorCode, Stat} = helpers:exec(getattr, Storage_helper_info, [Dir]),
    case ErrorCode of
        0 -> {error, dir_or_file_exists};
        error -> {ErrorCode, Stat};
        _ ->
            ErrorCode2 = helpers:exec(mkdir, Storage_helper_info, [Dir, Mode]),
            case ErrorCode2 of
                0 ->
                    derive_gid_from_parent(Storage_helper_info, Dir),

                    Query = fslogic_context:get_user_query(),

                    case Query of
                        undefined -> ok;
                        _ ->
                            {GetUserAns, User} = user_logic:get_user(Query),
                            case GetUserAns of
                                ok ->
                                    {_Login, UID} = user_logic:get_login_with_uid(User),
                                    ChownAns = chown(Storage_helper_info, Dir, UID, -1),
                                    case ChownAns of
                                        ok -> ok;
                                        _ -> {cannot_change_dir_owner, ChownAns}
                                    end;
                                _ -> {cannot_change_dir_owner, get_user_error}
                            end
                    end;
                {error, 'NIF_not_loaded'} -> ErrorCode2;
                _ ->
                    %% ?error("Can not create dir %p, code: %p, helper info: %p, mode: %p%n", [Dir, ErrorCode2, Storage_helper_info, NewDirStorageMode]),
                    {wrong_mkdir_return_code, ErrorCode2}
            end
    end.

%% mv/3
%% ====================================================================
%% @doc Moves file on storage
%% @end
-spec mv(Storage_helper_info :: record(), From :: string(), To :: string()) -> Result when
    Result :: ok | {ErrorGeneral, ErrorDetail},
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
mv(_Storage_helper_info, From, From) ->
    ok;
mv(Storage_helper_info, From, To) ->
    ErrorCode = helpers:exec(rename, Storage_helper_info, [From, To]),
    case ErrorCode of
        0 -> ok;
        {error, 'NIF_not_loaded'} -> ErrorCode;
        _ ->
%%       ?error("Can not move file from ~p to ~p, code: ~p, helper info: ~p", [From, To, ErrorCode, Storage_helper_info]),
            {wrong_rename_return_code, ErrorCode}
    end.

%% delete_dir/2
%% ====================================================================
%% @doc Deletes dir on storage
%% @end
-spec delete_dir(Storage_helper_info :: record(), Dir :: string()) -> Result when
    Result :: ok | {ErrorGeneral, ErrorDetail},
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
delete_dir(Storage_helper_info, File) ->
    {ErrorCode, Stat} = get_cached_value(File, is_dir, Storage_helper_info),
    case ErrorCode of
        ok ->
            case Stat of
                true ->
                    ErrorCode2 = helpers:exec(rmdir, Storage_helper_info, [File]),
                    case ErrorCode2 of
                        0 -> clear_cache(File);
                        {error, 'NIF_not_loaded'} ->
                            clear_cache(File),
                            ErrorCode2;
                        _ ->
                            clear_cache(File),
                            {wrong_rmdir_return_code, ErrorCode2}
                    end;
                false ->
                    clear_cache(File),
                    {error, not_directory}
            end;
        error ->
            clear_cache(File),
            {ErrorCode, Stat};
        _ ->
            clear_cache(File),
            {ErrorCode, Stat}
    end.

%% chmod/3
%% ====================================================================
%% @doc Change file mode at storage
%% @end
-spec chmod(Storage_helper_info :: record(), Dir :: string(), Mode :: integer()) -> Result when
    Result :: ok | {ErrorGeneral, ErrorDetail},
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
chmod(Storage_helper_info, File, Mode) ->
    ok = setup_ctx(File),
    ErrorCode = helpers:exec(chmod, Storage_helper_info, [File, Mode]),
    case ErrorCode of
        0 -> ok;
        _ -> {error, ErrorCode}
    end.

%% chown/4
%% ====================================================================
%% @doc Change file's owner (if user or group shouldn't be changed use "" as an argument)
%% @end
-spec chown(Storage_helper_info :: record(), Dir :: string(), User :: string(), Group :: string()) -> Result when
    Result :: ok | {ErrorGeneral, ErrorDetail},
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
chown(Storage_helper_info, File, User, Group) when is_integer(User), is_integer(Group) ->
    %% ok = setup_ctx(File),
    ErrorCode = helpers:exec(chown, Storage_helper_info, [File, User, Group]),
    case ErrorCode of
        0 -> ok;
        _ -> {error, ErrorCode}
    end;
chown(Storage_helper_info, File, User, Group) ->
    ErrorCode = helpers:exec(chown_name, Storage_helper_info, [File, User, Group]),
    case ErrorCode of
        0 -> ok;
        _ -> {error, ErrorCode}
    end.

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
    ok = case get_cached_value(File, mode, Storage_helper_info) of
             {ok, Mask} when (Mask band (?RWE_USR_PERM bor ?RWE_GRP_PERM bor ?RWE_OTH_PERM)) == 0 ->
                 case has_permission(File, read) of
                     true -> set_root_ctx();
                     false -> setup_ctx(File)
                 end;
             _ -> setup_ctx(File)
         end,
    {ErrorCode, CValue} = get_cached_value(File, size, Storage_helper_info),
    case ErrorCode of
        ok ->
            {IsReg, FSize} = CValue,
            case IsReg of
                true ->
                    case FSize < Offset of
                        false ->
                            {FlagCode, Flag} = get_cached_value(File, o_rdonly, Storage_helper_info),
                            case FlagCode of
                                ok ->
                                    {ErrorCode2, FFI} = helpers:exec(open, Storage_helper_info, [File, #st_fuse_file_info{flags = Flag}]),
                                    case ErrorCode2 of
                                        0 ->
                                            Size2 = case Offset + Size > FSize of
                                                        true -> FSize - Offset;
                                                        false -> Size
                                                    end,
                                            {ReadAns, Bytes} = read_bytes(Storage_helper_info, File, Offset, Size2, FFI),

                                            ErrorCode3 = helpers:exec(release, Storage_helper_info, [File, FFI]),
                                            case ErrorCode3 of
                                                0 -> {ReadAns, Bytes};
                                                {error, 'NIF_not_loaded'} -> ErrorCode3;
                                                _ -> {wrong_release_return_code, ErrorCode3}
                                            end;
                                        error -> {ErrorCode, FFI};
                                        _ -> {wrong_open_return_code, ErrorCode2}
                                    end;
                                _ -> {FlagCode, Flag}
                            end;
                        true -> {error, file_too_small}
                    end;
                false -> {error, not_regular_file}
            end;
        error -> {ErrorCode, CValue};
        _ -> {ErrorCode, CValue}
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
    ok = case get_cached_value(File, mode, Storage_helper_info) of
             {ok, Mask} when (Mask band (?RWE_USR_PERM bor ?RWE_GRP_PERM bor ?RWE_OTH_PERM)) == 0 ->
                 case has_permission(File, write) of
                     true -> set_root_ctx();
                     false -> setup_ctx(File)
                 end;
             _ -> setup_ctx(File)
         end,
    {ErrorCode, Stat} = get_cached_value(File, is_reg, Storage_helper_info),
    case ErrorCode of
        ok ->
            case Stat of
                true ->
                    {FlagCode, Flag} = get_cached_value(File, o_wronly, Storage_helper_info),
                    case FlagCode of
                        ok ->
                            {ErrorCode2, FFI} = helpers:exec(open, Storage_helper_info, [File, #st_fuse_file_info{flags = Flag}]),
                            case ErrorCode2 of
                                0 ->
                                    BytesWritten = write_bytes(Storage_helper_info, File, Offset, Buf, FFI),

                                    ErrorCode3 = helpers:exec(release, Storage_helper_info, [File, FFI]),
                                    case ErrorCode3 of
                                        0 -> BytesWritten;
                                        {error, 'NIF_not_loaded'} -> ErrorCode3;
                                        _ -> {wrong_release_return_code, ErrorCode3}
                                    end;
                                error -> {ErrorCode, FFI};
                                _ -> {wrong_open_return_code, ErrorCode2}
                            end;
                        _ -> {FlagCode, Flag}
                    end;
                false -> {error, not_regular_file}
            end;
        error -> {ErrorCode, Stat};
        _ -> {ErrorCode, Stat}
    end.

%% create/2
%% ====================================================================
%% @doc Creates file with default mode (operates only on storage). First it checks if file
%% exists. If not, it creates file.
%% @end
-spec create(Storage_helper_info :: record(), File :: string()) -> Result when
    Result :: ok | {ErrorGeneral, ErrorDetail},
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
create(Storage_helper_info, File) ->
    ok = setup_ctx(File),
    {ModeStatus, NewFileStorageMode} = get_mode(File),
    case ModeStatus of
        ok -> create(Storage_helper_info, File, NewFileStorageMode);
        _ -> {error, cannot_get_file_mode}
    end.

%% create/3
%% ====================================================================
%% @doc Creates file with given mode (operates only on storage). First it checks if file
%% exists. If not, it creates file.
%% @end
-spec create(Storage_helper_info :: record(), File :: string(), Mode :: integer()) -> Result when
    Result :: ok | {ErrorGeneral, ErrorDetail},
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
create(Storage_helper_info, File, Mode) ->
    ok = setup_ctx(File),
    {ErrorCode, Stat} = helpers:exec(getattr, Storage_helper_info, [File]),
    case ErrorCode of
        0 -> {error, file_exists};
        error -> {ErrorCode, Stat};
        _ ->
            ErrorCode2 = helpers:exec(mknod, Storage_helper_info, [File, Mode bor ?S_IFREG, 0]),
            case ErrorCode2 of
                0 ->
                    ErrorCode3 = helpers:exec(truncate, Storage_helper_info, [File, 0]),
                    case ErrorCode3 of
                        0 ->
                            derive_gid_from_parent(Storage_helper_info, File),

                            Query = fslogic_context:get_user_query(),

                            case Query of
                                undefined -> ok;
                                _ ->
                                    {GetUserAns, User} = user_logic:get_user(Query),
                                    case GetUserAns of
                                        ok ->
                                            {_Login, UID} = user_logic:get_login_with_uid(User),
                                            ChownAns = chown(Storage_helper_info, File, UID, -1),
                                            case ChownAns of
                                                ok ->
                                                    ok;
                                                _ ->
                                                    {cannot_change_file_owner, ChownAns}
                                            end;
                                        _ -> {cannot_change_file_owner, get_user_error}
                                    end
                            end;
                        {error, 'NIF_not_loaded'} -> ErrorCode3;
                        _ -> {wrong_truncate_return_code, ErrorCode3}
                    end;
                {error, 'NIF_not_loaded'} -> ErrorCode2;
                _ ->
                    ?error("Can not create file ~p, code: ~p, helper info: ~p, mode: ~p, CTX: ~p / ~p", [File, ErrorCode2, Storage_helper_info, Mode bor ?S_IFREG, fslogic_context:get_fs_user_ctx(), fslogic_context:get_fs_group_ctx()]),
                    {wrong_mknod_return_code, ErrorCode2}
            end
    end.

%% truncate/3
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
    ok = case get_cached_value(File, mode, Storage_helper_info) of
             {ok, Mask} when (Mask band (?RWE_USR_PERM bor ?RWE_GRP_PERM bor ?RWE_OTH_PERM)) == 0 ->
                 case has_permission(File, write) of
                     true -> set_root_ctx();
                     false -> setup_ctx(File)
                 end;
             _ -> setup_ctx(File)
         end,
    {ErrorCode, Stat} = get_cached_value(File, is_reg, Storage_helper_info),
    case ErrorCode of
        ok ->
            case Stat of
                true ->
                    ErrorCode2 = helpers:exec(truncate, Storage_helper_info, [File, Size]),
                    case ErrorCode2 of
                        0 -> ok;
                        {error, 'NIF_not_loaded'} -> ErrorCode2;
                        _ ->
                            {wrong_truncate_return_code, ErrorCode2}
                    end;
                false -> {error, not_regular_file}
            end;
        error -> {ErrorCode, Stat};
        _ -> {ErrorCode, Stat}
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
    ok = case get_cached_value(File, mode, Storage_helper_info) of
             {ok, Mask} when (Mask band (?RWE_USR_PERM bor ?RWE_GRP_PERM bor ?RWE_OTH_PERM)) == 0 ->
                 case has_permission(File, delete) of
                     true -> set_root_ctx();
                     false -> setup_ctx(File)
                 end;
             _ -> setup_ctx(File)
         end, {ErrorCode, Stat} = get_cached_value(File, is_reg, Storage_helper_info),
    case ErrorCode of
        ok ->
            case Stat of
                true ->
                    ErrorCode2 = helpers:exec(unlink, Storage_helper_info, [File]),
                    case ErrorCode2 of
                        0 -> clear_cache(File);
                        {error, 'NIF_not_loaded'} ->
                            clear_cache(File),
                            ErrorCode2;
                        _ ->
                            clear_cache(File),
                            {wrong_unlink_return_code, ErrorCode2}
                    end;
                false ->
                    clear_cache(File),
                    {error, not_regular_file}
            end;
        error ->
            clear_cache(File),
            {ErrorCode, Stat};
        _ ->
            clear_cache(File),
            {ErrorCode, Stat}
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
    {ErrorCode, Bytes} = helpers:exec(read, Storage_helper_info, [File, Size, Offset, FFI]),
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
    ErrorCode = helpers:exec(write, Storage_helper_info, [File, Buf, Offset, FFI]),
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
            {error, {wrong_write_return_code, ErrorCode}}
    end.


%% get_cached_value/3
%% ====================================================================
%% @doc Checks value using storage helper or gets its from cache
%% @end
-spec get_cached_value(File :: string(), ValueName :: atom(), Storage_helper_info :: record()) -> Result when
    Result :: {ok, Value} | {ErrorGeneral, ErrorDetail},
    Value :: term(),
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
get_cached_value(File, ValueName, Storage_helper_info) ->
    ValType = case ValueName of
                  is_reg -> file_stats;
                  is_dir -> file_stats;
                  grp_wr -> file_stats;
                  mode -> file_stats;
                  owner -> file_stats;
                  o_wronly -> flag;
                  o_rdonly -> flag;
                  size -> size
              end,

    CachedValue =
        case ValType of
            file_stats -> get({File, ValueName});
            flag -> get({Storage_helper_info, ValueName});
            _ -> undefined   %% size
        end,

    case CachedValue of
        undefined ->
            case ValType of
                file_stats ->
                    {ErrorCode, Stat} = case get({File, stats}) of
                                            undefined ->
                                                {TmpErrorCode, TmpStat} = helpers:exec(getattr, Storage_helper_info, [File]),
                                                case TmpErrorCode of
                                                    0 -> put({File, stats}, TmpStat);
                                                    _ -> ok
                                                end,
                                                {TmpErrorCode, TmpStat};
                                            StatsValue ->
                                                {0, StatsValue}
                                        end,
                    case ErrorCode of
                        0 ->
                            ReturnValue = case ValueName of
                                              grp_wr ->
                                                  case Stat#st_stat.st_mode band ?WR_GRP_PERM of
                                                      0 -> false;
                                                      _ -> true
                                                  end;
                                              mode ->
                                                  Stat#st_stat.st_mode;
                                              owner ->
                                                  integer_to_list(Stat#st_stat.st_uid);
                                              _ ->
                                                  helpers:exec(ValueName, Storage_helper_info, [Stat#st_stat.st_mode])
                                          end,
                            put({File, ValueName}, ReturnValue),
                            {ok, ReturnValue};
                        error -> {ErrorCode, Stat};
                        _ -> {wrong_getatt_return_code, ErrorCode}
                    end;
                flag ->
                    ReturnValue2 = helpers:exec(get_flag, Storage_helper_info, [ValueName]),
                    put({Storage_helper_info, ValueName}, ReturnValue2),
                    {ok, ReturnValue2};
                size ->
                    {ErrorCode2, Stat2} = helpers:exec(getattr, Storage_helper_info, [File]),
                    case ErrorCode2 of
                        0 ->
                            ReturnValue3 = helpers:exec(is_reg, Storage_helper_info, [Stat2#st_stat.st_mode]),
                            put({File, is_reg}, ReturnValue3),
                            {ok, {ReturnValue3, Stat2#st_stat.st_size}};
                        error -> {ErrorCode2, Stat2};
                        _ -> {wrong_getatt_return_code, ErrorCode2}
                    end
            end;
        _ -> {ok, CachedValue}
    end.


%% check_perms/2
%% ====================================================================
%% @doc Checks if the user has permission to modify file (e,g. change owner).
%% @end
-spec check_perms(File :: string(), Storage_helper_info :: record()) -> Result when
    Result :: {ok, Value} | {ErrorGeneral, ErrorDetail},
    Value :: boolean(),
    ErrorGeneral :: atom(),
    ErrorDetail :: atom().
%% ====================================================================
check_perms(File, Storage_helper_info) ->
    check_perms(File, Storage_helper_info, write).

%% check_perms/3
%% ====================================================================
%% @doc Checks if the user has permission to modify file (e,g. change owner).
%%      @todo: remove this function. Currently this functionality is provided by operating system via helpers
%% @end
-spec check_perms(File :: string(), Storage_helper_info :: record(), CheckType :: boolean()) -> Result when
    Result :: {ok, Value} | {ErrorGeneral, ErrorDetail},
    Value :: boolean(),
    ErrorGeneral :: atom(),
    ErrorDetail :: atom().
%% ====================================================================
check_perms(_File, _Storage_helper_info, _CheckType) ->
    {ok, true}.
%%   {AccessTypeStatus, AccessAns} = check_access_type(File),
%%   case AccessTypeStatus of
%%     ok ->
%%       {AccesType, AccessName} = AccessAns,
%%       case AccesType of
%%         user ->
%%           {UsrStatus, UserRoot} = fslogic_path:get_user_root(),
%%           case UsrStatus of
%%             ok ->
%%               {ok, UserDoc} = fslogic_objects:get_user(),
%%               fslogic_context:set_fs_user_ctx(UserDoc#db_document.record#user.login),
%%               [CleanUserRoot | _] = string:tokens(UserRoot, "/"),
%%               {ok, CleanUserRoot =:= AccessName};
%%             _ -> {error, can_not_get_user_root}
%%           end;
%%         group ->
%%           {UserDocStatus, UserDoc} = fslogic_objects:get_user(),
%%           {UsrStatus2, UserGroups} = fslogic_utils:get_user_groups(UserDocStatus, UserDoc),
%%           case UsrStatus2 of
%%             ok ->
%%               fslogic_context:set_fs_user_ctx(UserDoc#db_document.record#user.login),
%%               case lists:member(AccessName, UserGroups) of
%%                 true ->
%%                   fslogic_context:set_fs_group_ctx(AccessName),
%%                   case CheckType of
%%                     read ->
%%                       {ok, true};
%%                     _ ->
%%                       {Status, CheckOk} = case CheckType of
%%                                             write -> get_cached_value(File, grp_wr, Storage_helper_info);
%%                                             _ -> {ok, false} %perms
%%                                           end,
%%                       case Status of
%%                         ok ->
%%                           case CheckOk of
%%                             true -> {ok, true};
%%                             false ->
%%                               UserRecord = UserDoc#db_document.record,
%%                               IdFromSystem = fslogic_utils:get_user_id_from_system(UserRecord#user.login),
%%                               IdFromSystem2 = string:substr(IdFromSystem, 1, length(IdFromSystem) - 1),
%%                               {OwnWrStatus, Own} = get_cached_value(File, owner, Storage_helper_info),
%%                               case OwnWrStatus of
%%                                 ok ->
%%                                   {ok, IdFromSystem2 =:= Own};
%%                                 _ ->
%%                                   {error, can_not_check_file_owner}
%%                               end
%%                           end;
%%                         _ ->
%%                           {error, can_not_check_grp_perms}
%%                       end
%%                   end;
%%                 false ->
%%                   {ok, false}
%%               end;
%%             _ -> {error, can_not_get_user_groups}
%%           end
%%       end;
%%     _ ->
%%       {AccessTypeStatus, AccessAns}
%%   end.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% derive_gid_from_parent/2
%% ====================================================================
%% @doc Gets group owner form File's parent and sets same group owner for the File
%% @end
-spec derive_gid_from_parent(Storage_helper_info :: record(), File :: string()) -> ok | {error, ErrNo :: integer()}.
%% ====================================================================
derive_gid_from_parent(SHInfo, File) ->
    case helpers:exec(getattr, SHInfo, [fslogic_path:strip_path_leaf(File)]) of
        {0, #st_stat{st_gid = GID}} ->
            Res = chown(SHInfo, File, -1, GID),
            ?debug("Changing gid of file ~p to ~p. Status: ~p", [File, GID, Res]),
            Res;
        {ErrNo, _} ->
            ?error("Cannot fetch parent dir ~p attrs. Error: ~p", [fslogic_path:strip_path_leaf(File), ErrNo]),
            {error, ErrNo}
    end.

%% get_mode/1
%% ====================================================================
%% @doc Gets mode for a newly created file.
%% @end
-spec get_mode(FileName :: string()) -> Result when
    Result :: {ok, integer()} | {error, undefined}.
%% ====================================================================
get_mode(File) ->
    {AccessTypeStatus, AccesType} = check_access_type(File),
    case AccessTypeStatus of
        ok ->
            TmpAns = case AccesType of
                         {user, _} ->
                             application:get_env(?APP_Name, new_file_storage_mode);
                         {group, _} ->
                             application:get_env(?APP_Name, new_group_file_storage_mode)
                     end,
            case TmpAns of
                undefined -> {error, undefined};
                _ -> TmpAns
            end;
        _ ->
            case application:get_env(?APP_Name, new_file_storage_mode) of %% operation performed by cluster
                undefined -> {error, undefined};
                TmpAns2 -> TmpAns2
            end
    end.

%% check_access_type/1
%% ====================================================================
%% @doc Checks if the file belongs to user or group
%% @end
-spec check_access_type(FileName :: string()) -> Result when
    Result :: {ok, {Type, OwnerName}} | {error, ErrorDesc},
    Type :: atom(),
    OwnerName :: string(),
    ErrorDesc :: atom().
%% ====================================================================
check_access_type(File) ->
    FileTokens = string:tokens(File, "/"),
    FileTokensLen = length(FileTokens),
    case FileTokensLen > 2 of
        true ->
            case lists:nth(1, FileTokens) of
                "users" ->
                    {ok, {user, lists:nth(2, FileTokens)}};
                ?SPACES_BASE_DIR_NAME ->
                    {ok, {group, lists:nth(2, FileTokens)}};
                _ ->
                    {error, wrong_path_format}
            end;
        false ->
            {error, too_short_path}
    end.

%% set_root_ctx/0
%% ====================================================================
%% @doc Setups user filesystem context (uid and gid for helpers)
%%      as a root. Use with caution!
%% @end
-spec set_root_ctx() -> ok.
%% ====================================================================
set_root_ctx() ->
    fslogic_context:set_fs_root_user_ctx(),
    fslogic_context:set_fs_root_group_ctx(),
    ok.

%% setup_ctx/1
%% ====================================================================
%% @doc Setups user filesystem context (uid and gid for helpers)
%%      based on current fslogic user context and access path (group name -> primary GID)
%% @end
-spec setup_ctx(File :: string()) -> ok | {error, no_user}.
%% ====================================================================
setup_ctx(File) ->
    ?debug("Setup storage ctx based on user ctx: ~p", [fslogic_context:get_user_context()]),

    case fslogic_objects:get_user() of
        {ok, #db_document{record = #user{global_id = GRUID} = UserRec} = UserDoc} ->
            {_Login, UID} = user_logic:get_login_with_uid(UserDoc),
            fslogic_context:set_fs_user_ctx(UID),
            case check_access_type(File) of
                {ok, {group, SpaceId}} ->
                    UserSpaceIds = user_logic:get_space_ids(UserRec),
                    SelectedSpaceId = [X || X <- UserSpaceIds, utils:ensure_binary(SpaceId) =:= X],
                    SelectedSpaceIdOrSpace =
                        case SelectedSpaceId of
                            [] ->
                                UserSpaces0 =
                                    case dao_lib:apply(vfs, get_space_files, [{gruid, utils:ensure_binary(GRUID)}], fslogic_context:get_protocol_version()) of
                                        {ok, SpaceFiles} ->
                                            [fslogic_utils:file_to_space_info(SpaceFile) || #db_document{record = #file{}} = SpaceFile <- SpaceFiles];
                                        _ ->
                                            []
                                    end,
                                SelectedSpace0 = [SP || #space_info{space_id = X} = SP <- UserSpaces0, utils:ensure_binary(SpaceId) =:= X],
                                SelectedSpace0;
                            _ ->
                                SelectedSpaceId
                        end,

                    SelectedSpace =
                        case SelectedSpaceIdOrSpace of
                            [] -> [];
                            [MSpaceId | _] when is_binary(MSpaceId) ->
                                {ok, SelectedSpace1} = fslogic_objects:get_space({uuid, MSpaceId}),
                                [SelectedSpace1];
                            [#space_info{} = SpaceInfo1 | _] ->
                                [SpaceInfo1]
                        end,

                    GIDs =
                        case SelectedSpace of
                            [] ->
                                [];
                            [#space_info{} = SpaceInfo] ->
                                [fslogic_spaces:map_to_grp_owner(SpaceInfo)]
                        end,
                    fslogic_context:set_fs_group_ctx(GIDs),
                    ok;
                _ ->
                    fslogic_context:set_fs_group_ctx([]),
                    ok
            end;
        _ ->
            {error, no_user}
    end.


%% clear_cache/1
%% ====================================================================
%% @doc Clears caches connected with file.
%% @end
-spec clear_cache(File :: string()) -> ok.
clear_cache(File) ->
    erase({File, is_reg}),
    erase({File, grp_wr}),
    erase({File, is_dir}),
    erase({File, owner}),
    erase({File, stats}),
    ok.

%% has_permission/1
%% ====================================================================
%% @doc Checks file permission in cache
%% @end
-spec has_permission(StorageFilePath :: string(), PermissionAtom :: create | delete | read | write | execute) -> boolean() | {error, timeout}.
has_permission(StorageFilePath, PermissionAtom) ->
    {ok, #db_document{record = #user{global_id = GRUID}}} = fslogic_objects:get_user(),
    MsgID = make_ref(),
    gen_server:call(?Dispatcher_Name, {fslogic, fslogic_context:get_protocol_version(), self(), MsgID, {has_permission, StorageFilePath, utils:ensure_binary(GRUID), PermissionAtom}}, ?CACHE_REQUEST_TIMEOUT),
    receive
        {worker_answer, MsgID, {ok, Resp}} -> Resp
    after ?CACHE_REQUEST_TIMEOUT -> {error, timeout}
    end.
