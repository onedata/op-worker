%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module exports utility tools for fslogic
%% @end
%% ===================================================================
-module(fslogic_utils).

-include("veil_modules/dao/dao.hrl").
-include("veil_modules/dao/dao_types.hrl").
-include("veil_modules/fslogic/fslogic.hrl").
-include("fuse_messages_pb.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([create_children_list/1, create_children_list/2, get_user_id_from_system/1]).
-export([get_sh_and_id/3, get_sh_and_id/4, get_sh_and_id/5, get_files_number/3]).
-export([get_space_info_for_path/1, get_user_groups/2]).
-export([random_ascii_lowercase_sequence/1, path_walk/3, list_dir/1]).
-export([run_as_root/1, file_to_space_info/1, gen_storage_uid/1]).


%% ====================================================================
%% API functions
%% ====================================================================


%% gen_storage_uid/1
%% ====================================================================
%% @doc Generates storage UID/GID based arbitrary binary (e.g. user's global id, space id, etc)
%% @end
-spec gen_storage_uid(ID :: binary()) -> non_neg_integer().
%% ====================================================================
gen_storage_uid(ID) ->
    <<GID0:16/big-unsigned-integer-unit:8>> = crypto:hash(md5, ID),
    {ok, LowestGID} = veil_cluster_node_app:get_env(lowest_generated_storage_gid),
    LowestGID + GID0 rem 1000000.


%% file_to_space_info/1
%% ====================================================================
%% @doc Extracts space_info() from file_doc(). If given file is not an space's root file, fails with error:{badarg, file_info()}.
%% @end
-spec file_to_space_info(SpaceFile :: file_doc() | file_info()) -> space_info() | no_return().
%% ====================================================================
file_to_space_info(#veil_document{record = #file{} = File}) ->
    file_to_space_info(File);
file_to_space_info(#file{extensions = Exts} = File) ->
    case lists:keyfind(?file_space_info_extestion, 1, Exts) of
        {?file_space_info_extestion, #space_info{} = SpaceInfo} ->
            SpaceInfo;
        _ ->
            error({badarg, File})
    end.


%% run_as_root/1
%% ====================================================================
%% @doc Runs given function as root.
%% @end
-spec run_as_root(Fun :: function()) -> Result :: term().
%% ====================================================================
run_as_root(Fun) ->
    %% Save user context
    DN_CTX = fslogic_context:get_user_dn(),
    {AT_CTX1, AT_CTX2} = fslogic_context:get_gr_auth(),

    %% Clear user context
    fslogic_context:clear_user_ctx(),

    Result = Fun(),

    %% Restore user context
    fslogic_context:set_user_dn(DN_CTX),
    fslogic_context:set_gr_auth(AT_CTX1, AT_CTX2),

    Result.


%% path_walk/3
%% ====================================================================
%% @doc Executes given function for each file which path starts with StartPath. This function behaves very similar to
%%      lists:foldl/3 only that instead list iteration, recursive path walk is made. File order is unspecified.
%% @end
-spec path_walk(StartPath :: path(), InitAcc :: [term()],
    Fun :: fun((SubPath :: path(), FileType :: file_type_protocol(), AccIn :: [term()]) -> [term()])) ->
    AccOut :: [term()] | no_return().
%% ====================================================================
path_walk(Path, InitAcc, Fun) ->
    {ok, #fileattributes{type = FileType}} = logical_files_manager:getfileattr(Path),
    path_walk4(Path, FileType, InitAcc, Fun).
path_walk4(Path, ?DIR_TYPE_PROT = FileType, Acc, Fun) ->
    Acc1 = Fun(Path, FileType, Acc),
    lists:foldl(
        fun(#dir_entry{name = Elem, type = ElemType}, IAcc) ->
            ElemPath = filename:join(Path, Elem),
            path_walk4(ElemPath, ElemType, IAcc, Fun)
        end, Acc1, list_dir(Path));
path_walk4(Path, FileType, Acc, Fun) ->
    Fun(Path, FileType, Acc).


%% list_dir/1
%% ====================================================================
%% @doc Returns all dir's entries.
%% @end
-spec list_dir(Path :: path()) -> [#dir_entry{}] | no_return().
%% ====================================================================
list_dir(Path) ->
    BatchSize = 100,
    list_dir4(Path, 0, BatchSize, []).
list_dir4(Path, Offset, BatchSize, Acc) ->
    {ok, Childern} = logical_files_manager:ls(Path, BatchSize, Offset),
    case length(Childern) < BatchSize of
        true  -> Childern ++ Acc;
        false ->
            list_dir4(Path, Offset + length(Childern), BatchSize, Childern ++ Acc)
    end.

%% get_sh_and_id/3
%% ====================================================================
%% @doc Returns storage helper info and new file id (it may be changed for Cluster Proxy).
%% @end
-spec get_sh_and_id(FuseID :: string(), Storage :: term(), File_id :: string()) -> Result when
    Result :: {SHI, NewFileId},
    SHI :: term(),
    NewFileId :: string().
%% ====================================================================
get_sh_and_id(FuseID, Storage, File_id) ->
    get_sh_and_id(FuseID, Storage, File_id, <<>>).
get_sh_and_id(FuseID, Storage, File_id, SpaceId) ->
    get_sh_and_id(FuseID, Storage, File_id, SpaceId, false).
get_sh_and_id(FuseID, Storage, File_id, SpaceId, ForceClusterProxy) ->
    SHI =
        case ForceClusterProxy orelse fslogic_context:is_global_fuse_id(FuseID) of
            true ->
                #storage_helper_info{name = "ClusterProxy", init_args = []};
            false ->
                fslogic_storage:get_sh_for_fuse(FuseID, Storage)
        end,
    #storage_helper_info{name = SHName, init_args = _SHArgs} = SHI,
    case SHName =:= "ClusterProxy" of
        true ->
            {SHI#storage_helper_info{init_args = [binary_to_list(vcn_utils:ensure_binary(SpaceId))]},
                fslogic_path:absolute_join([integer_to_list(Storage#storage_info.id) | filename:split(File_id)])};
        false -> {SHI, File_id}
    end.

%% create_children_list/1
%% ====================================================================
%% @doc Creates list of children logical names on the basis of list with
%% veil_documents that describe children.
%% @end
-spec create_children_list(Files :: list()) -> Result when
  Result :: term().
%% ====================================================================

create_children_list(Files) ->
  create_children_list(Files, []).

%% create_children_list/2
%% ====================================================================
%% @doc Creates list of children logical names on the basis of list with
%% veil_documents that describe children.
%% @end
-spec create_children_list(Files :: list(), TmpAns :: list()) -> Result when
  Result :: term().
%% ====================================================================

create_children_list([], Ans) ->
  Ans;

create_children_list([File | Rest], Ans) ->
    FileDesc = File#veil_document.record,
    Name = FileDesc#file.name,
    Type = fslogic_file:normalize_file_type(protocol, FileDesc#file.type),
    create_children_list(Rest, [#filechildren_direntry{name = Name, type = Type} | Ans]).


%% random_ascii_lowercase_sequence
%% ====================================================================
%% @doc Create random sequence consisting of lowercase ASCII letters.
-spec random_ascii_lowercase_sequence(Length :: integer()) -> list().
%% ====================================================================
random_ascii_lowercase_sequence(Length) ->
    lists:foldl(fun(_, Acc) -> [random:uniform(26) + 96 | Acc] end, [], lists:seq(1, Length)).


%% get_space_info_for_path/1
%% ====================================================================
%% @doc Returns #space_info{} associated with given file path.
%% @end
-spec get_space_info_for_path(FileBasePath :: string()) -> {ok, #space_info{}} | {error, Reason :: term()}.
%% ====================================================================
get_space_info_for_path([$/ | FileBasePath]) ->
    get_space_info_for_path(FileBasePath);
get_space_info_for_path(FileBasePath) ->
    case filename:split(FileBasePath) of
        [?SPACES_BASE_DIR_NAME, SpaceName | _] ->
            ?debug("Attempting to fetch space ~p while resolving file path ~p", [SpaceName, FileBasePath]),
            case fslogic_objects:get_space(SpaceName) of
                {ok, #space_info{} = SP} -> {ok, SP};
                {error, Reason} ->
                    {error, {invalid_space_path, Reason}}
            end;
        _ -> {ok, #space_info{name = "root", space_id = "", providers = [cluster_manager_lib:get_provider_id()]}}
    end.


%% get_files_number/3
%% ====================================================================
%% @doc Returns number of user's or group's files
%% @end
-spec get_files_number(user | group, UUID :: uuid() | string(), ProtocolVersion :: integer()) -> Result when
    Result :: {ok, Sum} | {error, any()},
    Sum :: integer().
%% ====================================================================
get_files_number(Type, GroupName, ProtocolVersion) ->
    Ans = dao_lib:apply(dao_users, get_files_number, [Type, GroupName], ProtocolVersion),
    case Ans of
        {error, files_number_not_found} -> {ok, 0};
        _ -> Ans
    end.


%% get_user_groups/0
%% ====================================================================
%% @doc Gets user's group
%% @end
-spec get_user_groups(UserDocStatus :: atom(), UserDoc :: term()) -> Result when
    Result :: {ok, Groups} | {error, ErrorDesc},
    Groups :: list(),
    ErrorDesc :: atom.
%% ====================================================================

get_user_groups(UserDocStatus, UserDoc) ->
    case UserDocStatus of
        ok ->
            {ok, user_logic:get_space_names(UserDoc)};
        _ ->
            {error, UserDoc}
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================


%% get_user_id_from_system/1
%% ====================================================================
%% @doc Returns id of user in local system.
%% @end
-spec get_user_id_from_system(User :: string()) -> string().
%% ====================================================================
get_user_id_from_system(User) ->
  os:cmd("id -u " ++ User).


