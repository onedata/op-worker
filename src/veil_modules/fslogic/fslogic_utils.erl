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
-include("veil_modules/fslogic/fslogic.hrl").
-include("logging.hrl").

%% API
-export([create_children_list/1, create_children_list/2, get_user_id_from_system/1]).
-export([get_sh_and_id/3, get_files_number/3]).
-export([get_group_owner/1, get_user_groups/2]).
-export([random_ascii_lowercase_sequence/1]).


%% ====================================================================
%% API functions
%% ====================================================================


%% get_sh_and_id/3
%% ====================================================================
%% @doc Returns storage hel[per info and new file id (it may be changed for Cluster Proxy).
%% @end
-spec get_sh_and_id(FuseID :: string(), Storage :: term(), File_id :: string()) -> Result when
    Result :: {SHI, NewFileId},
    SHI :: term(),
    NewFileId :: string().
%% ====================================================================
get_sh_and_id(FuseID, Storage, File_id) ->
    SHI = fslogic_storage:get_sh_for_fuse(FuseID, Storage),
    #storage_helper_info{name = SHName, init_args = _SHArgs} = SHI,
    case SHName =:= "ClusterProxy" of
        true ->
            {SHI, integer_to_list(Storage#storage_info.id) ++ ?REMOTE_HELPER_SEPARATOR ++ File_id};
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
  create_children_list(Rest, [FileDesc#file.name | Ans]).

%% ====================================================================
%% Internal functions
%% ====================================================================

%% random_ascii_lowercase_sequence
%% ====================================================================
%% @doc Create random sequence consisting of lowercase ASCII letters.
-spec random_ascii_lowercase_sequence(Length :: integer()) -> list().
%% ====================================================================
random_ascii_lowercase_sequence(Length) ->
    lists:foldl(fun(_, Acc) -> [random:uniform(26) + 96 | Acc] end, [], lists:seq(1, Length)).

%% get_user_id_from_system/1
%% ====================================================================
%% @doc Returns id of user in local system.
%% @end
-spec get_user_id_from_system(User :: string()) -> string().
%% ====================================================================
get_user_id_from_system(User) ->
  os:cmd("id -u " ++ User).

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
            {ok, user_logic:get_team_names(UserDoc)};
        _ ->
            {error, UserDoc}
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

%% get_group_owner/1
%% ====================================================================
%% @doc Convinience method that returns list of group name(s) that are considered as default owner of file
%%      created with given path. E.g. when path like "/groups/gname/file1" is passed, the method will
%%      return ["gname"].
%% @end
-spec get_group_owner(FileBasePath :: string()) -> [string()].
%% ====================================================================
get_group_owner(FileBasePath) ->
    case string:tokens(FileBasePath, "/") of
        [?GROUPS_BASE_DIR_NAME, GroupName | _] -> [GroupName];
        _ -> []
    end.


