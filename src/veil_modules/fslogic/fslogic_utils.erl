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

-include("files_common.hrl").
-include("veil_modules/dao/dao.hrl").

%% API
-export([strip_path_leaf/1, basename/1, get_parent_and_name_from_path/2, create_children_list/1, create_children_list/2]).

%% ====================================================================
%% API functions
%% ====================================================================

%% strip_path_leaf/1
%% ====================================================================
%% @doc Strips file name from path
-spec strip_path_leaf(Path :: string()) -> string().
%% ==================================================================
strip_path_leaf(Path) when is_list(Path) ->
    strip_path_leaf({split, lists:reverse(string:tokens(Path, [?PATH_SEPARATOR]))});
strip_path_leaf({split, []}) -> [?PATH_SEPARATOR];
strip_path_leaf({split, [_ | Rest]}) ->
    [?PATH_SEPARATOR] ++ string:join(lists:reverse(Rest), [?PATH_SEPARATOR]).


%% basename/1
%% ====================================================================
%% @doc Gives file basename from given path
-spec basename(Path :: string()) -> string().
%% ==================================================================
basename(Path) ->
    case lists:reverse(string:tokens(Path, [?PATH_SEPARATOR])) of
        [Leaf | _] -> Leaf;
        _ -> [?PATH_SEPARATOR]
    end.

%% get_parent_and_name_from_path/2
%% ====================================================================
%% @doc Gets parent uuid and file name on the basis of absolute path.
%% @end
-spec get_parent_and_name_from_path(Path :: string(), ProtocolVersion :: term()) -> Result when
  Result :: tuple().
%% ====================================================================

get_parent_and_name_from_path(Path, ProtocolVersion) ->
  File = fslogic_utils:basename(Path), 
  Parent = fslogic_utils:strip_path_leaf(Path),
  case Parent of
    [?PATH_SEPARATOR] -> {ok, {File, ""}};
    _Other ->
      {Status, TmpAns} = dao_lib:apply(dao_vfs, get_file, [Parent], ProtocolVersion),
      case Status of
        ok -> {ok, {File, TmpAns#veil_document.uuid}};
        _BadStatus ->
          lager:error([{mod, ?MODULE}], "Error: cannot find parent for path: ~s", [Path]),
          {error, "Error: cannot find parent: " ++ TmpAns}
      end
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