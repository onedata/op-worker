%% ===================================================================
%% @author Piotr Ociepka
%% @copyright (C): 2015 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module provides convinience functions designed for
%% REST handling modules.
%% @end
%% ===================================================================
-module(rest_utils).
-author("Piotr Ociepka").

%% API
-export([ensure_path_ends_with_slash/1, get_path_leaf_with_ending_slash/1]).

%% ====================================================================
%% @doc Appends '/' to the end of filepath if last character is different
%% ====================================================================
-spec ensure_path_ends_with_slash(string()) -> string().
ensure_path_ends_with_slash([]) ->
  "/";
ensure_path_ends_with_slash(Path) ->
  case lists:last(Path) of
    $/ -> Path;
    _ -> Path ++ "/"
  end.

%% ====================================================================
%% @doc Get filepath leaf with '/' at the end
%% ====================================================================
-spec get_path_leaf_with_ending_slash(string()) -> string().
get_path_leaf_with_ending_slash(Path) ->
  BaseName = fslogic_path:basename(list_to_binary(Path)),
  ensure_path_ends_with_slash(binary_to_list(BaseName)).
