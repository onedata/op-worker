%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2014, ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: Utils functions for DAO module
%% @end
%% ===================================================================
-module(dao_utils).
-author("Rafal Slota").

%% API
-export([get_versioned_view_name/2]).

%% ====================================================================
%% API functions
%% ====================================================================

get_versioned_view_name(Name, Version) ->
    Name ++ "_v" ++ integer_to_list(Version).

%% ====================================================================
%% Internal functions
%% ====================================================================
