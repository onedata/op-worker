%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: Low level BigCouch DB API
%% @end
%% ===================================================================
-module(dao_helper).

-include_lib("veil_modules/dao/couch_db.hrl").

-ifdef(TEST).
-compile([export_all]).
-endif.

%% API
-export([]).

%% ===================================================================
%% API functions
%% ===================================================================

    
%% ===================================================================
%% Internal functions
%% ===================================================================

name(Name) when is_list(Name) ->
    ?l2b(Name);
name(Name) when is_binary(Name) ->
    Name.
