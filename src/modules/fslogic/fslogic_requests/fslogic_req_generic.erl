%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc FSLogic generic request handlers.
%%% @end
%%%-------------------------------------------------------------------
-module(fslogic_req_generic).
-author("Rafal Slota").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([chmod/3, get_attrs/2, delete_file/2, rename_file/3]).

%%--------------------------------------------------------------------
%% API functions
%%--------------------------------------------------------------------


%%--------------------------------------------------------------------
%% @doc Changes file permissions.
%% @end
%%--------------------------------------------------------------------
-check_permissions({owner, 2}).
-spec chmod(fslogic:ctx(), File :: fslogic:file(), Perms :: non_neg_integer()) ->
    #atom{} | no_return().
chmod(_, _File, _Mode) ->
    ?NOT_IMPLEMENTED.


%%--------------------------------------------------------------------
%% @doc Gets file's attributes.
%% @end
%%--------------------------------------------------------------------
-spec get_attrs(fslogic:ctx(), File :: fslogic:file()) ->
    #fileattr{} | no_return().
get_attrs(_, _File) ->
    ?NOT_IMPLEMENTED.


%%--------------------------------------------------------------------
%% @doc Deletes file.
%% @end
%%--------------------------------------------------------------------
-check_permissions({write, {parent, 2}}).
-spec delete_file(fslogic:ctx(), File :: fslogic:file()) ->
    #atom{} | no_return().
delete_file(_, _File) ->
    ?NOT_IMPLEMENTED.


%%--------------------------------------------------------------------
%% @doc Renames file.
%% @end
%%--------------------------------------------------------------------
-check_permissions([{write, {parent, {path, 2}}}, {write, {parent, {path, 3}}}]).
-spec rename_file(fslogic:ctx(), SourcePath :: file:path(), TargetPath :: file:path()) ->
    #atom{} | no_return().
rename_file(_, _SourcePath, _TargetPath) ->
    ?NOT_IMPLEMENTED.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
