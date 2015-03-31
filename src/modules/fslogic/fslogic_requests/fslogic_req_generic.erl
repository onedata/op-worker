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


%% change_file_perms/2
%%--------------------------------------------------------------------
%% @doc Changes file permissions.
%% @end
-spec chmod(fslogic:ctx(), File :: fslogic:file(), Perms :: non_neg_integer()) ->
    #atom{} | no_return().
%%--------------------------------------------------------------------
chmod(_, _File, _Mode) ->
    ?NOT_IMPLEMENTED.


%% get_file_attr/2
%%--------------------------------------------------------------------
%% @doc Gets file's attributes.
%% @end
-spec get_attrs(fslogic:ctx(), File :: fslogic:file()) ->
    #fileattr{} | no_return().
%%--------------------------------------------------------------------
get_attrs(_, _File) ->
    ?NOT_IMPLEMENTED.


%% delete_file/1
%%--------------------------------------------------------------------
%% @doc Deletes file.
%% @end
-spec delete_file(fslogic:ctx(), File :: fslogic:file()) ->
    #atom{} | no_return().
%%--------------------------------------------------------------------
delete_file(_, _File) ->
    ?NOT_IMPLEMENTED.


%% rename_file/2
%%--------------------------------------------------------------------
%% @doc Renames file.
%% @end
-spec rename_file(fslogic:ctx(), SourcePath :: file:path(), TargetPath :: file:path()) ->
    #atom{} | no_return().
%%--------------------------------------------------------------------
rename_file(_, _SourcePath, _TargetPath) ->
    ?NOT_IMPLEMENTED.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
