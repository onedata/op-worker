%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013, ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: FSLogic request handlers for special files.
%% @end
%% ===================================================================
-module(fslogic_req_special).
-author("Rafal Slota").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([mkdir/3, read_dir/4, link/3, read_link/2]).

%%--------------------------------------------------------------------
%% API functions
%%--------------------------------------------------------------------


%%--------------------------------------------------------------------
%% @doc Creates new directory.
%% @end
%%--------------------------------------------------------------------
-spec mkdir(fslogic:ctx(), Path :: file_meta:path(), Mode :: non_neg_integer()) ->
    no_return().
mkdir(_, _Path, _Mode) ->
    ?NOT_IMPLEMENTED.


%%--------------------------------------------------------------------
%% @doc Lists directory. Start with ROffset entity and limit returned list to RCount size.
%% For best performance use following arg types: document -> uuid -> path
%% @end
%%--------------------------------------------------------------------
-spec read_dir(fslogic:ctx(), File :: fslogic:file(), ROffset :: non_neg_integer(), RCount :: non_neg_integer()) ->
    no_return().
read_dir(_, _FIle, _ROffset, _RCount) ->
    ?NOT_IMPLEMENTED.


%%--------------------------------------------------------------------
%% @doc Creates new symbolic link.
%% @end
%%--------------------------------------------------------------------
-spec link(fslogic:ctx(), Path :: file_meta:path(), LinkValue :: binary()) ->
    no_return().
link(_, _File, LinkValue) ->
    ?NOT_IMPLEMENTED.


%%--------------------------------------------------------------------
%% @doc Gets value of symbolic link.
%% For best performance use following arg types: document -> uuid -> path
%% @end
%%--------------------------------------------------------------------
-spec read_link(fslogic:ctx(), File :: fslogic:file()) ->
    no_return().
read_link(_, _File) ->
    ?NOT_IMPLEMENTED.


%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
