%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%% @doc This module performs links-related operations of lfm_submodules.
%%% @end
%%%-------------------------------------------------------------------
-module(lfm_links).

-include("types.hrl").
-include("errors.hrl").

%% API
-export([create_symlink/2, read_symlink/1, remove_symlink/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates a symbolic link.
%%
%% @end
%%--------------------------------------------------------------------
-spec create_symlink(Path :: binary(), TargetFileKey :: file_key()) -> {ok, file_uuid()} | error_reply().
create_symlink(_Path, _TargetFileKey) ->
    {ok, <<"">>}.


%%--------------------------------------------------------------------
%% @doc
%% Returns the symbolic link's target file.
%%
%% @end
%%--------------------------------------------------------------------
-spec read_symlink(FileKey :: file_key()) -> {ok, {file_uuid(), file_name()}} | error_reply().
read_symlink(_FileKey) ->
    {ok, {<<"">>, <<"">>}}.


%%--------------------------------------------------------------------
%% @doc
%% Removes a symbolic link.
%%
%% @end
%%--------------------------------------------------------------------
-spec remove_symlink(FileKey :: file_key()) -> ok | error_reply().
remove_symlink(_FileKey) ->
    ok.
