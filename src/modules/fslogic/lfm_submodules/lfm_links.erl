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

-include_lib("ctool/include/posix/errors.hrl").

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
-spec create_symlink(Path :: binary(), TargetFileKey :: file_meta:key()) ->
    {ok, file_meta:uuid()} | logical_file_manager:error_reply().
create_symlink(_Path, _TargetFileKey) ->
    {ok, <<"">>}.


%%--------------------------------------------------------------------
%% @doc
%% Returns the symbolic link's target file.
%%
%% @end
%%--------------------------------------------------------------------
-spec read_symlink(FileKey :: file_meta:key()) ->
    {ok, {file_meta:uuid(), file_meta:name()}} | logical_file_manager:error_reply().
read_symlink(_FileKey) ->
    {ok, {<<"">>, <<"">>}}.


%%--------------------------------------------------------------------
%% @doc
%% Removes a symbolic link.
%%
%% @end
%%--------------------------------------------------------------------
-spec remove_symlink(FileKey :: file_meta:key()) ->
    ok | logical_file_manager:error_reply().
remove_symlink(_FileKey) ->
    ok.
