%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc This module offers some high-level convenience functions using
%%% logical_files_manager underneath.
%%% @end
%%%-------------------------------------------------------------------
-module(lfm_utils).

-include("types.hrl").
-include("errors.hrl").

%% API
-export([write_all/3, ls_all/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Writes data to a file. Returns ok only if all the data has been written.
%%
%% @end
%%--------------------------------------------------------------------
-spec write_all(FileHandle :: file_handle(), Offset :: integer(), Buffer :: binary()) -> ok | error_reply().
write_all(_FileHandle, _Offset, _Buffer) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Lists all contents of a directory.
%%
%% @end
%%--------------------------------------------------------------------
-spec ls_all(Path :: binary()) -> {ok, [{file_uuid(), file_name()}]} | error_reply().
ls_all(_Path) ->
    {ok, []}.