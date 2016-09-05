%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%% @doc This module performs shares-related operations of lfm_submodules.
%%% @end
%%%-------------------------------------------------------------------
-module(lfm_shares).

-include_lib("ctool/include/posix/errors.hrl").

-type share_id() :: binary().

-export_type([share_id/0]).

%% API
-export([create_share/1, remove_share/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates a share for given file. File can be shared with anyone or
%% only specified group of users.
%%
%% @end
%%--------------------------------------------------------------------
-spec create_share(FileKey :: logical_file_manager:file_key()) ->
    {ok, ShareID :: share_id()} | logical_file_manager:error_reply().
create_share(_FileKey) ->
    {ok, <<"">>}.

%%--------------------------------------------------------------------
%% @doc
%% Removes file share by ShareID.
%%
%% @end
%%--------------------------------------------------------------------
-spec remove_share(ShareID :: share_id()) ->
    ok | logical_file_manager:error_reply().
remove_share(_ShareID) ->
    ok.
