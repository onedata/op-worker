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

-include("types.hrl").
-include("errors.hrl").

%% API
-export([create_share/2, get_share/1, remove_share/1]).

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
-spec create_share(FileKey :: file_key(), ShareWith :: all | [{user, user_id()} | {group, group_id()}]) ->
    {ok, ShareID :: share_id()} | error_reply().
create_share(_Path, _ShareWith) ->
    {ok, <<"">>}.


%%--------------------------------------------------------------------
%% @doc
%% Returns shared file by share_id.
%%
%% @end
%%--------------------------------------------------------------------
-spec get_share(ShareID :: share_id()) -> {ok, {file_uuid(), file_name()}} | error_reply().
get_share(_ShareID) ->
    {ok, {<<"">>, <<"">>}}.


%%--------------------------------------------------------------------
%% @doc
%% Removes file share by ShareID.
%%
%% @end
%%--------------------------------------------------------------------
-spec remove_share(ShareID :: share_id()) -> ok | error_reply().
remove_share(_ShareID) ->
    ok.
