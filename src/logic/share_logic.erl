%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Interface to provider's share cache.
%%% Operations may involve interactions with OZ api
%%% or cached records from the datastore.
%%% @end
%%%-------------------------------------------------------------------
-module(share_logic).
-author("Lukasz Opiola").

-include("proto/common/credentials.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/oz/oz_spaces.hrl").

-export([get/2, create/5, set_name/3, delete/4]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Retrieves share document.
%% Returned document contains parameters tied to given user
%% (as space name may differ from user to user).
%% Provided client should be authorised to access user details.
%% @end
%%--------------------------------------------------------------------
-spec get(oz_endpoint:auth(), ShareId :: binary()) ->
    {ok, datastore:document()} | datastore:get_error().
get(Auth, ShareId) ->
    share_info:get_or_fetch(Auth, ShareId).


%%--------------------------------------------------------------------
%% @doc
%% Creates a new share. ShareId must be given -
%% it is generated by the client side.
%% @end
%%--------------------------------------------------------------------
-spec create(oz_endpoint:auth(), ShareId :: share_info:id(), Name :: share_info:name(),
    ParentSpaceId :: space_info:id(), RootFileId :: share_info:share_guid()) ->
    {ok, share_info:id()} | {error, Reason :: term()}.
create(Auth, ShareId, Name, ParentSpaceId, ShareFileGuid) ->
    Parameters = [
        {<<"name">>, Name},
        {<<"root_file_id">>, ShareFileGuid}
    ],
    oz_shares:create(Auth, ShareId, ParentSpaceId, Parameters).


%%--------------------------------------------------------------------
%% @doc
%% Sets name for an user.
%% User identity is determined using provided auth.
%% @end
%%--------------------------------------------------------------------
-spec set_name(oz_endpoint:auth(), SpaceId :: binary(), NewName :: binary()) ->
    ok | {error, Reason :: term()}.
set_name(Auth, ShareId, NewName) ->
    oz_shares:modify_details(Auth, ShareId, [{<<"name">>, NewName}]).


%%--------------------------------------------------------------------
%% @doc
%% Delete given share.
%% @end
%%--------------------------------------------------------------------
-spec delete(oz_endpoint:auth(), space_info:id(), share_info:id(), file_meta:uuid()) ->
    ok | {error, Reason :: term()}.
delete(Auth, _ParentSpaceId, ShareId, _FileUuid) ->
    oz_shares:remove(Auth, ShareId).
