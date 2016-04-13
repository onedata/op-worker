%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Interface between provider and spaces.
%%% Operations may involve interactions with OZ api
%%% or cached records from the datastore.
%%% @end
%%%-------------------------------------------------------------------
-module(space_logic).
-author("Michal Zmuda").

-include("proto/common/credentials.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/oz/oz_spaces.hrl").

-export([get/3, create_user_space/2, set_default/2, delete/2,
    set_name/3, set_user_privileges/4]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Retrieves space document.
%% Returned document contains parameters tied to given user
%% (as space name may differ from user to user).
%% Provided auth be authorised to access user details.
%% @end
%%--------------------------------------------------------------------
-spec get(Auth :: #auth{}, SpaceId :: binary(), UserId :: binary()) ->
    {ok, datastore:document()} | datastore:get_error().
get(Auth, SpaceId, UserId) ->
    #auth{macaroon = Macaroon, disch_macaroons = DischMacaroons} = Auth,
    Client = {user, {Macaroon, DischMacaroons}},
    space_info:get_or_fetch(Client, SpaceId, UserId).

%%--------------------------------------------------------------------
%% @doc
%% Creates space in context of an user.
%% User identity is determined using provided auth.
%% @end
%%--------------------------------------------------------------------
-spec create_user_space(Auth :: #auth{}, #space_info{}) ->
    {ok, SpaceId :: binary()} | {error, Reason :: term()}.
create_user_space(Auth, Record) ->
    Name = Record#space_info.name,
    oz_users:create_space(Auth, [{<<"name">>, Name}]).

%%--------------------------------------------------------------------
%% @doc
%% Deletes space from the system.
%% @end
%%--------------------------------------------------------------------
-spec delete(Auth :: #auth{}, SpaceId :: binary()) ->
    ok | {error, Reason :: term()}.
delete(Auth, SpaceId) ->
    oz_spaces:remove(Auth, SpaceId).

%%--------------------------------------------------------------------
%% @doc
%% Sets space as default for an user.
%% User identity is determined using provided auth.
%% @end
%%--------------------------------------------------------------------
-spec set_default(Auth :: #auth{}, SpaceId :: binary()) ->
    ok | {error, Reason :: term()}.
set_default(Auth, SpaceId) ->
    oz_users:set_default_space(Auth, [{<<"spaceId">>, SpaceId}]).

%%--------------------------------------------------------------------
%% @doc
%% Sets name for an user.
%% User identity is determined using provided auth.
%% @end
%%--------------------------------------------------------------------
-spec set_name(Auth :: #auth{}, SpaceId :: binary(), Name :: binary()) ->
    ok | {error, Reason :: term()}.
set_name(Auth, SpaceId, Name) ->
    oz_spaces:modify_details(Auth, SpaceId, [{<<"name">>, Name}]).

%%--------------------------------------------------------------------
%% @doc
%% Sets space privileges for an user.
%% User identity is determined using provided auth.
%% @end
%%--------------------------------------------------------------------
-spec set_user_privileges(Auth :: #auth{}, SpaceId :: binary(),
    UserId :: binary(), Privileges :: [binary()]) ->
    ok | {error, Reason :: term()}.
set_user_privileges(Auth, SpaceId, UserId, Privileges) ->
    oz_spaces:set_user_privileges(Auth, SpaceId, UserId, [
        % usort - make sure there are no duplicates
        {<<"privileges">>, lists:usort(Privileges)}
    ]).
