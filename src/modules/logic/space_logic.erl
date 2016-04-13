%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(space_logic).
-author("Michal Zmuda").

-include("proto/common/credentials.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/oz/oz_spaces.hrl").

-export([get/2, create/2, set_default/2, delete/2, set_name/3, set_user_privileges/4]).

-spec get(SessId :: binary(), SpaceId :: binary()) ->
    {ok, datastore:document()} | datastore:get_error().
get(SessId, SpaceId) ->
    space_info:get_or_fetch(SessId, SpaceId).

-spec create(Auth :: #auth{}, #space_info{}) ->
    {ok, SpaceId :: binary()} | {error, Reason :: term()}.
create(Auth, Record) ->
    Name = Record#space_info.name,
    oz_users:create_space(Auth, [{<<"name">>, Name}]).

-spec delete(Auth :: #auth{}, SpaceId :: binary()) ->
    ok | {error, Reason :: term()}.
delete(Auth, SpaceId) ->
    oz_spaces:remove(Auth, SpaceId).



set_default(Auth, SpaceId) ->
    oz_users:set_default_space(Auth, [{<<"spaceId">>, SpaceId}]).

set_name(Auth, SpaceId, Value) ->
    oz_spaces:modify_details(Auth, SpaceId, [{<<"name">>, Value}]).

set_user_privileges(Auth, SpaceId, UserId, Value) ->
    oz_spaces:set_user_privileges(Auth, SpaceId, UserId, [
        % usort - make sure there are no duplicates
        {<<"privileges">>, lists:usort(Value)}
    ]).
