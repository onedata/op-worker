%%%-------------------------------------------------------------------
%%% @author RoXeon
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 14. Jul 2014 23:56
%%%-------------------------------------------------------------------
-module(registry_spaces).
-author("RoXeon").

-include("veil_modules/dao/dao_vfs.hrl").
-include_lib("veil_modules/dao/dao.hrl").
-include_lib("veil_modules/dao/dao_types.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([get_user_spaces/1, get_space_info/2, get_space_providers/1]).


%% ====================================================================
%% API functions
%% ====================================================================

get_user_spaces({UserGID, AccessToken}) ->
    case user_logic:get_user({global_id, UserGID}) of
        {ok, UserDoc} ->
            #veil_document{record = #user{spaces = Spaces}} = user_logic:synchronize_spaces_info(UserDoc, AccessToken),
            {ok, Spaces};
        {error, Reason} ->
            {error, Reason}
    end.

get_space_info(SpaceId, {UserGID, AccessToken}) ->
    ?info("get_space_info ~p ~p", [SpaceId, {UserGID, AccessToken}]),
    get_space_info(global_registry:user_request(AccessToken, get, "spaces/" ++ SpaceId));
get_space_info(SpaceId, _) ->
    get_space_info(global_registry:provider_request(get, "spaces/" ++ SpaceId)).
get_space_info({ok, Response}) ->
    ?info("Resp: ~p", [Response]),
    #{<<"name">> := SpaceName, <<"spaceId">> := SpaceId0} = Response,
    SpaceId = binary_to_list(SpaceId0),
    {ok, Providers} = registry_spaces:get_space_providers(SpaceId),
    A = {ok, #space_info{uuid = SpaceId, name = unicode:characters_to_list(SpaceName), providers = Providers}},
    ?info("A =====> ~p", [A]),
    A;
get_space_info({error, Reason}) ->
    {error, Reason}.


get_space_providers(SpaceId) ->
    case global_registry:provider_request(get, "spaces/" ++ SpaceId ++ "/providers") of
        {ok, Response} ->
            ?info("Resp: ~p", [Response]),
            #{<<"providers">> := Providers} = Response,
            {ok, Providers};
        {error, Reason} ->
            {error, Reason}
    end.


%% ====================================================================
%% Internal functions
%% ====================================================================

