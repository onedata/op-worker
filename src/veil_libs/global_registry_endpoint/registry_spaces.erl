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

-include_lib("ctool/include/logging.hrl").
-include("veil_modules/dao/dao_vfs.hrl").

%% API
-export([get_space_info/2, get_space_providers/1]).


%% ====================================================================
%% API functions
%% ====================================================================


get_space_info(SpaceId, {UserGID, AccessToken}) ->
    ?info("get_space_info ~p ~p", [SpaceId, {UserGID, AccessToken}]),
    get_space_info(global_registry:user_request(AccessToken, get, <<"spaces/", (vcn_utils:ensure_binary(SpaceId))/binary>>));
get_space_info(SpaceId, _) ->
    get_space_info(global_registry:provider_request(get, <<"spaces/", (vcn_utils:ensure_binary(SpaceId))/binary>>)).
get_space_info({ok, Response}) ->
    ?info("Resp: ~p", [Response]),
    #{<<"name">> := SpaceName, <<"spaceId">> := SpaceId0} = Response,
    SpaceId = vcn_utils:ensure_binary(SpaceId0),
    {ok, Providers} = registry_spaces:get_space_providers(SpaceId),
    {ok, #space_info{space_id = SpaceId, name = SpaceName, providers = Providers}};
get_space_info({error, Reason}) ->
    {error, Reason}.


get_space_providers(SpaceId) ->
    case global_registry:provider_request(get, <<"spaces/", (vcn_utils:ensure_binary(SpaceId))/binary, "/providers">>) of
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

