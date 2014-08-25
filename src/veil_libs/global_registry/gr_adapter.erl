%% ===================================================================
%% @author Krzysztof Trzepla
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module contains project specific interaction functions
%% with Global Registry.
%% @end
%% ===================================================================
-module(gr_adapter).

-include("veil_modules/dao/dao.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/global_registry/gr_spaces.hrl").

%% API
-export([verify_client/2]).
-export([set_default_space/2, synchronize_user_spaces/1]).
-export([get_space_info/2, get_space_providers/2]).


%% set_default_space/2
%% ====================================================================
%% @doc Sets default user's Space.
%% @end
-spec set_default_space(SpaceId :: binary(), {UserGID :: string(), AccessToken :: binary()}) -> Result when
    Result :: ok | {error, Reason :: term()}.
%% ====================================================================
set_default_space(SpaceId, {UserGID, _AccessToken}) ->
    case user_logic:get_user({global_id, UserGID}) of
        {ok, #veil_document{record = #user{spaces = SpaceIds} = UserRec} = UserDoc} ->
            NewSpaces = [SpaceId | lists:delete(SpaceId, SpaceIds)],
            dao_lib:apply(dao_users, save_user, [UserDoc#veil_document{record = UserRec#user{spaces = NewSpaces}}], 1);
        {error, Reason} ->
            {error, Reason}
    end.


%% synchronize_user_spaces/1
%% ====================================================================
%% @doc Synchronizes and returns list of IDs of Spaces that user belongs to.
%% @end
-spec synchronize_user_spaces({UserGID :: string(), AccessToken :: binary()}) -> Result when
    Result :: {ok, SpaceIds :: [binary()]} | {error, Reason :: term()}.
%% ====================================================================
synchronize_user_spaces({UserGID, AccessToken}) ->
    case user_logic:get_user({global_id, UserGID}) of
        {ok, UserDoc} ->
            #veil_document{record = #user{spaces = Spaces}} = user_logic:synchronize_spaces_info(UserDoc, AccessToken),
            {ok, Spaces};
        {error, Reason} ->
            {error, Reason}
    end.


%% get_space_info/2
%% ====================================================================
%% @doc Returns public information about Space.
%% @end
-spec get_space_info(SpaceId :: binary(), undefined | {UserGID :: string(), AccessToken :: binary()}) -> Result when
    Result :: {ok, SpaceInfo :: #space_info{}} | {error, Reason :: term()}.
%% ====================================================================
get_space_info(SpaceId, undefined) ->
    get_space_info(SpaceId, {undefined, undefined});
get_space_info(SpaceId, {UserGID, AccessToken}) ->
    try
        {ok, #space_details{
            id = BinarySpaceId,
            name = Name}
        } = gr_spaces:get_details({try_user, AccessToken}, vcn_utils:ensure_binary(SpaceId)),
        {ok, ProviderIds} = get_space_providers(SpaceId, {UserGID, AccessToken}),
        {ok, #space_info{space_id = BinarySpaceId, name = Name, providers = ProviderIds}}
    catch
        _:Reason ->
            ?error("Cannot get info of Space with ID ~p: ~p", [SpaceId, Reason]),
            {error, Reason}
    end.


%% get_space_providers/2
%% ====================================================================
%% @doc Returns list of IDs of providers that supports Space.
%% @end
-spec get_space_providers(SpaceId :: binary(), undefined | {UserGID :: string(), AccessToken :: binary()}) -> Result when
    Result :: {ok, ProviderIds :: [binary()]} | {error, Reason :: term()}.
%% ====================================================================
get_space_providers(SpaceId, undefined) ->
    get_space_providers(SpaceId, {undefined, undefined});
get_space_providers(SpaceId, {_UserGID, AccessToken}) ->
    gr_spaces:get_providers({try_user, AccessToken}, vcn_utils:ensure_binary(SpaceId)).


%% verify_client/2
%% ====================================================================
%% @doc Verifies client identity in Global Registry.
%% @end
-spec verify_client(UserGID :: binary(), TokenHash :: binary()) -> Result when
    Result :: boolean() | no_return().
%% ====================================================================
verify_client(undefined, _) ->
    false;
verify_client(_, undefined) ->
    false;
verify_client(UserGID, TokenHash) when is_binary(UserGID), is_binary(TokenHash) ->
    case gr_openid:verify_client(provider, [{<<"userId">>, UserGID}, {<<"secret">>, vcn_utils:ensure_binary(TokenHash)}]) of
        {ok, VerifyStatus} ->
            VerifyStatus;
        {error, Reason} ->
            ?error("Cannot verify user (~p) authentication due to: ~p", [UserGID, Reason]),
            throw({unable_to_authenticate, Reason})
    end.