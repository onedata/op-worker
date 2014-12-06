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

-include("oneprovider_modules/dao/dao.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/global_registry/gr_spaces.hrl").

%% API
-export([verify_client/2]).
-export([synchronize_user_spaces/1, synchronize_user_groups/1, get_space_info/2, get_space_providers/2]).

%% ====================================================================
%% API functions
%% ====================================================================


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
            #db_document{record = #user{spaces = Spaces}} = user_logic:synchronize_spaces_info(UserDoc, AccessToken),
            {ok, Spaces};
        {error, Reason} ->
            ?error("Cannot synchronize Spaces of user with ID ~p: ~p", [UserGID, Reason]),
            {error, Reason}
    end.

%% synchronize_user_groups/1
%% ====================================================================
%% @doc Synchronizes and returns list of groups that user belongs to.
%% @end
-spec synchronize_user_groups({UserGID :: string(), AccessToken :: binary()}) -> Result when
    Result :: {ok, GroupIds :: [binary()]} | {error, Reason :: term()}.
%% ====================================================================
synchronize_user_groups({UserGID, AccessToken}) ->
    case user_logic:get_user({global_id, UserGID}) of
        {ok, UserDoc} ->
            #db_document{record = #user{groups = Groups}} = user_logic:synchronize_groups(UserDoc, AccessToken),
            synchronize_user_group_details(Groups, AccessToken), %todo optimize so we dont synchronize each group every time
            {ok, Groups};
        {error, Reason} ->
            ?error("Cannot synchronize Groups of user with ID ~p: ~p", [UserGID, Reason]),
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
            name = Name,
            size = Size}
        } = gr_spaces:get_details({try_user, AccessToken}, utils:ensure_binary(SpaceId)),
        {ok, ProviderIds} = get_space_providers(SpaceId, {UserGID, AccessToken}),

        GroupDetailsList =
            case get_space_groups(SpaceId, {UserGID, AccessToken}) of
                {ok, Groups} -> Groups;
                _           -> []
            end,

        UserIds =
            case gr_spaces:get_users(provider, SpaceId) of
                {ok, Users} -> Users;
                _           -> []
            end,

        {ok, #space_info{space_id = BinarySpaceId, name = Name, size = Size, providers = ProviderIds, groups = GroupDetailsList, users = UserIds}}
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
    gr_spaces:get_providers({try_user, AccessToken}, utils:ensure_binary(SpaceId)).

%% get_space_groups/2
%% ====================================================================
%% @doc Returns list of IDs of groups that have access to Space.
%% @end
-spec get_space_groups(SpaceId :: binary(), undefined | {UserGID :: string(), AccessToken :: binary()}) -> Result when
    Result :: {ok, GroupIds :: [binary()]} | {error, Reason :: term()}.
%% ====================================================================
get_space_groups(SpaceId, undefined) ->
    get_space_groups(SpaceId, {undefined, undefined});
get_space_groups(SpaceId, {_UserGID, AccessToken}) ->
    case gr_spaces:get_groups({try_user, AccessToken}, utils:ensure_binary(SpaceId)) of
        {ok, GroupIds} ->
            synchronize_space_group_details(SpaceId, GroupIds, {_UserGID, AccessToken}), %todo optimize so we dont synchronize each group every time
            {ok, GroupIds};
        Error ->
            ?error("Cannot get space ~p groups, error: ~p", [SpaceId, Error]),
            Error
    end.

%% synchronize_space_group_details/3
%% ====================================================================
%% @doc Synchronizes (with globalregistry) all space group details
%% @end
-spec synchronize_space_group_details(SpaceId :: binary(), GroupIds :: [binary()], undefined | {UserGID :: string(), AccessToken :: binary()}) -> Result when
    Result :: ok | {error, Reason :: term()}.
%% ====================================================================
synchronize_space_group_details(SpaceId, GroupIds, undefined) ->
    synchronize_space_group_details(SpaceId, GroupIds, {undefined, undefined});
synchronize_space_group_details(SpaceId, GroupIds, {_UserGID, AccessToken}) ->
    AnsList = lists:map(fun(Id) ->
        case gr_spaces:get_group_details({try_user, AccessToken}, utils:ensure_binary(SpaceId), utils:ensure_binary(Id)) of
            {ok, #group_details{id = Id} = GroupDetails} -> dao_lib:apply(dao_groups, save_group, [#db_document{uuid = Id, record = GroupDetails}], 1);
            Error ->
                ?error("Cannot get info of group ~p, error: ~p", [Id, Error]),
                Error
        end
    end, GroupIds),
    case lists:dropwhile(fun(Ans) -> Ans == ok end, AnsList) of
        [] -> ok;
        [Error | _] -> Error
    end.


%% synchronize_user_group_details/2
%% ====================================================================
%% @doc Synchronizes (with globalregistry) user group details present in database
%% @end
-spec synchronize_user_group_details(GroupIds :: [binary()], AccessToken :: binary()) -> Result when
    Result :: ok | {error, Reason :: term()}.
%% ====================================================================
synchronize_user_group_details(GroupIds, AccessToken) ->
    AnsList = lists:map(fun(Id) ->
        case gr_users:get_group_details({user, AccessToken}, utils:ensure_binary(Id)) of
            {ok, #group_details{id = Id} = GroupDetails} -> dao_lib:apply(dao_groups, save_group, [#db_document{uuid = Id, record = GroupDetails}], 1);
            Error ->
                ?error("Cannot get info of group ~p, error: ~p", [Id, Error]),
                Error
        end
    end, GroupIds),
    case lists:dropwhile(fun(Ans) -> Ans == ok end, AnsList) of
        [] -> ok;
        [Error | _] -> Error
    end.

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
    case gr_openid:verify_client(provider, [{<<"userId">>, UserGID}, {<<"secret">>, utils:ensure_binary(TokenHash)}]) of
        {ok, VerifyStatus} ->
            VerifyStatus;
        {error, Reason} ->
            ?error("Cannot verify user (~p) authentication due to: ~p", [UserGID, Reason]),
            throw({unable_to_authenticate, Reason})
    end.