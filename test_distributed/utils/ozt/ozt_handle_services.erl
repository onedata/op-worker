%%%-------------------------------------------------------------------
%%% @author Katarzyna Such
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Common functions related to handle services operations in Onezone to be used
%%% in CT tests.
%%% @end
%%%-------------------------------------------------------------------
-module(ozt_handle_services).
-author("Katarzyna Such").

-include("modules/datastore/datastore_models.hrl").

-export([list_handle_services/0]).
-export([add_user_to_all_handle_services/1, add_user_to_all_handle_services/2]).
-export([remove_user_from_all_handle_services/2]).


%%%===================================================================
%%% API
%%%===================================================================

-spec list_handle_services() -> [od_handle_service:id()].
list_handle_services() ->
    ozw_test_rpc:list_handle_services().


-spec add_user_to_all_handle_services(oct_background:entity_selector()) -> ok.
add_user_to_all_handle_services(UserSelector) ->
    Privileges = privileges:handle_service_member(),
    UserId = oct_background:get_user_id(UserSelector),
    lists:foreach(fun(HServiceId) ->
        ozw_test_rpc:add_user_to_handle_service(HServiceId, UserId, Privileges)
    end, list_handle_services()).

-spec add_user_to_all_handle_services(oct_background:entity_selector(), [atom()]) -> ok.
add_user_to_all_handle_services(UserSelector, Privileges) ->
    UserId = oct_background:get_user_id(UserSelector),
    lists:foreach(fun(HServiceId) ->
        ozw_test_rpc:add_user_to_handle_service(HServiceId, UserId, Privileges)
    end, list_handle_services()).


-spec remove_user_from_all_handle_services(
    oct_background:entity_selector(), oct_background:entity_selector()
) -> ok.
remove_user_from_all_handle_services(UserSelector, ProviderSelector) ->
    UserId = oct_background:get_user_id(UserSelector),
    lists:foreach(fun(HServiceId) ->
        ozw_test_rpc:remove_user_from_handle_service(HServiceId, UserId)
    end, list_user_handle_services(UserSelector, ProviderSelector)).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
list_user_handle_services(UserSelector, ProviderSelector) ->
    UserId = oct_background:get_user_id(UserSelector),
    Node = oct_background:get_random_provider_node(ProviderSelector),
    SessId = oct_background:get_user_session_id(UserSelector, ProviderSelector),
    {ok,  #document{value = User}} = rpc:call(Node, user_logic, get, [SessId, UserId]),
    #od_user{eff_handle_services = HandleServices} = User,
    HandleServices.