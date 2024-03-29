%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019-2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles middleware operations (create, get, update, delete)
%%% corresponding to user.
%%% @end
%%%-------------------------------------------------------------------
-module(user_middleware_plugin).
-author("Bartosz Walkowicz").

-behaviour(middleware_router).
-behaviour(middleware_handler).

-include("middleware/middleware.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/privileges.hrl").

%% middleware_router callbacks
-export([resolve_handler/3]).

%% middleware_handler callbacks
-export([data_spec/1, fetch_entity/1, authorize/2, validate/2]).
-export([create/1, get/2, update/1, delete/1]).


%%%===================================================================
%%% middleware_router callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_router} callback resolve_handler/3.
%% @end
%%--------------------------------------------------------------------
-spec resolve_handler(middleware:operation(), gri:aspect(), middleware:scope()) ->
    module() | no_return().
resolve_handler(get, instance, private) -> ?MODULE;
resolve_handler(get, instance, shared) -> ?MODULE;
resolve_handler(get, eff_spaces, private) -> ?MODULE;
resolve_handler(get, eff_groups, private) -> ?MODULE;
resolve_handler(get, eff_handle_services, private) -> ?MODULE;
resolve_handler(get, eff_atm_inventories, private) -> ?MODULE;

resolve_handler(_, _, _) -> throw(?ERROR_NOT_SUPPORTED).


%%%===================================================================
%%% middleware_handler callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback data_spec/1.
%% @end
%%--------------------------------------------------------------------
-spec data_spec(middleware:req()) -> undefined | middleware_sanitizer:data_spec().
data_spec(#op_req{operation = get, gri = #gri{aspect = As}}) when
    As =:= instance;
    As =:= eff_spaces;
    As =:= eff_groups;
    As =:= eff_handle_services;
    As =:= eff_atm_inventories
->
    undefined.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback fetch_entity/1.
%%
%% For now fetches only records for authorized users.
%% @end
%%--------------------------------------------------------------------
-spec fetch_entity(middleware:req()) ->
    {ok, middleware:versioned_entity()} | errors:error().
fetch_entity(#op_req{auth = ?NOBODY}) ->
    ?ERROR_UNAUTHORIZED;

fetch_entity(#op_req{auth = ?USER(UserId, SessionId), gri = #gri{id = UserId}}) ->
    case user_logic:get(SessionId, UserId) of
        {ok, #document{value = User}} ->
            {ok, {User, 1}};
        {error, _} = Error ->
            Error
    end;

fetch_entity(#op_req{auth = ?USER(_ClientId, SessionId), auth_hint = AuthHint, gri = #gri{
    id = UserId,
    aspect = instance,
    scope = shared
}}) ->
    case user_logic:get_shared_data(SessionId, UserId, AuthHint) of
        {ok, #document{value = User}} ->
            {ok, {User, 1}};
        {error, _} = Error ->
            Error
    end;

fetch_entity(_) ->
    ?ERROR_FORBIDDEN.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback authorize/2.
%% @end
%%--------------------------------------------------------------------
-spec authorize(middleware:req(), middleware:entity()) -> boolean().
authorize(#op_req{auth = ?GUEST}, _) ->
    false;

%% User can perform all operations on his record
authorize(#op_req{auth = ?USER(UserId), gri = #gri{id = UserId}}, _) ->
    true;

authorize(#op_req{operation = get, gri = #gri{aspect = instance, scope = shared}}, _) ->
    % authorization was checked by oz in `fetch_entity`
    true.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback validate/2.
%% @end
%%--------------------------------------------------------------------
-spec validate(middleware:req(), middleware:entity()) -> ok | no_return().
validate(#op_req{operation = get, gri = #gri{aspect = As}}, _) when
    As =:= instance;
    As =:= eff_spaces;
    As =:= eff_groups;
    As =:= eff_handle_services;
    As =:= eff_atm_inventories
->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(middleware:req()) -> middleware:create_result().
create(_) ->
    ?ERROR_NOT_SUPPORTED.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(middleware:req(), middleware:entity()) -> middleware:get_result().
get(#op_req{gri = #gri{aspect = instance, scope = private}}, User) ->
    {ok, User};

get(#op_req{gri = #gri{aspect = instance, scope = shared}}, #od_user{
    full_name = FullName,
    username = Username
}) ->
    {ok, #{
        <<"fullName">> => FullName,
        <<"username">> => Username
    }};

get(#op_req{gri = #gri{aspect = eff_spaces}}, User) ->
    user_logic:get_eff_spaces(User);

get(#op_req{gri = #gri{aspect = eff_groups}}, User) ->
    user_logic:get_eff_groups(User);

get(#op_req{gri = #gri{aspect = eff_handle_services}}, User) ->
    user_logic:get_eff_handle_services(User);

get(#op_req{gri = #gri{aspect = eff_atm_inventories}}, User) ->
    user_logic:get_eff_atm_inventories(User).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback update/1.
%% @end
%%--------------------------------------------------------------------
-spec update(middleware:req()) -> middleware:update_result().
update(_) ->
    ?ERROR_NOT_SUPPORTED.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(middleware:req()) -> middleware:delete_result().
delete(_) ->
    ?ERROR_NOT_SUPPORTED.
