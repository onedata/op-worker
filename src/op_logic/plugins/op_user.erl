%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles op logic operations (create, get, update, delete)
%%% corresponding to user.
%%% @end
%%%-------------------------------------------------------------------
-module(op_user).
-author("Bartosz Walkowicz").

-behaviour(op_logic_behaviour).

-include("op_logic.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/privileges.hrl").

-export([
    operation_supported/3,
    data_spec/1,
    fetch_entity/1,
    exists/2,
    authorize/2,
    validate/2
]).
-export([create/1, get/2, update/1, delete/1]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback operation_supported/3.
%% @end
%%--------------------------------------------------------------------
-spec operation_supported(op_logic:operation(), gri:aspect(),
    op_logic:scope()) -> boolean().
operation_supported(get, instance, private) -> true;
operation_supported(get, instance, shared) -> true;
operation_supported(get, eff_spaces, private) -> true;

operation_supported(_, _, _) -> false.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback data_spec/1.
%% @end
%%--------------------------------------------------------------------
-spec data_spec(op_logic:req()) -> undefined | op_sanitizer:data_spec().
data_spec(#op_req{operation = get, gri = #gri{aspect = instance}}) ->
    undefined;

data_spec(#op_req{operation = get, gri = #gri{aspect = eff_spaces}}) ->
    undefined.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback fetch_entity/1.
%%
%% For now fetches only records for authorized users.
%% @end
%%--------------------------------------------------------------------
-spec fetch_entity(op_logic:req()) ->
    {ok, op_logic:versioned_entity()} | errors:error().
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
%% {@link op_logic_behaviour} callback exists/2.
%% @end
%%--------------------------------------------------------------------
-spec exists(op_logic:req(), op_logic:entity()) -> boolean().
exists(_, _) ->
    true.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback authorize/2.
%% @end
%%--------------------------------------------------------------------
-spec authorize(op_logic:req(), op_logic:entity()) -> boolean().
authorize(#op_req{auth = ?NOBODY}, _) ->
    false;

%% User can perform all operations on his record
authorize(#op_req{auth = ?USER(UserId), gri = #gri{id = UserId}}, _) ->
    true;

authorize(#op_req{operation = get, gri = #gri{aspect = instance, scope = shared}}, _) ->
    % authorization was checked by oz in `fetch_entity`
    true.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback validate/2.
%% @end
%%--------------------------------------------------------------------
-spec validate(op_logic:req(), op_logic:entity()) -> ok | no_return().
validate(#op_req{operation = get, gri = #gri{aspect = As}}, _) when
    As =:= instance;
    As =:= eff_spaces
->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(op_logic:req()) -> op_logic:create_result().
create(_) ->
    ?ERROR_NOT_SUPPORTED.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(op_logic:req(), op_logic:entity()) -> op_logic:get_result().
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
    user_logic:get_eff_spaces(User).


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback update/1.
%% @end
%%--------------------------------------------------------------------
-spec update(op_logic:req()) -> op_logic:update_result().
update(_) ->
    ?ERROR_NOT_SUPPORTED.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(op_logic:req()) -> op_logic:delete_result().
delete(_) ->
    ?ERROR_NOT_SUPPORTED.
