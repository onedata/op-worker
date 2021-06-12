%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019-2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles middleware operations (create, get, update, delete)
%%% corresponding to oz space aspects.
%%% @end
%%%-------------------------------------------------------------------
-module(space_oz_middleware_handler).
-author("Bartosz Walkowicz").

-behaviour(middleware_handler).

-include("middleware/middleware.hrl").
-include("modules/datastore/transfer.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/privileges.hrl").

%% middleware_handler callbacks
-export([data_spec/1, fetch_entity/1, authorize/2, validate/2]).
-export([create/1, get/2, update/1, delete/1]).


-define(MAX_LIST_LIMIT, 1000).
-define(DEFAULT_LIST_LIMIT, 1000).


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
    As =:= list;
    As =:= instance;
    As =:= eff_users;
    As =:= eff_groups;
    As =:= shares;
    As =:= providers
->
    undefined.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback fetch_entity/1.
%% @end
%%--------------------------------------------------------------------
-spec fetch_entity(middleware:req()) ->
    {ok, middleware:versioned_entity()} | errors:error().
fetch_entity(_) ->
    {ok, {undefined, 1}}.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback authorize/2.
%% @end
%%--------------------------------------------------------------------
-spec authorize(middleware:req(), middleware:entity()) -> boolean().
authorize(#op_req{auth = ?GUEST}, _) ->
    false;

authorize(#op_req{operation = get, gri = #gri{aspect = list}}, _) ->
    % User is always authorized to list his spaces
    true;

authorize(#op_req{operation = get, auth = Auth, gri = #gri{
    id = SpaceId,
    aspect = As
}}, _) when
    As =:= instance;
    As =:= providers
->
    middleware_utils:is_eff_space_member(Auth, SpaceId);

authorize(#op_req{operation = get, auth = ?USER(UserId), gri = #gri{
    id = SpaceId,
    aspect = As
}}, _) when
    As =:= eff_users;
    As =:= eff_groups;
    As =:= shares
->
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_VIEW).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback validate/2.
%% @end
%%--------------------------------------------------------------------
-spec validate(middleware:req(), middleware:entity()) -> ok | no_return().
validate(#op_req{operation = get, gri = #gri{aspect = list}}, _) ->
    % User spaces are listed by fetching information from zone,
    % whether they are supported locally is irrelevant.
    ok;

validate(#op_req{operation = get, gri = #gri{id = SpaceId, aspect = As}}, _) when
    As =:= instance;
    As =:= eff_users;
    As =:= eff_groups;
    As =:= shares;
    As =:= providers
->
    middleware_utils:assert_space_supported_locally(SpaceId).


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
get(#op_req{auth = ?USER(UserId, SessionId), gri = #gri{aspect = list}}, _) ->
    case user_logic:get_eff_spaces(SessionId, UserId) of
        {ok, EffSpaces} ->
            {ok ,lists:map(fun(SpaceId) ->
                SpaceDirGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
                {ok, SpaceDirObjectId} = file_id:guid_to_objectid(SpaceDirGuid),
                {ok, SpaceName} = space_logic:get_name(SessionId, SpaceId),

                #{
                    <<"spaceId">> => SpaceId,
                    <<"fileId">> => SpaceDirObjectId,
                    <<"name">> => SpaceName
                }
            end, EffSpaces)};
        {error, _} = Error ->
            Error
    end;

get(#op_req{auth = Auth, gri = #gri{id = SpaceId, aspect = instance}}, _) ->
    case space_logic:get(Auth#auth.session_id, SpaceId) of
        {ok, #document{value = Space}} ->
            {ok, Space};
        {error, _} = Error ->
            Error
    end;

get(#op_req{auth = Auth, gri = #gri{id = SpaceId, aspect = eff_users}}, _) ->
    space_logic:get_eff_users(Auth#auth.session_id, SpaceId);

get(#op_req{auth = Auth, gri = #gri{id = SpaceId, aspect = eff_groups}}, _) ->
    space_logic:get_eff_groups(Auth#auth.session_id, SpaceId);

get(#op_req{auth = Auth, gri = #gri{id = SpaceId, aspect = shares}}, _) ->
    space_logic:get_shares(Auth#auth.session_id, SpaceId);

get(#op_req{auth = Auth, gri = #gri{id = SpaceId, aspect = providers}}, _) ->
    space_logic:get_provider_ids(Auth#auth.session_id, SpaceId).


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
