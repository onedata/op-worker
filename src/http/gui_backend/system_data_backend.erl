%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements data_backend_behaviour and is used to synchronize
%%% the couple of models for ember app:
%%%     - system-user
%%%     - system-group
%%%     - system-provider
%%%
%%% The 'system' prefix means that the models holds only informational data
%%% about given entity, mainly human-readable names. The provider might not have
%%% rights to view full data or modify it. They are used for displaying info
%%% on certain pages, for example a list of users of given space.
%%% @end
%%%-------------------------------------------------------------------
-module(system_data_backend).
-behavior(data_backend_behaviour).
-author("Lukasz Opiola").

-include("proto/common/credentials.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/oz/oz_providers.hrl").

%% API
-export([init/0, terminate/0]).
-export([find/2, find_all/1, find_query/2]).
-export([create_record/2, update_record/3, delete_record/2]).

%%%===================================================================
%%% data_backend_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback init/0.
%% @end
%%--------------------------------------------------------------------
-spec init() -> ok.
init() ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback terminate/0.
%% @end
%%--------------------------------------------------------------------
-spec terminate() -> ok.
terminate() ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find/2.
%% @end
%%--------------------------------------------------------------------
-spec find(ResourceType :: binary(), Id :: binary()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
find(<<"system-provider">>, _ProviderId) ->
    gui_error:report_error(<<"Not implemented">>);

find(<<"system-user">>, _UserId) ->
    gui_error:report_error(<<"Not implemented">>);

find(<<"system-group">>, _GroupId) ->
    gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_all/1.
%% @end
%%--------------------------------------------------------------------
-spec find_all(ResourceType :: binary()) ->
    {ok, [proplists:proplist()]} | gui_error:error_result().
find_all(_ResourceType) ->
    gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_query/2.
%% @end
%%--------------------------------------------------------------------
-spec find_query(ResourceType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
find_query(<<"system-provider">>, Data) ->
    ProviderId = proplists:get_value(<<"id">>, Data),
    % Do not check context, provider name can always be fetched
    _Context = proplists:get_value(<<"context">>, Data),
    {ok, #provider_details{
        name = Name
    }} = oz_providers:get_details(provider, ProviderId),
    {ok, [
        {<<"id">>, ProviderId},
        {<<"name">>, Name}
    ]};

find_query(<<"system-user">>, Data) ->
    CurrentUserId = gui_session:get_user_id(),
    UserId = proplists:get_value(<<"id">>, Data),
    Context = proplists:get_value(<<"context">>, Data),
    [{EntityType, EntityId}] = Context,
    Authorized = op_gui_utils:can_view_public_data(
        CurrentUserId,
        od_user, UserId,
        binary_to_existing_atom(EntityType, utf8), EntityId
    ),
    case Authorized of
        false ->
            gui_error:unauthorized();
        true ->
            CurrentUserAuth = op_gui_utils:get_user_auth(),
            {ok, #document{value = #od_user{name = UserName}}} =
                user_logic:get(CurrentUserAuth, UserId),
            {ok, [
                {<<"id">>, UserId},
                {<<"name">>, UserName}
            ]}
    end;

find_query(<<"system-group">>, Data) ->
    CurrentUserId = gui_session:get_user_id(),
    GroupId = proplists:get_value(<<"id">>, Data),
    Context = proplists:get_value(<<"context">>, Data),
    [{EntityType, EntityId}] = Context,
    Authorized = op_gui_utils:can_view_public_data(
        CurrentUserId,
        od_group, GroupId,
        binary_to_existing_atom(EntityType, utf8), EntityId
    ),
    case Authorized of
        false ->
            gui_error:unauthorized();
        true ->
            CurrentUserAuth = op_gui_utils:get_user_auth(),
            {ok, #document{value = #od_group{name = GroupName}}} =
                group_logic:get(CurrentUserAuth, GroupId),
            {ok, [
                {<<"id">>, GroupId},
                {<<"name">>, GroupName}
            ]}
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback create_record/2.
%% @end
%%--------------------------------------------------------------------
-spec create_record(RsrcType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
create_record(_ResourceType, _Data) ->
    gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback update_record/3.
%% @end
%%--------------------------------------------------------------------
-spec update_record(RsrcType :: binary(), Id :: binary(),
    Data :: proplists:proplist()) ->
    ok | gui_error:error_result().
update_record(_ResourceType, _Id, _Data) ->
    gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback delete_record/2.
%% @end
%%--------------------------------------------------------------------
-spec delete_record(RsrcType :: binary(), Id :: binary()) ->
    ok | gui_error:error_result().
delete_record(_ResourceType, _Id) ->
    gui_error:report_error(<<"Not implemented">>).

