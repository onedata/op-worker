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
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("cluster_worker/include/api_errors.hrl").
-include_lib("cluster_worker/include/graph_sync/graph_sync.hrl").

%% API
-export([init/0, terminate/0]).
-export([find_record/2, find_all/1, query/2, query_record/2]).
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
%% {@link data_backend_behaviour} callback find_record/2.
%% @end
%%--------------------------------------------------------------------
-spec find_record(ResourceType :: binary(), Id :: binary()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
find_record(<<"system-provider">>, _ProviderId) ->
    gui_error:report_error(<<"Not implemented">>);

find_record(<<"system-user">>, _UserId) ->
    gui_error:report_error(<<"Not implemented">>);

find_record(<<"system-group">>, _GroupId) ->
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
%% {@link data_backend_behaviour} callback query/2.
%% @end
%%--------------------------------------------------------------------
-spec query(ResourceType :: binary(), Data :: proplists:proplist()) ->
    {ok, [proplists:proplist()]} | gui_error:error_result().
query(_ResourceType, _Data) ->
    gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback query_record/2.
%% @end
%%--------------------------------------------------------------------
-spec query_record(ResourceType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
query_record(<<"system-provider">>, Data) ->
    SessionId = gui_session:get_session_id(),
    ProviderId = proplists:get_value(<<"id">>, Data),
    case provider_logic:get_name(SessionId, ProviderId) of
        ?ERROR_FORBIDDEN ->
            gui_error:unauthorized();
        {ok, ProviderName} ->
            {ok, [
                {<<"id">>, ProviderId},
                {<<"name">>, ProviderName}
            ]}
    end;

query_record(<<"system-user">>, Data) ->
    SessionId = gui_session:get_session_id(),
    UserId = proplists:get_value(<<"id">>, Data),
    [{EntityType, EntityId}] = proplists:get_value(<<"context">>, Data),
    AuthHint = case EntityType of
        <<"od_group">> -> ?THROUGH_GROUP(EntityId);
        <<"od_space">> -> ?THROUGH_SPACE(EntityId)
    end,

    case user_logic:get_name(SessionId, UserId, AuthHint) of
        ?ERROR_FORBIDDEN ->
            gui_error:unauthorized();
        {ok, UserName} ->
            {ok, [
                {<<"id">>, UserId},
                {<<"name">>, UserName}
            ]}
    end;

query_record(<<"system-group">>, Data) ->
    SessionId = gui_session:get_session_id(),
    GroupId = proplists:get_value(<<"id">>, Data),
    Context = proplists:get_value(<<"context">>, Data),
    [{EntityType, EntityId}] = Context,
    AuthHint = case EntityType of
        <<"od_group">> -> ?THROUGH_GROUP(EntityId);
        <<"od_space">> -> ?THROUGH_SPACE(EntityId)
    end,

    case group_logic:get_name(SessionId, GroupId, AuthHint) of
        ?ERROR_FORBIDDEN ->
            gui_error:unauthorized();
        {ok, GroupName} ->
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

