%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @author Jakub Liput
%%% @copyright (C) 2015-2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements data_backend_behaviour and is used to synchronize
%%% the data-space model used in Ember application.
%%% @end
%%%-------------------------------------------------------------------
-module(data_space_data_backend).
-author("Lukasz Opiola").
-author("Jakub Liput").

-include("proto/common/credentials.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").

%% Key under which default space is stored in session memory.
-define(DEFAULT_SPACE_KEY, default_space).

%% API
-export([init/0]).
-export([find/2, find_all/1, find_query/2]).
-export([create_record/2, update_record/3, delete_record/2]).


%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback init/0.
%% @end
%%--------------------------------------------------------------------
-spec init() -> ok.
init() ->
    % Resolve default space and put it in session memory
    SessionId = g_session:get_session_id(),
    {ok, #document{value = #session{auth = Auth}}} = session:get(SessionId),
    #auth{macaroon = Mac, disch_macaroons = DMacs} = Auth,
    {ok, DefaultSpaceId} = oz_users:get_default_space({user, {Mac, DMacs}}),
    DefaultSpaceDirId = fslogic_uuid:spaceid_to_space_dir_uuid(DefaultSpaceId),
    g_session:put_value(?DEFAULT_SPACE_KEY, DefaultSpaceDirId),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find/2.
%% @end
%%--------------------------------------------------------------------
-spec find(ResourceType :: binary(), Ids :: [binary()]) ->
    {ok, proplists:proplist()} | gui_error:error_result().
find(<<"data-space">>, [SpaceDirId]) ->
    %% todo: ensure ID is correct
    SpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(SpaceDirId),
    {ok, #document{
        value = #space_info{
            name = Name
        }}} = space_info:get(SpaceId),
    DefaultSpaceDirId = g_session:get_value(?DEFAULT_SPACE_KEY),
    Res = [
        {<<"id">>, SpaceDirId},
        {<<"name">>, Name},
        {<<"isDefault">>, SpaceDirId =:= DefaultSpaceDirId},
        {<<"rootDir">>, SpaceDirId}
    ],
    {ok, Res}.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_all/1.
%% @end
%%--------------------------------------------------------------------
-spec find_all(ResourceType :: binary()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
find_all(<<"data-space">>) ->
    UserId = op_gui_utils:get_user_id(),
    {ok, #document{
        value = #onedata_user{
            space_ids = SpaceIds}}} = onedata_user:get(UserId),
    Res = lists:map(
        fun(SpaceId) ->
            %% todo: ensure ID is correct
            SpaceDirId = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),
            {ok, SpaceData} = find(<<"data-space">>, [SpaceDirId]),
            SpaceData
        end, SpaceIds),
    {ok, Res}.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_query/2.
%% @end
%%--------------------------------------------------------------------
-spec find_query(ResourceType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
find_query(<<"data-space">>, _Data) ->
    gui_error:report_error(<<"Not iplemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback create_record/2.
%% @end
%%--------------------------------------------------------------------
-spec create_record(RsrcType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
create_record(<<"data-space">>, _Data) ->
    gui_error:report_error(<<"Not iplemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback update_record/3.
%% @end
%%--------------------------------------------------------------------
-spec update_record(RsrcType :: binary(), Id :: binary(),
    Data :: proplists:proplist()) ->
    ok | gui_error:error_result().
update_record(<<"data-space">>, _Id, _Data) ->
    gui_error:report_error(<<"Not iplemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback delete_record/2.
%% @end
%%--------------------------------------------------------------------
-spec delete_record(RsrcType :: binary(), Id :: binary()) ->
    ok | gui_error:error_result().
delete_record(<<"data-space">>, _Id) ->
    gui_error:report_error(<<"Not iplemented">>).
