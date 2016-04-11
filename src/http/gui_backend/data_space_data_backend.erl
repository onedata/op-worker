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

-export([init/0, terminate/0]).
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
-spec find(ResourceType :: binary(), Ids :: [binary()]) ->
    {ok, proplists:proplist()} | gui_error:error_result().
find(<<"data-space">>, [SpaceDirId]) ->
    SessionId = g_session:get_session_id(),
    Auth = op_gui_utils:get_user_rest_auth(),
    SpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(SpaceDirId),
    {ok, #document{
        value = #space_info{
            name = Name
        }}} = space_info:get_or_fetch(Auth, SpaceId),
    DefaultSpaceDirId = fslogic_uuid:spaceid_to_space_dir_uuid(
        op_gui_utils:get_users_default_space()),
    % If current provider cannot get info about, return null rootDir which will
    % cause the client to render a
    % "space not supported or cannot be synced" message
    RootDir = try
        % This will crash if provider cannot sync this space
        file_data_backend:get_parent(SessionId, SpaceDirId),
        SpaceDirId
    catch T:M ->
        ?warning(
            "Cannot get parent for space (~p). ~p:~p", [SpaceDirId, T, M]),
        null
    end,
    Res = [
        {<<"id">>, SpaceDirId},
        {<<"name">>, Name},
        {<<"isDefault">>, SpaceDirId =:= DefaultSpaceDirId},
        {<<"rootDir">>, RootDir}
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
    SessionId = g_session:get_session_id(),
    {ok, SpaceDirs} = logical_file_manager:ls(SessionId,
        {path, <<"/spaces">>}, 0, 1000),
    DefaultSpaceDirId = fslogic_uuid:spaceid_to_space_dir_uuid(
        op_gui_utils:get_users_default_space()),
    Res = lists:foldl(
        fun({SpaceDirId, SpaceName}, Acc) ->
            % Returns error when this space is not supported by this provider
            SpaceData = try
                {ok, Data} = find(<<"data-space">>, [SpaceDirId]),
                Data
            catch
                T:M ->
                    ?error_stacktrace(
                        "Cannot read space data (~p). ~p:~p", [SpaceDirId, T, M]),
                    [
                        {<<"id">>, SpaceDirId},
                        {<<"name">>, SpaceName},
                        {<<"isDefault">>, SpaceDirId =:= DefaultSpaceDirId},
                        {<<"rootDir">>, null}
                    ]
            end,
            Acc ++ [SpaceData]
        end, [], SpaceDirs),
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

