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


% @todo currently unused - every time taken from OZ
%% Key under which default space is stored in session memory.
-define(DEFAULT_SPACE_KEY, default_space).

%% API
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
    op_gui_utils:register_backend(?MODULE, self()),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback terminate/0.
%% @end
%%--------------------------------------------------------------------
-spec terminate() -> ok.
terminate() ->
    op_gui_utils:unregister_backend(?MODULE, self()),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find/2.
%% @end
%%--------------------------------------------------------------------
-spec find(ResourceType :: binary(), Ids :: [binary()]) ->
    {ok, proplists:proplist()} | gui_error:error_result().
find(<<"data-space">>, [SpaceDirId]) ->
    SpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(SpaceDirId),
    {ok, #document{
        value = #space_info{
            providers = Providers,
            name = Name
        }}} = space_info:get(SpaceId),
    % Allow only spaces that are supported by this provider
    case lists:member(oneprovider:get_provider_id(), Providers) of
        true ->
            DefaultSpaceDirId = fslogic_uuid:spaceid_to_space_dir_uuid(
                op_gui_utils:get_users_default_space()),
            Res = [
                {<<"id">>, SpaceDirId},
                {<<"name">>, Name},
                {<<"isDefault">>, SpaceDirId =:= DefaultSpaceDirId},
                {<<"rootDir">>, SpaceDirId}
            ],
            {ok, Res};
        false ->
            gui_error:report_error(<<"Space not supported by this provider">>)
    end.


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
    Res = lists:foldl(
        fun({SpaceDirId, _SpaceName}, Acc) ->
            % Returns error when this space is not supported by this provider
            case find(<<"data-space">>, [SpaceDirId]) of
                {ok, Data} ->
                    Acc ++ Data;
                _ ->
                    Acc
            end
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

