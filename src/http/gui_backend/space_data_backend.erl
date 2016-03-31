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
-module(space_data_backend).
-author("Lukasz Opiola").
-author("Jakub Liput").

-include("proto/common/credentials.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").

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
    g_session:put_value(?DEFAULT_SPACE_KEY,
        op_gui_utils:get_users_default_space()),
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
find(<<"space">>, [SpaceId]) ->
    {ok, #space_info{
        name = Name,
        size = _Size,
        users = Users,
        groups = Groups,
        providers = _Providers
    }} = space_info:get(SpaceId),

    UserPermissions = lists:map(
        fun(UserId) ->
            ids_to_association(SpaceId, UserId)
        end, Users),

    GroupPermissions = lists:map(
        fun(GroupId) ->
            ids_to_association(SpaceId, GroupId)
        end, Groups),

    DefaultSpaceId = g_session:get_value(?DEFAULT_SPACE_KEY),
    Res = [
        {<<"id">>, SpaceId},
        {<<"name">>, Name},
        {<<"isDefault">>, SpaceId =:= DefaultSpaceId},
        {<<"userPermissions">>, []},
        {<<"groupPermissions">>, []}
    ],
    {ok, Res}.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_all/1.
%% @end
%%--------------------------------------------------------------------
-spec find_all(ResourceType :: binary()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
find_all(<<"space">>) ->
    UserId = op_gui_utils:get_user_id(),
    {ok, #document{
        value = #onedata_user{
            space_ids = SpaceIds}}} = onedata_user:get(UserId),
    Res = lists:map(
        fun(SpaceId) ->
            {ok, SpaceData} = find(<<"space">>, SpaceId),
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
find_query(<<"space">>, _Data) ->
    gui_error:report_error(<<"Not iplemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback create_record/2.
%% @end
%%--------------------------------------------------------------------
-spec create_record(RsrcType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
create_record(<<"space">>, _Data) ->
    gui_error:report_error(<<"Not iplemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback update_record/3.
%% @end
%%--------------------------------------------------------------------
-spec update_record(RsrcType :: binary(), Id :: binary(),
    Data :: proplists:proplist()) ->
    ok | gui_error:error_result().
update_record(<<"space">>, _Id, _Data) ->
    gui_error:report_error(<<"Not iplemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback delete_record/2.
%% @end
%%--------------------------------------------------------------------
-spec delete_record(RsrcType :: binary(), Id :: binary()) ->
    ok | gui_error:error_result().
delete_record(<<"space">>, _Id) ->
    gui_error:report_error(<<"Not iplemented">>).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a space record based on space id and name.
%% @end
%%--------------------------------------------------------------------
-spec space_record(SpaceId :: binary(), SpaceName :: binary()) ->
    proplists:proplist().
space_record(SpaceId, SpaceName) ->
    DefaultSpaceId = g_session:get_value(?DEFAULT_SPACE_KEY),
    [
        {<<"id">>, SpaceId},
        {<<"name">>, SpaceName},
        {<<"isDefault">>, SpaceId =:= DefaultSpaceId},
        {<<"rootDir">>, SpaceId}
    ].


ids_to_association(FirstId, SecondId) ->
    <<FirstId/binary, "@", SecondId/binary>>.

association_to_ids(AssocId) ->
    [FirstId, SecondId] = binary:split(AssocId, <<"@">>, [global]),
    {FirstId, SecondId}.

